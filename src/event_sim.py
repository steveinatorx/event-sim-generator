#!/usr/bin/env python3
"""
Event Simulator for local data engineering practice (Question #2: event logging).

Generates realistic e-commerce order lifecycle events with:
- event_time vs ingest_time (late / out-of-order arrivals)
- schema evolution (v1 + v2 for order_created)
- corrections as events (order_amount_corrected)
- append-only design (unique event_id)
- optional Kafka output, plus newline-delimited JSON (NDJSON) files

Outputs:
- <out_dir>/events.ndjson
- <out_dir>/events_by_ingest_date/YYYY-MM-DD.ndjson
- <out_dir>/events_by_event_date/YYYY-MM-DD.ndjson

Usage:
  python src/event_sim.py --out ./out --n-orders 5000 --seed 42
  python src/event_sim.py --out ./out --n-orders 5000 --kafka-bootstrap localhost:9092 --kafka-topic events
  python src/event_sim.py --out ./out --n-orders 5000 --kafka-bootstrap localhost:9092 --kafka-topic events --kafka-key-field order_id
"""

from __future__ import annotations

import argparse
import json
import math
import random
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


# -----------------------------
# Helpers
# -----------------------------

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def date_str(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).date().isoformat()

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def weighted_choice(rng: random.Random, items: List[Tuple[Any, float]]) -> Any:
    total = sum(w for _, w in items)
    r = rng.random() * total
    upto = 0.0
    for item, w in items:
        upto += w
        if upto >= r:
            return item
    return items[-1][0]

def mkdirp(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


# -----------------------------
# Domain constants
# -----------------------------

CURRENCIES = [("USD", 0.85), ("EUR", 0.08), ("GBP", 0.04), ("CAD", 0.03)]
PAYMENT_METHODS = [("card", 0.75), ("paypal", 0.15), ("apple_pay", 0.08), ("bank_transfer", 0.02)]
CARRIERS = [("UPS", 0.45), ("FedEx", 0.35), ("USPS", 0.15), ("DHL", 0.05)]
CANCEL_REASONS = [("payment_failed", 0.5), ("customer_request", 0.35), ("out_of_stock", 0.15)]
DEVICE_TYPES = [("desktop", 0.45), ("mobile", 0.45), ("tablet", 0.10)]
SOURCE_SYSTEMS = [("web", 0.55), ("ios", 0.22), ("android", 0.18), ("partner_api", 0.05)]
ENVIRONMENTS = [("prod", 0.92), ("staging", 0.08)]


@dataclass
class SimConfig:
    n_orders: int
    start_time: datetime
    span_days: int
    tenant_count: int
    user_count: int
    v2_rate: float
    cancel_rate: float
    ship_rate: float
    correction_rate: float
    late_rate: float
    out_of_order_rate: float
    max_late_days: int
    max_event_gap_minutes: int
    seed: int


def make_base_envelope(
    *,
    rng: random.Random,
    event_type: str,
    schema_version: int,
    event_time: datetime,
    ingest_time: datetime,
    tenant_id: str,
    user_id: Optional[str],
    session_id: Optional[str],
    source_system: str,
    environment: str,
    payload: Dict[str, Any],
    record_source: str = "event_sim",
) -> Dict[str, Any]:
    checksum = uuid.uuid5(uuid.NAMESPACE_OID, json.dumps(payload, sort_keys=True)).hex
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "schema_version": schema_version,
        "event_time": iso(event_time),
        "ingest_time": iso(ingest_time),
        "tenant_id": tenant_id,
        "user_id": user_id,
        "session_id": session_id,
        "source_system": source_system,
        "environment": environment,
        "record_source": record_source,
        "payload": payload,
        "checksum": checksum,
    }


def simulate_order_events(cfg: SimConfig) -> List[Dict[str, Any]]:
    rng = random.Random(cfg.seed)
    events: List[Dict[str, Any]] = []

    tenants = [f"T-{i:03d}" for i in range(1, cfg.tenant_count + 1)]
    users = [f"U-{i:05d}" for i in range(1, cfg.user_count + 1)]

    for order_ix in range(cfg.n_orders):
        tenant_id = tenants[rng.randrange(len(tenants))]
        user_id = users[rng.randrange(len(users))]
        session_id = f"S-{rng.randrange(1, 200000):06d}"

        offset_seconds = rng.random() * (cfg.span_days * 24 * 3600)
        order_created_time = cfg.start_time + timedelta(seconds=offset_seconds)

        currency = weighted_choice(rng, CURRENCIES)
        source_system = weighted_choice(rng, SOURCE_SYSTEMS)
        environment = weighted_choice(rng, ENVIRONMENTS)

        amt = math.exp(rng.normalvariate(mu=4.4, sigma=0.6))
        amt = round(clamp(amt, 5.0, 500.0), 2)

        order_id = f"O-{order_ix+1:07d}"

        is_v2 = rng.random() < cfg.v2_rate
        schema_version = 2 if is_v2 else 1

        payload_created = {
            "order_id": order_id,
            "user_id": user_id,
            "amount": amt,
            "currency": currency,
        }
        if is_v2:
            payload_created["promo_code"] = (None if rng.random() < 0.6 else f"PROMO{rng.randrange(10,99)}")
            payload_created["device_type"] = weighted_choice(rng, DEVICE_TYPES)

        payment_time = order_created_time + timedelta(minutes=rng.randint(0, cfg.max_event_gap_minutes))
        payment_method = weighted_choice(rng, PAYMENT_METHODS)
        payload_payment = {
            "order_id": order_id,
            "payment_method": payment_method,
            "auth_code": uuid.uuid4().hex[:8].upper(),
        }

        cancelled = rng.random() < cfg.cancel_rate
        shipped = (not cancelled) and (rng.random() < cfg.ship_rate)

        ship_time = None
        payload_shipped = None
        if shipped:
            ship_time = payment_time + timedelta(minutes=rng.randint(30, cfg.max_event_gap_minutes * 3))
            payload_shipped = {
                "order_id": order_id,
                "carrier": weighted_choice(rng, CARRIERS),
                "tracking_id": f"TRK{rng.randrange(10_000_000, 99_999_999)}",
            }

        cancel_time = None
        payload_cancel = None
        if cancelled:
            cancel_time = payment_time + timedelta(minutes=rng.randint(1, cfg.max_event_gap_minutes))
            payload_cancel = {
                "order_id": order_id,
                "reason": weighted_choice(rng, CANCEL_REASONS),
            }

        corrected = (not cancelled) and (rng.random() < cfg.correction_rate)
        correction_time = None
        payload_correct = None
        if corrected:
            correction_time = payment_time + timedelta(minutes=rng.randint(1, cfg.max_event_gap_minutes))
            delta = amt * rng.uniform(-0.15, 0.15)
            new_amt = round(clamp(amt + delta, 1.0, 9999.0), 2)
            if new_amt != amt:
                payload_correct = {
                    "order_id": order_id,
                    "old_amount": amt,
                    "new_amount": new_amt,
                    "currency": currency,
                    "reason": "post_auth_adjustment",
                }

        business_events: List[Tuple[str, int, datetime, Dict[str, Any]]] = []
        business_events.append(("order_created", schema_version, order_created_time, payload_created))
        business_events.append(("payment_authorized", 1, payment_time, payload_payment))
        if payload_correct is not None:
            business_events.append(("order_amount_corrected", 1, correction_time, payload_correct))
        if cancelled and payload_cancel is not None:
            business_events.append(("order_cancelled", 1, cancel_time, payload_cancel))
        if shipped and payload_shipped is not None:
            business_events.append(("order_shipped", 1, ship_time, payload_shipped))

        business_events.sort(key=lambda x: x[2])

        envelopes: List[Dict[str, Any]] = []
        for (etype, ver, etime, payload) in business_events:
            ingest_time = etime + timedelta(seconds=rng.randint(0, 60))
            if rng.random() < cfg.late_rate:
                ingest_time += timedelta(days=rng.randint(1, cfg.max_late_days), minutes=rng.randint(0, 120))

            envelopes.append(
                make_base_envelope(
                    rng=rng,
                    event_type=etype,
                    schema_version=ver,
                    event_time=etime,
                    ingest_time=ingest_time,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    session_id=session_id,
                    source_system=source_system,
                    environment=environment,
                    payload=payload,
                )
            )

        if rng.random() < cfg.out_of_order_rate:
            rng.shuffle(envelopes)
        else:
            envelopes.sort(key=lambda e: e["ingest_time"])

        events.extend(envelopes)

    events.sort(key=lambda e: e["ingest_time"])
    return events


# -----------------------------
# Output writers
# -----------------------------

def write_ndjson(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    mkdirp(path.parent)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, separators=(",", ":"), ensure_ascii=False) + "\n")

def write_partitioned(out_dir: Path, events: List[Dict[str, Any]], *, by: str, subdir: str) -> None:
    buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for e in events:
        dt = datetime.fromisoformat(e[by].replace("Z", "+00:00"))
        buckets[date_str(dt)].append(e)

    for d, rows in buckets.items():
        write_ndjson(out_dir / subdir / f"{d}.ndjson", rows)


# -----------------------------
# Kafka producer (optional)
# -----------------------------

def try_kafka_produce(
    events: List[Dict[str, Any]],
    bootstrap: str,
    topic: str,
    kafka_key_field: Optional[str],
) -> None:
    try:
        from kafka import KafkaProducer  # type: ignore
    except Exception as e:
        raise RuntimeError("kafka-python not installed. Run: pip install kafka-python") from e

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: (k.encode("utf-8") if isinstance(k, str) else None),
        linger_ms=25,
        acks="all",
    )

    for e in events:
        key: Optional[str] = None
        if kafka_key_field:
            # Prefer payload.<field>, then top-level field
            if isinstance(e.get("payload"), dict) and kafka_key_field in e["payload"]:
                key = str(e["payload"][kafka_key_field])
            elif kafka_key_field in e:
                key = str(e[kafka_key_field])

        producer.send(topic, key=key, value=e)

    producer.flush()
    producer.close()


# -----------------------------
# CLI
# -----------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate simulated event logs (NDJSON) and optionally publish to Kafka.")
    p.add_argument("--out", type=str, required=True)
    p.add_argument("--n-orders", type=int, default=5000)
    p.add_argument("--seed", type=int, default=42)

    p.add_argument("--start", type=str, default=None, help="UTC ISO e.g. 2026-01-01T00:00:00Z (default: now - span)")
    p.add_argument("--span-days", type=int, default=14)

    p.add_argument("--tenant-count", type=int, default=25)
    p.add_argument("--user-count", type=int, default=5000)

    p.add_argument("--v2-rate", type=float, default=0.35)
    p.add_argument("--cancel-rate", type=float, default=0.08)
    p.add_argument("--ship-rate", type=float, default=0.85)
    p.add_argument("--correction-rate", type=float, default=0.10)

    p.add_argument("--late-rate", type=float, default=0.06)
    p.add_argument("--out-of-order-rate", type=float, default=0.12)
    p.add_argument("--max-late-days", type=int, default=3)
    p.add_argument("--max-event-gap-minutes", type=int, default=120)

    p.add_argument("--kafka-bootstrap", type=str, default=None)
    p.add_argument("--kafka-topic", type=str, default="events")
    p.add_argument("--kafka-key-field", type=str, default=None, help="e.g. order_id (recommended)")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    out_dir = Path(args.out)
    mkdirp(out_dir)

    if args.start:
        start_time = datetime.fromisoformat(args.start.replace("Z", "+00:00")).astimezone(timezone.utc)
    else:
        start_time = utcnow() - timedelta(days=args.span_days)

    cfg = SimConfig(
        n_orders=args.n_orders,
        start_time=start_time,
        span_days=args.span_days,
        tenant_count=args.tenant_count,
        user_count=args.user_count,
        v2_rate=args.v2_rate,
        cancel_rate=args.cancel_rate,
        ship_rate=args.ship_rate,
        correction_rate=args.correction_rate,
        late_rate=args.late_rate,
        out_of_order_rate=args.out_of_order_rate,
        max_late_days=args.max_late_days,
        max_event_gap_minutes=args.max_event_gap_minutes,
        seed=args.seed,
    )

    events = simulate_order_events(cfg)

    write_ndjson(out_dir / "events.ndjson", events)
    write_partitioned(out_dir, events, by="ingest_time", subdir="events_by_ingest_date")
    write_partitioned(out_dir, events, by="event_time", subdir="events_by_event_date")

    by_type = defaultdict(int)
    late_count = 0
    for e in events:
        by_type[e["event_type"]] += 1
        et = datetime.fromisoformat(e["event_time"].replace("Z", "+00:00"))
        it = datetime.fromisoformat(e["ingest_time"].replace("Z", "+00:00"))
        if it.date() > et.date():
            late_count += 1

    stats = {
        "n_events": len(events),
        "n_orders": cfg.n_orders,
        "event_types": dict(sorted(by_type.items(), key=lambda kv: kv[0])),
        "late_events_estimate": late_count,
        "late_rate_observed": round(late_count / max(1, len(events)), 4),
        "out_dir": str(out_dir),
    }
    (out_dir / "stats.json").write_text(json.dumps(stats, indent=2), encoding="utf-8")

    print(f"Wrote {len(events):,} events to: {out_dir / 'events.ndjson'}")
    print(f"Wrote partitions to: {out_dir / 'events_by_ingest_date'} and {out_dir / 'events_by_event_date'}")
    print(f"Stats: {out_dir / 'stats.json'}")

    if args.kafka_bootstrap:
        print(f"Publishing to Kafka topic '{args.kafka_topic}' @ {args.kafka_bootstrap} ...")
        try_kafka_produce(events, args.kafka_bootstrap, args.kafka_topic, args.kafka_key_field)
        print("Kafka publish complete.")


if __name__ == "__main__":
    main()
