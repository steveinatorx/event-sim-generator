#!/usr/bin/env python3
"""
Event Simulator for a local data-engineering project (Question #2: event logging).

Generates realistic e-commerce order lifecycle events with:
- event_time vs ingest_time (late / out-of-order arrivals)
- schema evolution (v1 + v2 for order_created)
- corrections as events (order_amount_corrected)
- append-only design (unique event_id)
- optional Kafka output, plus newline-delimited JSON (NDJSON) files

Outputs:
- <out_dir>/events.ndjson                 (all events)
- <out_dir>/events_by_ingest_date/YYYY-MM-DD.ndjson
- <out_dir>/events_by_event_date/YYYY-MM-DD.ndjson

Usage examples:
  python event_sim.py --out ./out --n-orders 5000 --seed 42
  python event_sim.py --out ./out --n-orders 5000 --late-rate 0.08 --out-of-order-rate 0.15
  python event_sim.py --out ./out --n-orders 20000 --kafka-bootstrap localhost:9092 --kafka-topic events

Notes:
- "late" means ingest_time occurs on a later day than event_time.
- "out-of-order" means events for a given order may arrive in a non-chronological sequence.
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
    # ISO 8601 with Z
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
# Domain model
# -----------------------------

EVENT_TYPES = [
    "order_created",
    "payment_authorized",
    "order_shipped",
    "order_cancelled",
    "order_amount_corrected",
]

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
    # A "gold" envelope you can map to your DDL.
    # Keep stable "canonical" fields; everything else in payload.
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

    # Basic time generation: uniform order creation times across span
    for order_ix in range(cfg.n_orders):
        tenant_id = tenants[rng.randrange(len(tenants))]
        user_id = users[rng.randrange(len(users))]
        session_id = f"S-{rng.randrange(1, 200000):06d}"

        # Random base time across span_days
        offset_seconds = rng.random() * (cfg.span_days * 24 * 3600)
        order_created_time = cfg.start_time + timedelta(seconds=offset_seconds)

        # Choose stable dimensions
        currency = weighted_choice(rng, CURRENCIES)
        source_system = weighted_choice(rng, SOURCE_SYSTEMS)
        environment = weighted_choice(rng, ENVIRONMENTS)

        # Amount generation: log-normal-ish with bounds
        amt = math.exp(rng.normalvariate(mu=4.4, sigma=0.6))  # ~ (exp) distribution
        amt = round(clamp(amt, 5.0, 500.0), 2)

        order_id = f"O-{order_ix+1:07d}"

        # Schema evolution: order_created v2 adds promo_code + device_type
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

        # Payment
        payment_time = order_created_time + timedelta(minutes=rng.randint(0, cfg.max_event_gap_minutes))
        payment_method = weighted_choice(rng, PAYMENT_METHODS)
        payload_payment = {
            "order_id": order_id,
            "payment_method": payment_method,
            "auth_code": uuid.uuid4().hex[:8].upper(),
        }

        # Decide cancel vs ship path
        cancelled = rng.random() < cfg.cancel_rate
        shipped = (not cancelled) and (rng.random() < cfg.ship_rate)

        # Shipping
        ship_time = None
        payload_shipped = None
        if shipped:
            ship_time = payment_time + timedelta(minutes=rng.randint(30, cfg.max_event_gap_minutes * 3))
            payload_shipped = {
                "order_id": order_id,
                "carrier": weighted_choice(rng, CARRIERS),
                "tracking_id": f"TRK{rng.randrange(10_000_000, 99_999_999)}",
            }

        # Cancel
        cancel_time = None
        payload_cancel = None
        if cancelled:
            cancel_time = payment_time + timedelta(minutes=rng.randint(1, cfg.max_event_gap_minutes))
            payload_cancel = {
                "order_id": order_id,
                "reason": weighted_choice(rng, CANCEL_REASONS),
            }

        # Correction event (append-only)
        corrected = (not cancelled) and (rng.random() < cfg.correction_rate)
        correction_time = None
        payload_correct = None
        if corrected:
            correction_time = payment_time + timedelta(minutes=rng.randint(1, cfg.max_event_gap_minutes))
            # Apply a small correction ± up to 15%
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

        # Build the "true" chronological sequence (business time)
        business_events: List[Tuple[str, int, datetime, Dict[str, Any]]] = []

        business_events.append(("order_created", schema_version, order_created_time, payload_created))
        business_events.append(("payment_authorized", 1, payment_time, payload_payment))

        if payload_correct is not None:
            business_events.append(("order_amount_corrected", 1, correction_time, payload_correct))

        if cancelled and payload_cancel is not None:
            business_events.append(("order_cancelled", 1, cancel_time, payload_cancel))

        if shipped and payload_shipped is not None:
            business_events.append(("order_shipped", 1, ship_time, payload_shipped))

        # Sort by event_time (business truth)
        business_events.sort(key=lambda x: x[2])

        # Now simulate ingest_time with lateness and out-of-order arrival
        envelopes: List[Dict[str, Any]] = []
        for (etype, ver, etime, payload) in business_events:
            ingest_time = etime + timedelta(seconds=rng.randint(0, 60))  # small baseline delay

            # Late arrivals: shift ingest_time forward by up to max_late_days
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

        # Out-of-order arrival: shuffle ingest ordering for some orders
        if rng.random() < cfg.out_of_order_rate:
            rng.shuffle(envelopes)
        else:
            # Otherwise order by ingest_time to simulate normal flow
            envelopes.sort(key=lambda e: e["ingest_time"])

        events.extend(envelopes)

    # Global: sort events by ingest_time to emulate arrival stream
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

def write_partitioned(
    out_dir: Path,
    events: List[Dict[str, Any]],
    *,
    by: str,
    subdir: str,
) -> None:
    """
    Partition NDJSON by either:
      by="ingest_time" or by="event_time"
    """
    buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for e in events:
        ts = e[by]
        # ts is ISO string; parse minimal
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        buckets[date_str(dt)].append(e)

    for d, rows in buckets.items():
        p = out_dir / subdir / f"{d}.ndjson"
        write_ndjson(p, rows)


# -----------------------------
# Optional Kafka producer
# -----------------------------

def try_kafka_produce(events: List[Dict[str, Any]], bootstrap: str, topic: str) -> None:
    try:
        from kafka import KafkaProducer  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "kafka-python not installed. Run: pip install kafka-python"
        ) from e

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        linger_ms=25,
        acks="all",
    )
    for e in events:
        producer.send(topic, e)
    producer.flush()
    producer.close()


# -----------------------------
# CLI
# -----------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate simulated event logs (NDJSON) and optionally publish to Kafka.")
    p.add_argument("--out", type=str, required=True, help="Output directory (e.g., ./out)")
    p.add_argument("--n-orders", type=int, default=5000, help="Number of orders to simulate")
    p.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")

    p.add_argument("--start", type=str, default=None, help="Start datetime (UTC) e.g. 2026-01-01T00:00:00Z (default: now - span)")
    p.add_argument("--span-days", type=int, default=14, help="Time span in days across which order_created times are generated")

    p.add_argument("--tenant-count", type=int, default=25, help="Number of distinct tenants")
    p.add_argument("--user-count", type=int, default=5000, help="Number of distinct users")

    p.add_argument("--v2-rate", type=float, default=0.35, help="Fraction of order_created events that are schema_version=2")
    p.add_argument("--cancel-rate", type=float, default=0.08, help="Fraction of orders that are cancelled")
    p.add_argument("--ship-rate", type=float, default=0.85, help="Fraction of non-cancelled orders that are shipped")
    p.add_argument("--correction-rate", type=float, default=0.10, help="Fraction of non-cancelled orders that have a correction event")

    p.add_argument("--late-rate", type=float, default=0.06, help="Probability an event is late (ingest_time shifted forward)")
    p.add_argument("--out-of-order-rate", type=float, default=0.12, help="Probability events for an order arrive out-of-order")
    p.add_argument("--max-late-days", type=int, default=3, help="Max days to shift ingest_time forward for late events")
    p.add_argument("--max-event-gap-minutes", type=int, default=120, help="Max minutes between lifecycle steps")

    p.add_argument("--kafka-bootstrap", type=str, default=None, help="Kafka bootstrap servers, e.g. localhost:9092")
    p.add_argument("--kafka-topic", type=str, default="events", help="Kafka topic name (if kafka-bootstrap set)")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    out_dir = Path(args.out)
    mkdirp(out_dir)

    if args.start:
        start_time = datetime.fromisoformat(args.start.replace("Z", "+00:00")).astimezone(timezone.utc)
    else:
        # default: end at "now", start span_days in the past
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

    # Write "all events" file
    all_path = out_dir / "events.ndjson"
    write_ndjson(all_path, events)

    # Partitioned views for common ingestion / querying patterns
    write_partitioned(out_dir, events, by="ingest_time", subdir="events_by_ingest_date")
    write_partitioned(out_dir, events, by="event_time", subdir="events_by_event_date")

    # Simple stats
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
    with (out_dir / "stats.json").open("w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)

    print(f"Wrote {len(events):,} events to: {all_path}")
    print(f"Wrote partitions to: {out_dir / 'events_by_ingest_date'} and {out_dir / 'events_by_event_date'}")
    print(f"Stats: {out_dir / 'stats.json'}")

    # Optional Kafka publish
    if args.kafka_bootstrap:
        print(f"Publishing to Kafka topic '{args.kafka_topic}' @ {args.kafka_bootstrap} ...")
        try_kafka_produce(events, args.kafka_bootstrap, args.kafka_topic)
        print("Kafka publish complete.")

if __name__ == "__main__":
    main()
