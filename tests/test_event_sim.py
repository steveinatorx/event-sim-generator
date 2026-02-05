"""Tests for event-sim.py"""

import importlib.util
import json
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

# Import the module with hyphenated filename
# Need to register in sys.modules before execution for dataclasses to work
spec = importlib.util.spec_from_file_location(
    "event_sim",
    Path(__file__).parent.parent / "src" / "event-sim.py"
)
event_sim = importlib.util.module_from_spec(spec)
sys.modules["event_sim"] = event_sim  # Register before execution
spec.loader.exec_module(event_sim)

# Import functions from the loaded module
SimConfig = event_sim.SimConfig
clamp = event_sim.clamp
date_str = event_sim.date_str
iso = event_sim.iso
make_base_envelope = event_sim.make_base_envelope
simulate_order_events = event_sim.simulate_order_events
utcnow = event_sim.utcnow
weighted_choice = event_sim.weighted_choice
write_ndjson = event_sim.write_ndjson
write_partitioned = event_sim.write_partitioned


class TestHelpers:
    """Test helper functions"""

    def test_clamp(self):
        assert clamp(5.0, 1.0, 10.0) == 5.0
        assert clamp(0.0, 1.0, 10.0) == 1.0
        assert clamp(15.0, 1.0, 10.0) == 10.0
        assert clamp(-5.0, 0.0, 100.0) == 0.0

    def test_iso(self):
        dt = datetime(2026, 1, 15, 12, 30, 45, tzinfo=timezone.utc)
        result = iso(dt)
        assert result.endswith("Z")
        assert "2026-01-15T12:30:45" in result

    def test_date_str(self):
        dt = datetime(2026, 1, 15, 12, 30, 45, tzinfo=timezone.utc)
        assert date_str(dt) == "2026-01-15"

    def test_weighted_choice(self):
        import random
        rng = random.Random(42)
        items = [("A", 0.5), ("B", 0.3), ("C", 0.2)]
        result = weighted_choice(rng, items)
        assert result in ["A", "B", "C"]
        
        # Test with deterministic seed
        rng2 = random.Random(42)
        result2 = weighted_choice(rng2, items)
        assert result == result2  # Should be deterministic


class TestEnvelope:
    """Test event envelope creation"""

    def test_make_base_envelope(self):
        import random
        rng = random.Random(42)
        dt = datetime.now(timezone.utc)
        
        envelope = make_base_envelope(
            rng=rng,
            event_type="order_created",
            schema_version=1,
            event_time=dt,
            ingest_time=dt,
            tenant_id="T-001",
            user_id="U-00001",
            session_id="S-123456",
            source_system="web",
            environment="prod",
            payload={"order_id": "O-0000001", "amount": 99.99},
        )
        
        assert envelope["event_type"] == "order_created"
        assert envelope["schema_version"] == 1
        assert envelope["tenant_id"] == "T-001"
        assert envelope["user_id"] == "U-00001"
        assert envelope["session_id"] == "S-123456"
        assert envelope["source_system"] == "web"
        assert envelope["environment"] == "prod"
        assert "event_id" in envelope
        assert "checksum" in envelope
        assert "payload" in envelope
        assert envelope["payload"]["order_id"] == "O-0000001"


class TestSimulation:
    """Test event simulation"""

    def test_simulate_order_events_basic(self):
        start_time = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        cfg = SimConfig(
            n_orders=10,
            start_time=start_time,
            span_days=7,
            tenant_count=5,
            user_count=100,
            v2_rate=0.5,
            cancel_rate=0.1,
            ship_rate=0.9,
            correction_rate=0.05,
            late_rate=0.0,  # No late events for simpler testing
            out_of_order_rate=0.0,  # No out-of-order for simpler testing
            max_late_days=3,
            max_event_gap_minutes=60,
            seed=42,
        )
        
        events = simulate_order_events(cfg)
        
        assert len(events) > 0
        assert len(events) >= cfg.n_orders  # At least one event per order
        
        # Check that all events have required fields
        for event in events:
            assert "event_id" in event
            assert "event_type" in event
            assert "event_time" in event
            assert "ingest_time" in event
            assert "payload" in event
            assert event["event_type"] in [
                "order_created",
                "payment_authorized",
                "order_shipped",
                "order_cancelled",
                "order_amount_corrected",
            ]

    def test_simulate_order_events_deterministic(self):
        """Test that same seed produces same results"""
        start_time = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        cfg = SimConfig(
            n_orders=5,
            start_time=start_time,
            span_days=7,
            tenant_count=3,
            user_count=50,
            v2_rate=0.3,
            cancel_rate=0.1,
            ship_rate=0.8,
            correction_rate=0.05,
            late_rate=0.0,
            out_of_order_rate=0.0,
            max_late_days=3,
            max_event_gap_minutes=60,
            seed=12345,
        )
        
        events1 = simulate_order_events(cfg)
        events2 = simulate_order_events(cfg)
        
        # Should produce same number of events
        assert len(events1) == len(events2)
        
        # Event types should match
        types1 = [e["event_type"] for e in events1]
        types2 = [e["event_type"] for e in events2]
        assert types1 == types2


class TestOutput:
    """Test output writing functions"""

    def test_write_ndjson(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.ndjson"
            events = [
                {"event_id": "1", "type": "test", "data": "value1"},
                {"event_id": "2", "type": "test", "data": "value2"},
            ]
            
            write_ndjson(path, events)
            
            assert path.exists()
            lines = path.read_text().strip().split("\n")
            assert len(lines) == 2
            
            # Verify valid JSON
            for line in lines:
                data = json.loads(line)
                assert "event_id" in data

    def test_write_partitioned(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir)
            
            # Create events with different dates
            base_time = datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
            events = [
                {
                    "event_id": "1",
                    "event_type": "test",
                    "event_time": iso(base_time),
                    "ingest_time": iso(base_time),
                },
                {
                    "event_id": "2",
                    "event_type": "test",
                    "event_time": iso(base_time + timedelta(days=1)),
                    "ingest_time": iso(base_time + timedelta(days=1)),
                },
            ]
            
            write_partitioned(out_dir, events, by="event_time", subdir="by_date")
            
            # Check that partitioned files were created
            partition_dir = out_dir / "by_date"
            assert partition_dir.exists()
            
            files = list(partition_dir.glob("*.ndjson"))
            assert len(files) >= 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
