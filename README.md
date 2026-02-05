# Event Simulator

An little event simulator for generating e-commerce order lifecycle events. This tool generates event logs with realistic characteristics including late arrivals, out-of-order events, schema evolution, and corrections.

## Features

- **Realistic Event Generation**: Simulates complete order lifecycles from creation to shipping/cancellation
- **Late Arrivals**: Configurable probability of events arriving late (ingest_time after event_time)
- **Out-of-Order Events**: Simulates events arriving in non-chronological order
- **Schema Evolution**: Supports multiple schema versions (v1 and v2 for order_created events)
- **Corrections**: Generates correction events for order amounts
- **Multiple Output Formats**: 
  - Newline-delimited JSON (NDJSON) files
  - Partitioned by ingest date and event date
  - Optional Kafka output

## Installation

### Prerequisites

- Python 3.10 or higher
- pipenv (recommended) or pip

### Setup

```bash
# Using pipenv (recommended)
pipenv install --dev

# Or using pip
pip install pytest kafka-python  # kafka-python only needed if using Kafka output
```

## Usage

### Basic Usage

Generate 5000 orders with default settings:

```bash
python src/event-sim.py --out ./out --n-orders 5000 --seed 42
```

### Advanced Usage

Generate orders with custom late arrival and out-of-order rates:

```bash
python src/event-sim.py --out ./out --n-orders 5000 --late-rate 0.08 --out-of-order-rate 0.15
```

### Kafka Output

Publish events to Kafka:

```bash
python src/event-sim.py --out ./out --n-orders 20000 --kafka-bootstrap localhost:9092 --kafka-topic events
```

### Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--out` | (required) | Output directory for generated files |
| `--n-orders` | 5000 | Number of orders to simulate |
| `--seed` | 42 | Random seed for reproducibility |
| `--start` | None | Start datetime (UTC), e.g. `2026-01-01T00:00:00Z` |
| `--span-days` | 14 | Time span in days for order creation times |
| `--tenant-count` | 25 | Number of distinct tenants |
| `--user-count` | 5000 | Number of distinct users |
| `--v2-rate` | 0.35 | Fraction of order_created events using schema v2 |
| `--cancel-rate` | 0.08 | Fraction of orders that are cancelled |
| `--ship-rate` | 0.85 | Fraction of non-cancelled orders that are shipped |
| `--correction-rate` | 0.10 | Fraction of non-cancelled orders with corrections |
| `--late-rate` | 0.06 | Probability an event arrives late |
| `--out-of-order-rate` | 0.12 | Probability events arrive out-of-order |
| `--max-late-days` | 3 | Maximum days to shift ingest_time for late events |
| `--max-event-gap-minutes` | 120 | Maximum minutes between lifecycle steps |
| `--kafka-bootstrap` | None | Kafka bootstrap servers (e.g., `localhost:9092`) |
| `--kafka-topic` | events | Kafka topic name (if kafka-bootstrap is set) |

## Output Structure

The simulator generates the following output structure:

```
out/
├── events.ndjson                          # All events in a single file
├── events_by_ingest_date/                 # Partitioned by ingest date
│   ├── 2026-01-22.ndjson
│   ├── 2026-01-23.ndjson
│   └── ...
├── events_by_event_date/                  # Partitioned by event date
│   ├── 2026-01-22.ndjson
│   ├── 2026-01-23.ndjson
│   └── ...
└── stats.json                             # Summary statistics
```

## Event Schema

Each event follows this structure:

```json
{
  "event_id": "uuid",
  "event_type": "order_created|payment_authorized|order_shipped|order_cancelled|order_amount_corrected",
  "schema_version": 1,
  "event_time": "2026-01-15T12:30:45Z",
  "ingest_time": "2026-01-15T12:30:46Z",
  "tenant_id": "T-001",
  "user_id": "U-00001",
  "session_id": "S-123456",
  "source_system": "web|ios|android|partner_api",
  "environment": "prod|staging",
  "record_source": "event_sim",
  "payload": { ... },
  "checksum": "hex_string"
}
```

### Event Types

- **order_created**: Initial order creation (schema v1 or v2)
- **payment_authorized**: Payment authorization
- **order_shipped**: Order shipment notification
- **order_cancelled**: Order cancellation
- **order_amount_corrected**: Amount correction event

## Testing

Run tests with pytest:

```bash
# Using pipenv
pipenv run pytest

# Or directly
pytest tests/
```

Run tests with verbose output:

```bash
pytest tests/ -v
```

## Development

### Type Checking

This project uses Pylance (Pyright) for type checking. Configuration is in `pyrightconfig.json`.

### Code Style

The code follows PEP 8 style guidelines and uses type hints throughout.

## Notes

- **Late Events**: A "late" event means `ingest_time` occurs on a later day than `event_time`
- **Out-of-Order Events**: Events for a given order may arrive in a non-chronological sequence
- **Deterministic**: Using the same seed will produce the same sequence of events
- **Schema Evolution**: Schema v2 adds `promo_code` and `device_type` fields to order_created events

## License

This project is part of a portfolio/data engineering demonstration.
