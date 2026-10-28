"""Validate the Avro data contracts in schemas/ against the simulators.

Run from the repo root:  python -m scripts.validate_schemas
Exit code 0 means every check passed, 1 means at least one failed.
"""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path

import fastavro
from fastavro.validation import ValidationError, validate

from simulators.gps_simulator import GPSSimulator
from simulators.payment_simulator import PaymentSimulator
from simulators.trip_simulator import TripSimulator

SCHEMA_DIR = Path(__file__).resolve().parent.parent / "schemas"
SAMPLE_TRIPS = 200
SAMPLE_TICKS = 5
SAMPLE_SURGE = 1.5


def load_schema(file_name: str) -> dict:
    """Read one .avsc file and return the parsed Avro schema."""
    raw = json.loads((SCHEMA_DIR / file_name).read_text(encoding="utf-8"))
    return fastavro.parse_schema(raw)


def build_samples() -> dict[str, list[dict]]:
    """Generate sample events from the real simulators, as plain dictionaries."""
    trip_events = TripSimulator().generate_events(SAMPLE_TRIPS)
    pings = GPSSimulator().generate_pings(SAMPLE_TICKS)
    payments = PaymentSimulator().generate_payments(trip_events)
    return {
        "trips": [e.to_dict() for e in trip_events],
        "gps": [e.to_dict() for e in pings],
        "payments": [e.to_dict() for e in payments],
    }


def errors_for(records: list[dict], schema: dict, label: str) -> list[str]:
    """Return one error message for every record that does not match the schema."""
    if not records:
        return [f"{label}: no sample records were generated"]
    errors = []
    for index, record in enumerate(records):
        try:
            validate(record, schema, raise_errors=True)
        except ValidationError as exc:
            errors.append(f"{label} record {index}: {exc}")
    return errors


def check_parse() -> list[str]:
    """Every .avsc file must be valid JSON and a valid Avro schema."""
    paths = sorted(SCHEMA_DIR.glob("*.avsc"))
    if not paths:
        return ["no .avsc files found in schemas/"]
    errors = []
    for path in paths:
        try:
            load_schema(path.name)
        except Exception as exc:
            errors.append(f"{path.name}: {exc}")
    return errors


def check_v1_to_v2() -> list[str]:
    """A v2 reader must read v1 data, filling surge_multiplier with 1.0."""
    v1 = load_schema("trip_event_v1.avsc")
    v2 = load_schema("trip_event_v2.avsc")
    record = TripSimulator().generate_events(1)[0].to_dict()
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, v1, record)
    buffer.seek(0)
    result = fastavro.schemaless_reader(buffer, v1, v2)
    if result.get("surge_multiplier") != 1.0:
        return [f"v2 reader gave surge_multiplier={result.get('surge_multiplier')!r}, expected 1.0"]
    return []


def check_samples() -> list[str]:
    """Simulator output must match the schemas."""
    samples = build_samples()
    trips_v2 = [{**t, "surge_multiplier": SAMPLE_SURGE} for t in samples["trips"]]
    errors = []
    errors += errors_for(samples["trips"], load_schema("trip_event_v1.avsc"), "trips v1")
    errors += errors_for(trips_v2, load_schema("trip_event_v2.avsc"), "trips v2")
    errors += errors_for(samples["gps"], load_schema("gps_event_v1.avsc"), "gps v1")
    errors += errors_for(samples["payments"], load_schema("payment_event_v1.avsc"), "payments v1")
    return errors


CHECKS = [
    ("schemas parse", check_parse),
    ("v2 reads v1 data", check_v1_to_v2),
    ("simulator samples match schemas", check_samples),
]


def main() -> int:
    """Run all checks, print a result line for each, and return the exit code."""
    failed = False
    for name, check in CHECKS:
        errors = check()
        if errors:
            failed = True
            print(f"FAIL {name}")
            for message in errors[:5]:
                print(f"     {message}")
        else:
            print(f"OK   {name}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
