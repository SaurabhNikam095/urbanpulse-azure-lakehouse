"""Bronze layer: check raw events, tag the good ones, quarantine the bad ones.

Bronze keeps the raw fields exactly as they arrived. Duplicates and late events
pass through untouched; Silver deals with them. Demo, from the repo root:

    python -m ingestion.bronze
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import cache
from pathlib import Path

import fastavro
from fastavro.validation import ValidationError, validate

SCHEMA_DIR = Path(__file__).resolve().parent.parent / "schemas"

_SCHEMA_FILES = {
    "trips_v1": "trip_event_v1.avsc",
    "trips_v2": "trip_event_v2.avsc",
    "gps": "gps_event_v1.avsc",
    "payments": "payment_event_v1.avsc",
}

# Value rules for things that are valid Avro but still wrong.
RULES: dict[str, dict[str, tuple[str, ...]]] = {
    "trips": {
        "lat": ("pickup_lat", "drop_lat"),
        "lng": ("pickup_lng", "drop_lng"),
        "non_negative": ("fare",),
    },
    "gps": {"lat": ("lat",), "lng": ("lng",), "non_negative": ("speed_kmph",)},
    "payments": {"lat": (), "lng": (), "non_negative": ("amount",)},
}


@dataclass
class BronzeResult:
    """Good records (with metadata) and quarantined records (with a reason)."""

    good: list[dict[str, object]] = field(default_factory=list)
    quarantine: list[dict[str, object]] = field(default_factory=list)

    @property
    def total(self) -> int:
        """Every input record ends up in exactly one of the two lists."""
        return len(self.good) + len(self.quarantine)


@cache
def _schema(key: str) -> dict:
    raw = json.loads((SCHEMA_DIR / _SCHEMA_FILES[key]).read_text(encoding="utf-8"))
    return fastavro.parse_schema(raw)


def _schema_key(record: dict[str, object], stream: str) -> str:
    if stream == "trips":
        return "trips_v2" if "surge_multiplier" in record else "trips_v1"
    return stream


def check_record(record: object, stream: str) -> str | None:
    """Return None if the record is good, otherwise the reason it is bad."""
    if stream not in RULES:
        raise ValueError(f"unknown stream {stream!r}; use one of {sorted(RULES)}")
    if not isinstance(record, dict):
        return "record is not a JSON object"

    try:
        validate(record, _schema(_schema_key(record, stream)), raise_errors=True)
    except ValidationError as exc:
        return f"schema: {exc}"

    rules = RULES[stream]
    for name in rules["lat"]:
        if not -90 <= record[name] <= 90:
            return f"value: {name}={record[name]} is outside -90..90"
    for name in rules["lng"]:
        if not -180 <= record[name] <= 180:
            return f"value: {name}={record[name]} is outside -180..180"
    for name in rules["non_negative"]:
        if record[name] is not None and record[name] < 0:
            return f"value: {name}={record[name]} is negative"
    try:
        datetime.fromisoformat(record["event_ts"])
    except ValueError:
        return "value: event_ts is not ISO 8601"
    return None


def _quarantine_entry(reason: str, stamp: str, source_file: str, record: object) -> dict:
    return {
        "_error_reason": reason,
        "_ingested_at": stamp,
        "_source_file": source_file,
        "record": record,
    }


def parse_events(
    events: Iterable[object], stream: str, source_file: str, ingested_at: str | None = None
) -> BronzeResult:
    """Split events into good (tagged with metadata) and quarantined (with a reason)."""
    stamp = ingested_at or datetime.now(UTC).isoformat()
    result = BronzeResult()
    for record in events:
        reason = check_record(record, stream)
        if reason is None:
            result.good.append({**record, "_ingested_at": stamp, "_source_file": source_file})
        else:
            result.quarantine.append(_quarantine_entry(reason, stamp, source_file, record))
    return result


def write_jsonl(path: Path, records: Iterable[object]) -> None:
    """Write one JSON object per line."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")


def parse_jsonl(path: Path, stream: str, ingested_at: str | None = None) -> BronzeResult:
    """Read a JSON lines file and parse it. Lines that are not valid JSON are quarantined."""
    stamp = ingested_at or datetime.now(UTC).isoformat()
    records: list[object] = []
    broken: list[dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError as exc:
            broken.append(_quarantine_entry(f"json: {exc}", stamp, path.name, line))
    result = parse_events(records, stream, path.name, stamp)
    result.quarantine.extend(broken)
    return result


def main() -> None:
    """Generate chaos data, write raw files, parse them, and print the counts."""
    from simulators.chaos import ChaosInjector
    from simulators.gps_simulator import GPSSimulator
    from simulators.payment_simulator import PaymentSimulator
    from simulators.trip_simulator import TripSimulator

    base = Path("data")
    trip_events = TripSimulator(seed=42).generate_events(200)
    raw = {
        "trips": [e.to_dict() for e in trip_events],
        "gps": [e.to_dict() for e in GPSSimulator().generate_pings(2)],
        "payments": [e.to_dict() for e in PaymentSimulator().generate_payments(trip_events)],
    }
    injector = ChaosInjector(seed=42)
    for stream, events in raw.items():
        chaotic, manifest = injector.inject(events, stream)
        raw_path = base / "raw" / f"{stream}.jsonl"
        write_jsonl(raw_path, chaotic)
        result = parse_jsonl(raw_path, stream)
        write_jsonl(base / "bronze" / f"{stream}.jsonl", result.good)
        write_jsonl(base / "quarantine" / f"{stream}.jsonl", result.quarantine)
        print(
            f"{stream:9} input={manifest.input_count} duplicates={manifest.duplicates} "
            f"late={manifest.late} bad={manifest.bad} | "
            f"bronze={len(result.good)} quarantine={len(result.quarantine)}"
        )


if __name__ == "__main__":
    main()
