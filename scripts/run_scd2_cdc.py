"""Run SCD Type 2 on captured CDC driver events and print every version.

Input: JSON lines with op, source.table, source.ts_ms, before and after.
The PostgreSQL tables use full_name and have no email, so a small adapter maps them to
the fields ingestion/scd2.py expects. valid_from uses the database change time
(source.ts_ms). A driver whose create event expired from Event Hubs gets its first version
rebuilt from the `before` image of its first update and is marked "(rebuilt)".
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ingestion.pii import MissingSaltError
from ingestion.scd2 import build_dimensions

Row = dict[str, Any]


def adapt_record(record: Row | None) -> Row | None:
    """Map PostgreSQL column names to the names scd2.py expects."""
    if record is None:
        return None
    adapted = dict(record)
    adapted["name"] = adapted.pop("full_name", None)
    adapted.setdefault("email", None)
    return adapted


def to_event(raw: Row) -> Row:
    """Build a scd2-style event that uses the database change time."""
    return {
        "op": raw["op"],
        "ts_ms": raw["source"]["ts_ms"],
        "source": {"table": raw["source"]["table"]},
        "before": adapt_record(raw.get("before")),
        "after": adapt_record(raw.get("after")),
    }


def parse_ms(timestamp: str) -> int:
    """Convert an ISO timestamp such as 2026-11-10T13:19:25.523532Z to epoch ms."""
    moment = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    return int(moment.timestamp() * 1000)


def add_rebuilt_creates(events: list[Row]) -> tuple[list[Row], set[str]]:
    """Rebuild the first version of drivers whose create event is missing."""
    seen: set[str] = set()
    rebuilt: set[str] = set()
    extra: list[Row] = []
    for event in events:
        record = event["after"] or event["before"]
        key = record["driver_id"]
        if key not in seen and event["op"] == "u" and event["before"]:
            before = event["before"]
            extra.append(
                {
                    "op": "c",
                    "ts_ms": parse_ms(before["updated_at"]),
                    "source": event["source"],
                    "before": None,
                    "after": before,
                }
            )
            rebuilt.add(key)
        seen.add(key)
    return extra + events, rebuilt


def stamp(ms: int | None) -> str:
    """Format epoch milliseconds as a UTC time, or a dash when open."""
    if ms is None:
        return "-"
    return datetime.fromtimestamp(ms / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")


def main() -> None:
    parser = argparse.ArgumentParser(description="SCD2 demo on captured CDC driver events.")
    parser.add_argument("events_file", help="JSON lines file with captured CDC events")
    args = parser.parse_args()

    with Path(args.events_file).open(encoding="utf-8-sig") as handle:
        raw_events = [json.loads(line) for line in handle if line.strip()]

    events, rebuilt = add_rebuilt_creates([to_event(raw) for raw in raw_events])
    try:
        rows = build_dimensions(events)["drivers"]
    except MissingSaltError as error:
        sys.exit(str(error))

    versions: dict[str, int] = {}
    for row in rows:
        key = row["driver_id"]
        versions[key] = versions.get(key, 0) + 1
        note = " (rebuilt)" if key in rebuilt and versions[key] == 1 else ""
        print(
            f"{key} v{versions[key]} {row['name']:<14} {row['status']:<5} "
            f"{stamp(row['valid_from'])} -> {stamp(row['valid_to'])} "
            f"current={row['is_current']} phone_hash={row['phone_hash'][:10]}{note}"
        )
    current = sum(1 for row in rows if row["is_current"])
    print(f"Total versions: {len(rows)} | current: {current} | closed: {len(rows) - current}")


if __name__ == "__main__":
    main()
