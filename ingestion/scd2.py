"""SCD Type 2 for drivers and riders from Debezium-style CDC events.

Pure Python. Phone and email are hashed (Stage 6.1) before they enter a row,
and row_hash is computed over the hashed values, so no raw PII is stored.

Method: rebuild a per-key timeline from the existing rows, insert the new
event at its ts_ms position, then regenerate the rows. This handles
duplicates, no-change updates and out-of-order events with one code path.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ingestion.pii import hash_pii

Row = dict[str, Any]
Timeline = list[tuple[int, Row | None]]  # (ts_ms, state) ; None = deleted

META_FIELDS = frozenset({"valid_from", "valid_to", "is_current", "row_hash", "source_ts_ms"})
VALID_OPS = frozenset({"c", "u", "d"})


@dataclass(frozen=True)
class DimensionSpec:
    table: str
    key: str
    plain_fields: tuple[str, ...]  # stored as-is; phone and email are hashed

    @property
    def tracked_fields(self) -> tuple[str, ...]:
        return (*self.plain_fields, "phone_hash", "email_hash")


DRIVERS = DimensionSpec("drivers", "driver_id", ("name", "vehicle_type", "rating", "status"))
RIDERS = DimensionSpec("riders", "rider_id", ("name", "status"))
SPECS = {spec.table: spec for spec in (DRIVERS, RIDERS)}


def build_state(spec: DimensionSpec, record: Row, salt: bytes | None = None) -> Row:
    """Turn a raw CDC record into a state with hashed PII (no raw values)."""
    state: Row = {spec.key: record[spec.key]}
    for field in spec.plain_fields:
        state[field] = record.get(field)
    state["phone_hash"] = hash_pii(record.get("phone"), salt=salt)
    state["email_hash"] = hash_pii(record.get("email"), is_email=True, salt=salt)
    state["updated_at"] = record.get("updated_at")
    return state


def compute_row_hash(spec: DimensionSpec, state: Row) -> str:
    """Hash of the tracked (hashed-PII) attributes. updated_at is excluded."""
    payload = {field: state.get(field) for field in spec.tracked_fields}
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _state_of(row: Row) -> Row:
    return {k: v for k, v in row.items() if k not in META_FIELDS}


def _timeline_from_rows(rows: list[Row]) -> Timeline:
    ordered = sorted(rows, key=lambda r: r["valid_from"])
    timeline: Timeline = []
    for i, row in enumerate(ordered):
        timeline.append((row["valid_from"], _state_of(row)))
        valid_to = row["valid_to"]
        if valid_to is not None:
            nxt = ordered[i + 1] if i + 1 < len(ordered) else None
            if nxt is None or nxt["valid_from"] != valid_to:
                timeline.append((valid_to, None))  # a delete closed this row
    return timeline


def _insert(timeline: Timeline, ts_ms: int, state: Row | None) -> None:
    for i, (ts, _) in enumerate(timeline):
        if ts == ts_ms:
            timeline[i] = (ts_ms, state)  # same ts: identical = no-op
            return
    timeline.append((ts_ms, state))
    timeline.sort(key=lambda entry: entry[0])


def _rebuild(spec: DimensionSpec, timeline: Timeline) -> list[Row]:
    entries: Timeline = []
    open_hash: str | None = None  # hash of the open state; None = nothing open
    for ts, state in timeline:
        if state is None:
            if open_hash is None:
                continue  # delete with nothing open
            entries.append((ts, None))
            open_hash = None
        else:
            row_hash = compute_row_hash(spec, state)
            if row_hash == open_hash:
                continue  # no-change update
            entries.append((ts, state))
            open_hash = row_hash

    rows: list[Row] = []
    for i, (ts, state) in enumerate(entries):
        if state is None:
            continue
        valid_to = entries[i + 1][0] if i + 1 < len(entries) else None
        row = dict(state)
        row.update(
            valid_from=ts,
            valid_to=valid_to,
            is_current=valid_to is None,
            row_hash=compute_row_hash(spec, state),
            source_ts_ms=ts,
        )
        rows.append(row)
    return rows


def apply_event(
    rows: list[Row],
    event: Row,
    spec: DimensionSpec,
    *,
    salt: bytes | None = None,
) -> list[Row]:
    """Apply one CDC event to the rows of ONE business key. Returns new rows."""
    op = event.get("op")
    ts_ms = event.get("ts_ms")
    if op not in VALID_OPS:
        raise ValueError(f"Unknown op: {op!r}")
    if ts_ms is None:
        raise ValueError("Event has no ts_ms")

    timeline = _timeline_from_rows(rows)
    if op == "d":
        _insert(timeline, ts_ms, None)
    else:
        after = event.get("after")
        if not after:
            raise ValueError(f"op {op!r} needs an 'after' record")
        _insert(timeline, ts_ms, build_state(spec, after, salt))
    return _rebuild(spec, timeline)


def build_dimensions(events: Iterable[Row], *, salt: bytes | None = None) -> dict[str, list[Row]]:
    """Apply many events (drivers and riders mixed). Returns rows per table."""
    store: dict[tuple[str, Any], list[Row]] = {}
    for event in events:
        table = (event.get("source") or {}).get("table")
        if table not in SPECS:
            raise ValueError(f"Unknown table: {table!r}")
        spec = SPECS[table]
        record = event.get("after") or event.get("before")
        if not record:
            raise ValueError("Event has neither 'after' nor 'before'")
        key = (table, record[spec.key])
        store[key] = apply_event(store.get(key, []), event, spec, salt=salt)

    result: dict[str, list[Row]] = {table: [] for table in SPECS}
    for (table, _), rows in sorted(store.items(), key=lambda kv: str(kv[0])):
        result[table].extend(rows)
    return result


def read_jsonl(path: str | Path) -> list[Row]:
    with Path(path).open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]
