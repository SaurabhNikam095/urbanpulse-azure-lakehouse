import json
from pathlib import Path

import pytest

from ingestion.pii import hash_pii
from ingestion.scd2 import (
    DRIVERS,
    META_FIELDS,
    RIDERS,
    apply_event,
    build_dimensions,
    compute_row_hash,
    read_jsonl,
)

SALT = b"test-salt"
SAMPLE = Path(__file__).resolve().parents[1] / "data/samples/cdc_drivers_riders.jsonl"

BASE = {
    "driver_id": "D001",
    "name": "Ravi Kumar",
    "phone": "+919876500001",
    "email": "Ravi.Kumar@example.com",
    "vehicle_type": "sedan",
    "rating": 4.5,
    "status": "active",
    "updated_at": "2026-10-01T08:00:00Z",
}


def driver_event(op, ts, **changes):
    record = {**BASE, **changes}
    if op == "d":
        return {
            "op": op,
            "ts_ms": ts,
            "source": {"table": "drivers"},
            "before": record,
            "after": None,
        }
    return {"op": op, "ts_ms": ts, "source": {"table": "drivers"}, "before": None, "after": record}


def run(events, spec=DRIVERS, salt=SALT):
    rows = []
    for event in events:
        rows = apply_event(rows, event, spec, salt=salt)
    return rows


def test_create_opens_current_row():
    rows = run([driver_event("c", 1000)])
    assert len(rows) == 1
    assert rows[0]["valid_from"] == 1000
    assert rows[0]["valid_to"] is None
    assert rows[0]["is_current"] is True


def test_update_closes_old_and_opens_new():
    rows = run([driver_event("c", 1000), driver_event("u", 2000, status="inactive")])
    assert len(rows) == 2
    assert rows[0]["valid_to"] == 2000
    assert rows[0]["is_current"] is False
    assert rows[1]["valid_from"] == 2000
    assert rows[1]["is_current"] is True
    assert rows[1]["status"] == "inactive"


def test_delete_closes_current_row():
    rows = run([driver_event("c", 1000), driver_event("d", 3000)])
    assert len(rows) == 1
    assert rows[0]["valid_to"] == 3000
    assert rows[0]["is_current"] is False


def test_create_after_delete_reopens():
    rows = run([driver_event("c", 1000), driver_event("d", 2000), driver_event("c", 3000)])
    assert len(rows) == 2
    assert rows[0]["valid_to"] == 2000
    assert rows[1]["valid_from"] == 3000
    assert rows[1]["is_current"] is True


def test_no_change_update_ignored():
    rows = run(
        [
            driver_event("c", 1000),
            driver_event("u", 2000, updated_at="2026-10-02T00:00:00Z"),
        ]
    )
    assert len(rows) == 1
    assert rows[0]["is_current"] is True


def test_email_normalization_counts_as_no_change():
    rows = run(
        [
            driver_event("c", 1000),
            driver_event("u", 2000, email=" RAVI.KUMAR@EXAMPLE.COM "),
        ]
    )
    assert len(rows) == 1


def test_duplicate_event_ignored():
    events = [driver_event("c", 1000), driver_event("u", 2000, status="inactive")]
    assert run([*events, events[1]]) == run(events)


def test_replaying_events_is_idempotent():
    events = [driver_event("c", 1000), driver_event("u", 2000, status="inactive")]
    assert run(events + events) == run(events)


def test_out_of_order_update_matches_in_order():
    ordered = [
        driver_event("c", 1000),
        driver_event("u", 2000, rating=4.7),
        driver_event("u", 3000, rating=4.7, status="suspended"),
    ]
    shuffled = [ordered[0], ordered[2], ordered[1]]
    rows = run(shuffled)
    assert rows == run(ordered)
    assert [(r["valid_from"], r["valid_to"]) for r in rows] == [
        (1000, 2000),
        (2000, 3000),
        (3000, None),
    ]


def test_out_of_order_delete():
    rows = run(
        [
            driver_event("c", 1000),
            driver_event("u", 3000, rating=4.7),
            driver_event("d", 2000),
        ]
    )
    assert len(rows) == 2
    assert (rows[0]["valid_from"], rows[0]["valid_to"]) == (1000, 2000)
    assert rows[1]["valid_from"] == 3000
    assert rows[1]["is_current"] is True


def test_row_hash_changes_when_attribute_changes():
    rows = run([driver_event("c", 1000), driver_event("u", 2000, status="inactive")])
    assert rows[0]["row_hash"] != rows[1]["row_hash"]


def test_no_raw_pii_in_rows():
    rows = run([driver_event("c", 1000)])
    dumped = json.dumps(rows)
    assert "+919876500001" not in dumped
    assert "example.com" not in dumped.lower()
    assert "phone" not in rows[0]
    assert "email" not in rows[0]
    assert rows[0]["phone_hash"] == hash_pii(BASE["phone"], salt=SALT)
    assert rows[0]["email_hash"] == hash_pii(BASE["email"], is_email=True, salt=SALT)


def test_row_hash_uses_hashed_values():
    rows = run([driver_event("c", 1000)])
    state = {k: v for k, v in rows[0].items() if k not in META_FIELDS}
    assert compute_row_hash(DRIVERS, state) == rows[0]["row_hash"]
    other = run([driver_event("c", 1000)], salt=b"other-salt")
    assert other[0]["row_hash"] != rows[0]["row_hash"]


def test_rider_rows_have_no_driver_fields():
    event = {
        "op": "c",
        "ts_ms": 1000,
        "source": {"table": "riders"},
        "before": None,
        "after": {
            "rider_id": "R001",
            "name": "Meera Joshi",
            "phone": "+919876500101",
            "email": "meera@example.com",
            "status": "active",
            "updated_at": "2026-10-01T08:00:10Z",
        },
    }
    rows = run([event], spec=RIDERS)
    assert rows[0]["rider_id"] == "R001"
    assert "vehicle_type" not in rows[0]
    assert "rating" not in rows[0]


def test_unknown_op_raises():
    with pytest.raises(ValueError):
        apply_event([], {"op": "x", "ts_ms": 1}, DRIVERS, salt=SALT)


def test_update_without_after_raises():
    with pytest.raises(ValueError):
        apply_event([], {"op": "u", "ts_ms": 1, "after": None}, DRIVERS, salt=SALT)


def test_unknown_table_raises():
    event = {"op": "c", "ts_ms": 1, "source": {"table": "cars"}, "after": {"x": 1}}
    with pytest.raises(ValueError):
        build_dimensions([event], salt=SALT)


def test_sample_file_end_to_end():
    dims = build_dimensions(read_jsonl(SAMPLE), salt=SALT)
    d1 = [r for r in dims["drivers"] if r["driver_id"] == "D001"]
    d2 = [r for r in dims["drivers"] if r["driver_id"] == "D002"]
    r1 = [r for r in dims["riders"] if r["rider_id"] == "R001"]
    r2 = [r for r in dims["riders"] if r["rider_id"] == "R002"]

    assert len(d1) == 3  # create, rating change, suspended (dup and no-change ignored)
    assert not any(r["is_current"] for r in d1)  # deleted
    assert d1[-1]["valid_to"] == 1760000600000
    assert len(d2) == 3  # out-of-order phone change placed correctly
    assert d2[1]["valid_from"] == 1760000400000
    assert d2[-1]["is_current"] is True
    assert len(r1) == 2
    assert len(r2) == 1 and r2[0]["is_current"] is False
    assert "example.com" not in json.dumps(dims).lower()
