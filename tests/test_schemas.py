"""Tests for the Avro data contracts and the validation script."""

from __future__ import annotations

import io
import json

import fastavro
import pytest
from fastavro.validation import validate

from scripts.validate_schemas import (
    SCHEMA_DIR,
    build_samples,
    check_parse,
    errors_for,
    load_schema,
    main,
)


@pytest.fixture(scope="module")
def samples() -> dict[str, list[dict]]:
    return build_samples()


def test_all_schemas_parse() -> None:
    assert check_parse() == []
    assert len(list(SCHEMA_DIR.glob("*.avsc"))) == 4


def test_validation_script_passes() -> None:
    assert main() == 0


def test_v2_reads_v1_data_with_default_surge(samples: dict[str, list[dict]]) -> None:
    v1 = load_schema("trip_event_v1.avsc")
    v2 = load_schema("trip_event_v2.avsc")
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, v1, samples["trips"][0])
    buffer.seek(0)
    result = fastavro.schemaless_reader(buffer, v1, v2)
    assert result["surge_multiplier"] == 1.0


def test_v2_surge_field_is_nullable_with_default() -> None:
    raw = json.loads((SCHEMA_DIR / "trip_event_v2.avsc").read_text(encoding="utf-8"))
    field = next(f for f in raw["fields"] if f["name"] == "surge_multiplier")
    assert field["default"] == 1.0
    assert "null" in field["type"]


def test_trip_events_match_v1(samples: dict[str, list[dict]]) -> None:
    assert errors_for(samples["trips"], load_schema("trip_event_v1.avsc"), "trips v1") == []


def test_trip_events_with_surge_match_v2(samples: dict[str, list[dict]]) -> None:
    trips = [{**t, "surge_multiplier": 1.8} for t in samples["trips"]]
    assert errors_for(trips, load_schema("trip_event_v2.avsc"), "trips v2") == []


def test_gps_events_match_v1(samples: dict[str, list[dict]]) -> None:
    assert errors_for(samples["gps"], load_schema("gps_event_v1.avsc"), "gps v1") == []


def test_payment_events_match_v1(samples: dict[str, list[dict]]) -> None:
    schema = load_schema("payment_event_v1.avsc")
    assert errors_for(samples["payments"], schema, "payments v1") == []


def test_bad_enum_value_is_rejected(samples: dict[str, list[dict]]) -> None:
    bad = {**samples["trips"][0], "event_type": "teleported"}
    assert validate(bad, load_schema("trip_event_v1.avsc"), raise_errors=False) is False


def test_missing_required_field_is_rejected(samples: dict[str, list[dict]]) -> None:
    bad = {k: v for k, v in samples["payments"][0].items() if k != "payment_id"}
    assert validate(bad, load_schema("payment_event_v1.avsc"), raise_errors=False) is False
