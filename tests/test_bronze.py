"""Tests for the Bronze parser, reconciled against the chaos manifest."""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.bronze import check_record, parse_events, parse_jsonl, write_jsonl
from simulators.chaos import BAD_KINDS, ChaosConfig, ChaosInjector
from simulators.gps_simulator import GPSSimulator
from simulators.payment_simulator import PaymentSimulator
from simulators.trip_simulator import TripSimulator

STREAMS = ["trips", "gps", "payments"]
BAD_CASES = [(stream, kind) for stream, kinds in BAD_KINDS.items() for kind in kinds]


@pytest.fixture(scope="module")
def clean() -> dict[str, list[dict]]:
    trips = TripSimulator(seed=7).generate_events(300)
    return {
        "trips": [e.to_dict() for e in trips],
        "gps": [e.to_dict() for e in GPSSimulator().generate_pings(2)],
        "payments": [e.to_dict() for e in PaymentSimulator().generate_payments(trips)],
    }


@pytest.mark.parametrize("stream", STREAMS)
def test_clean_events_all_pass(clean: dict[str, list[dict]], stream: str) -> None:
    result = parse_events(clean[stream], stream, f"{stream}.jsonl")
    assert result.quarantine == []
    assert len(result.good) == len(clean[stream])


@pytest.mark.parametrize("stream", STREAMS)
def test_quarantine_matches_manifest(clean: dict[str, list[dict]], stream: str) -> None:
    config = ChaosConfig(duplicate_rate=0.05, late_rate=0.05, bad_rate=0.05)
    chaotic, manifest = ChaosInjector(config, seed=11).inject(clean[stream], stream)
    result = parse_events(chaotic, stream, "raw.jsonl")
    assert manifest.bad > 0
    assert len(result.quarantine) == manifest.bad
    assert len(result.good) == manifest.output_count - manifest.bad
    assert result.total == len(chaotic)


@pytest.mark.parametrize(("stream", "kind"), BAD_CASES)
def test_every_bad_kind_is_caught(clean: dict[str, list[dict]], stream: str, kind: str) -> None:
    record = dict(clean[stream][0])
    ChaosInjector(seed=1)._corrupt(record, stream, kind)
    assert check_record(record, stream) is not None


@pytest.mark.parametrize("stream", STREAMS)
def test_duplicates_and_late_pass_through(clean: dict[str, list[dict]], stream: str) -> None:
    config = ChaosConfig(duplicate_rate=0.2, late_rate=0.3, bad_rate=0.0)
    chaotic, manifest = ChaosInjector(config, seed=5).inject(clean[stream], stream)
    result = parse_events(chaotic, stream, "raw.jsonl")
    assert manifest.duplicates > 0
    assert manifest.late > 0
    assert result.quarantine == []
    assert len(result.good) == len(chaotic)


def test_good_records_get_metadata_and_stay_raw(clean: dict[str, list[dict]]) -> None:
    original = clean["trips"][:5]
    snapshot = [dict(r) for r in original]
    stamp = "2026-10-28T10:00:00+00:00"
    result = parse_events(original, "trips", "trips.jsonl", ingested_at=stamp)
    assert original == snapshot
    for raw, good in zip(snapshot, result.good, strict=True):
        assert good["_ingested_at"] == stamp
        assert good["_source_file"] == "trips.jsonl"
        assert {k: v for k, v in good.items() if not k.startswith("_")} == raw


def test_quarantine_keeps_original_and_reason(clean: dict[str, list[dict]]) -> None:
    bad = {**clean["trips"][0], "event_type": "teleported"}
    result = parse_events([bad], "trips", "trips.jsonl")
    assert result.good == []
    entry = result.quarantine[0]
    assert entry["record"] == bad
    assert str(entry["_error_reason"]).startswith("schema:")
    assert entry["_source_file"] == "trips.jsonl"


def test_value_rules_catch_records_that_are_valid_avro(clean: dict[str, list[dict]]) -> None:
    far_north = {**clean["gps"][0], "lat": 150.0}
    too_slow = {**clean["gps"][0], "speed_kmph": -5.0}
    refund = {**clean["payments"][0], "amount": -10.0}
    assert str(check_record(far_north, "gps")).startswith("value:")
    assert str(check_record(too_slow, "gps")).startswith("value:")
    assert str(check_record(refund, "payments")).startswith("value:")


def test_trips_with_surge_use_v2(clean: dict[str, list[dict]]) -> None:
    with_surge = {**clean["trips"][0], "surge_multiplier": 1.5}
    bad_surge = {**clean["trips"][0], "surge_multiplier": "high"}
    assert check_record(with_surge, "trips") is None
    assert check_record(bad_surge, "trips") is not None


def test_jsonl_round_trip_with_broken_lines(clean: dict[str, list[dict]], tmp_path: Path) -> None:
    config = ChaosConfig(duplicate_rate=0.05, late_rate=0.05, bad_rate=0.05)
    chaotic, manifest = ChaosInjector(config, seed=3).inject(clean["trips"], "trips")
    path = tmp_path / "trips.jsonl"
    write_jsonl(path, chaotic)
    with path.open("a", encoding="utf-8") as handle:
        handle.write("{not valid json\n\n")
    result = parse_jsonl(path, "trips")
    assert len(result.quarantine) == manifest.bad + 1
    assert len(result.good) == manifest.output_count - manifest.bad
    assert all(e["_source_file"] == "trips.jsonl" for e in result.quarantine)


def test_unknown_stream_raises() -> None:
    with pytest.raises(ValueError, match="unknown stream"):
        parse_events([{}], "orders", "x.jsonl")


def test_non_object_record_is_quarantined() -> None:
    result = parse_events(["hello", 42], "gps", "gps.jsonl")
    assert len(result.quarantine) == 2
    assert "not a JSON object" in str(result.quarantine[0]["_error_reason"])
