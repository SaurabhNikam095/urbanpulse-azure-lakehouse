"""Tests for the Silver layer, reconciled against the chaos manifest."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from ingestion.bronze import parse_events
from ingestion.silver import STREAMS, process
from simulators.chaos import ChaosConfig, ChaosInjector, ChaosManifest
from simulators.gps_simulator import GPSSimulator
from simulators.payment_simulator import PaymentSimulator
from simulators.trip_simulator import TripSimulator

TOP = datetime(2026, 10, 28, 10, 0, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def clean() -> dict[str, list[dict]]:
    trips = TripSimulator(seed=7).generate_events(300)
    return {
        "trips": [e.to_dict() for e in trips],
        "gps": [e.to_dict() for e in GPSSimulator().generate_pings(2)],
        "payments": [e.to_dict() for e in PaymentSimulator().generate_payments(trips)],
    }


def bronze_good(events: list[dict], stream: str) -> tuple[list[dict], ChaosManifest]:
    """Inject chaos, run Bronze, and return the good records plus the manifest."""
    config = ChaosConfig(duplicate_rate=0.05, late_rate=0.10, bad_rate=0.02)
    chaotic, manifest = ChaosInjector(config, seed=11).inject(events, stream)
    return parse_events(chaotic, stream, "raw.jsonl").good, manifest


@pytest.mark.parametrize("stream", STREAMS)
def test_conservation_and_duplicates_match_manifest(
    clean: dict[str, list[dict]], stream: str
) -> None:
    bronze, manifest = bronze_good(clean[stream], stream)
    result = process(bronze, stream)
    stats = result.stats
    assert stats.input_count == len(bronze)
    assert stats.silver + stats.late + stats.duplicates == stats.input_count
    assert stats.conserved
    assert stats.duplicates == manifest.duplicates
    assert len(result.silver) == stats.silver
    assert len(result.late_arrivals) == stats.late


@pytest.mark.parametrize("stream", STREAMS)
def test_late_records_are_beyond_allowed_lateness(
    clean: dict[str, list[dict]], stream: str
) -> None:
    bronze, manifest = bronze_good(clean[stream], stream)
    result = process(bronze, stream, allowed_lateness_seconds=300)
    assert 0 < result.stats.late <= manifest.late
    for entry in result.late_arrivals:
        assert entry["_late_by_seconds"] > 300
        assert "_watermark" in entry


@pytest.mark.parametrize("stream", STREAMS)
def test_processing_is_idempotent(clean: dict[str, list[dict]], stream: str) -> None:
    bronze, _ = bronze_good(clean[stream], stream)
    first = process(bronze, stream)
    second = process(bronze, stream)
    assert first == second
    again = process(first.silver, stream)
    assert again.silver == first.silver
    assert again.late_arrivals == []
    assert again.stats.duplicates == 0


def test_watermark_boundary() -> None:
    records = [
        {"driver_id": "D1", "event_ts": TOP.isoformat()},
        {"driver_id": "D2", "event_ts": (TOP - timedelta(seconds=299)).isoformat()},
        {"driver_id": "D3", "event_ts": (TOP - timedelta(seconds=300)).isoformat()},
        {"driver_id": "D4", "event_ts": (TOP - timedelta(seconds=301)).isoformat()},
    ]
    result = process(records, "gps", allowed_lateness_seconds=300)
    assert [r["driver_id"] for r in result.silver] == ["D1", "D2", "D3"]
    assert [r["driver_id"] for r in result.late_arrivals] == ["D4"]
    late = result.late_arrivals[0]
    assert late["_late_by_seconds"] == 301.0
    assert late["_watermark"] == (TOP - timedelta(seconds=300)).isoformat()


def test_trip_duplicates_keep_first_copy() -> None:
    first = {"trip_id": "T1", "event_type": "started", "event_ts": TOP.isoformat(), "note": "1st"}
    repeat = {**first, "note": "2nd"}
    other_stage = {**first, "event_type": "completed", "note": "other stage"}
    result = process([first, repeat, other_stage], "trips")
    assert [r["note"] for r in result.silver] == ["1st", "other stage"]
    assert result.stats.duplicates == 1


def test_payment_duplicates_keep_first_copy() -> None:
    first = {"payment_id": "P1", "event_ts": TOP.isoformat(), "note": "1st"}
    repeat = {**first, "note": "2nd"}
    other = {"payment_id": "P2", "event_ts": TOP.isoformat(), "note": "other payment"}
    result = process([first, repeat, other], "payments")
    assert [r["note"] for r in result.silver] == ["1st", "other payment"]
    assert result.stats.duplicates == 1


def test_gps_key_includes_the_timestamp() -> None:
    ping = {"driver_id": "D1", "event_ts": TOP.isoformat()}
    next_ping = {"driver_id": "D1", "event_ts": (TOP + timedelta(seconds=5)).isoformat()}
    result = process([ping, dict(ping), next_ping], "gps")
    assert len(result.silver) == 2
    assert result.stats.duplicates == 1


def test_event_ts_is_normalised_to_utc() -> None:
    record = {"driver_id": "D1", "event_ts": "2026-10-28T15:30:00+05:30"}
    result = process([record], "gps")
    assert result.silver[0]["event_ts"] == "2026-10-28T10:00:00+00:00"


def test_unknown_stream_raises() -> None:
    with pytest.raises(ValueError, match="unknown stream"):
        process([], "orders")


def test_negative_lateness_raises() -> None:
    with pytest.raises(ValueError, match="must not be negative"):
        process([], "gps", allowed_lateness_seconds=-1)
