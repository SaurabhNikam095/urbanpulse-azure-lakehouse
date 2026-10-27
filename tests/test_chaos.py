"""Unit tests for chaos injection."""

import copy
from datetime import UTC, datetime

import pytest

from simulators.chaos import BAD_KINDS, ChaosConfig, ChaosInjector
from simulators.gps_simulator import GPSSimulator
from simulators.payment_simulator import PaymentSimulator
from simulators.trip_simulator import TripSimulator

START = datetime(2026, 11, 1, 8, 0, tzinfo=UTC)
VALID_TYPES = {"requested", "accepted", "started", "completed", "cancelled"}


def trip_dicts(count: int = 6000):
    trips = TripSimulator(seed=7, start_time=START).generate_events(count)
    return [event.to_dict() for event in trips]


def is_bad_trip(record) -> bool:
    return (
        record["pickup_lat"] is None
        or record["pickup_lng"] is None
        or abs(record["pickup_lat"]) > 90
        or (record["fare"] is not None and record["fare"] < 0)
        or record["event_type"] not in VALID_TYPES
    )


def test_manifest_counts_are_consistent_with_the_output():
    events = trip_dicts()
    output, manifest = ChaosInjector(seed=1).inject(events, "trips")
    assert manifest.input_count == len(events)
    assert len(output) == manifest.output_count == len(events) + manifest.duplicates
    assert manifest.bad == sum(manifest.bad_by_kind.values())


def test_rates_are_close_to_the_configuration():
    events = trip_dicts()
    _, manifest = ChaosInjector(seed=1).inject(events, "trips")
    total = manifest.input_count
    assert 0.014 <= manifest.duplicates / total <= 0.026
    assert 0.022 <= manifest.late / total <= 0.038
    assert 0.006 <= manifest.bad / total <= 0.014


def test_duplicates_are_exact_copies_right_after_the_original():
    events = trip_dicts()
    output, manifest = ChaosInjector(seed=1).inject(events, "trips")
    repeats = sum(1 for before, after in zip(output, output[1:], strict=False) if before == after)
    assert manifest.duplicates > 0
    assert repeats == manifest.duplicates


def test_late_events_are_one_to_ten_minutes_older():
    events = trip_dicts()
    original_ts = {(e["trip_id"], e["event_type"]): e["event_ts"] for e in events}
    output, manifest = ChaosInjector(seed=1).inject(events, "trips")
    late_count = 0
    for record in output:
        if is_bad_trip(record):
            continue
        before = datetime.fromisoformat(original_ts[(record["trip_id"], record["event_type"])])
        after = datetime.fromisoformat(record["event_ts"])
        if after != before:
            late_count += 1
            assert 60 <= (before - after).total_seconds() <= 600
    assert late_count >= manifest.late


def test_bad_trip_records_match_the_manifest():
    events = trip_dicts()
    output, manifest = ChaosInjector(seed=1).inject(events, "trips")
    unique_bad = {id(record) for record in output if is_bad_trip(record)}
    assert len(unique_bad) == manifest.bad
    assert set(manifest.bad_by_kind) <= set(BAD_KINDS["trips"])
    assert set(manifest.bad_by_kind) == set(BAD_KINDS["trips"])


def test_input_is_not_modified():
    events = trip_dicts(500)
    snapshot = copy.deepcopy(events)
    config = ChaosConfig(duplicate_rate=0.5, late_rate=0.3, bad_rate=0.2)
    ChaosInjector(config, seed=1).inject(events, "trips")
    assert events == snapshot


def test_same_seed_gives_same_output():
    events = trip_dicts(500)
    first = ChaosInjector(seed=9).inject(events, "trips")
    second = ChaosInjector(seed=9).inject(events, "trips")
    assert first[0] == second[0]
    assert first[1] == second[1]


def test_zero_rates_change_nothing():
    events = trip_dicts(500)
    config = ChaosConfig(duplicate_rate=0, late_rate=0, bad_rate=0)
    output, manifest = ChaosInjector(config, seed=1).inject(events, "trips")
    assert output == events
    assert (manifest.duplicates, manifest.late, manifest.bad) == (0, 0, 0)


def test_gps_stream_gets_gps_specific_problems():
    pings = GPSSimulator(seed=3, num_drivers=100, start_time=START).generate_pings(100)
    events = [ping.to_dict() for ping in pings]
    output, manifest = ChaosInjector(seed=1).inject(events, "gps")
    assert manifest.bad > 0
    assert set(manifest.bad_by_kind) == set(BAD_KINDS["gps"])
    assert any(record["status"] == "sleeping" for record in output)


def test_payment_stream_gets_payment_specific_problems():
    trips = TripSimulator(seed=7, start_time=START).generate_events(8000)
    payments = PaymentSimulator(seed=1).generate_payments(trips)
    events = [payment.to_dict() for payment in payments]
    _, manifest = ChaosInjector(seed=1).inject(events, "payments")
    assert manifest.bad > 0
    assert set(manifest.bad_by_kind) == set(BAD_KINDS["payments"])


def test_invalid_configuration_and_stream_raise_value_error():
    with pytest.raises(ValueError):
        ChaosConfig(duplicate_rate=1.5)
    with pytest.raises(ValueError):
        ChaosConfig(late_rate=0.7, bad_rate=0.7)
    with pytest.raises(ValueError):
        ChaosConfig(min_delay_seconds=600, max_delay_seconds=60)
    with pytest.raises(ValueError):
        ChaosInjector().inject([], "orders")
