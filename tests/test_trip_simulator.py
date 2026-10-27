"""Unit tests for the trip simulator."""

from datetime import UTC, datetime

import pytest

from simulators.models import EventType
from simulators.trip_simulator import (
    LAT_RANGE,
    LNG_RANGE,
    MIN_FARE,
    TripSimulator,
    calculate_fare,
    haversine_km,
)

START = datetime(2026, 11, 1, 8, 0, tzinfo=UTC)

R = EventType.REQUESTED
A = EventType.ACCEPTED
S = EventType.STARTED
C = EventType.COMPLETED
X = EventType.CANCELLED

VALID_PATHS = [
    [R, A, S, C],
    [R, X],
    [R, A, X],
]


def make_simulator(seed: int = 7, **kwargs) -> TripSimulator:
    return TripSimulator(seed=seed, start_time=START, **kwargs)


def test_event_order_is_valid():
    for trip in make_simulator().generate_trips(1000):
        assert [event.event_type for event in trip] in VALID_PATHS


def test_cancelled_trips_have_no_fare_and_end_in_cancelled():
    trips = make_simulator().generate_trips(1000)
    cancelled = [trip for trip in trips if trip[-1].event_type == X]
    assert cancelled
    for trip in cancelled:
        assert all(event.fare is None for event in trip)
        assert EventType.COMPLETED not in [event.event_type for event in trip]


def test_fare_only_on_completed_events():
    for event in make_simulator().generate_events(1000):
        if event.event_type == C:
            assert event.fare is not None
            assert event.fare >= MIN_FARE
        else:
            assert event.fare is None


def test_timestamps_increase_within_a_trip():
    for trip in make_simulator().generate_trips(1000):
        times = [event.event_ts for event in trip]
        assert times == sorted(times)
        assert len(set(times)) == len(times)


def test_coordinates_are_inside_pune_box():
    for event in make_simulator().generate_events(1000):
        for lat in (event.pickup_lat, event.drop_lat):
            assert LAT_RANGE[0] <= lat <= LAT_RANGE[1]
        for lng in (event.pickup_lng, event.drop_lng):
            assert LNG_RANGE[0] <= lng <= LNG_RANGE[1]


def test_same_seed_gives_same_output():
    first = [e.to_dict() for e in make_simulator(seed=123).generate_events(200)]
    second = [e.to_dict() for e in make_simulator(seed=123).generate_events(200)]
    other = [e.to_dict() for e in make_simulator(seed=124).generate_events(200)]
    assert first == second
    assert first != other


def test_trip_ids_are_unique():
    trips = make_simulator().generate_trips(1000)
    trip_ids = [trip[0].trip_id for trip in trips]
    assert len(set(trip_ids)) == len(trip_ids)


def test_driver_is_assigned_from_accepted_onward():
    for trip in make_simulator().generate_trips(1000):
        assert trip[0].driver_id is None
        assigned = {event.driver_id for event in trip[1:] if event.event_type != X}
        if trip[-1].event_type == X and len(trip) == 2:
            assert trip[1].driver_id is None
        else:
            assert len(assigned) == 1
            assert None not in assigned


def test_each_trip_keeps_the_same_rider_and_route():
    for trip in make_simulator().generate_trips(500):
        assert len({event.rider_id for event in trip}) == 1
        assert len({(event.pickup_lat, event.pickup_lng) for event in trip}) == 1
        assert len({(event.drop_lat, event.drop_lng) for event in trip}) == 1


def test_cancel_rate_is_close_to_the_setting():
    trips = make_simulator(cancel_rate=0.12).generate_trips(5000)
    rate = sum(1 for trip in trips if trip[-1].event_type == X) / len(trips)
    assert 0.10 <= rate <= 0.14


def test_zero_cancel_rate_means_every_trip_completes():
    for trip in make_simulator(cancel_rate=0.0).generate_trips(200):
        assert trip[-1].event_type == C


def test_generate_events_are_sorted_by_time():
    events = make_simulator().generate_events(500)
    times = [event.event_ts for event in events]
    assert times == sorted(times)


def test_to_dict_is_json_friendly():
    event = make_simulator().generate_trip()[0]
    data = event.to_dict()
    assert data["event_type"] == "requested"
    assert isinstance(data["event_ts"], str)
    assert data["driver_id"] is None


def test_fare_formula_and_minimum_fare():
    assert calculate_fare(10.0, 30.0) == 205.0
    assert calculate_fare(0.5, 2.0) == MIN_FARE


def test_haversine_pune_to_mumbai_is_about_120_km():
    distance = haversine_km(18.5204, 73.8567, 19.0760, 72.8777)
    assert 115 < distance < 125


def test_invalid_arguments_raise_value_error():
    with pytest.raises(ValueError):
        TripSimulator(num_drivers=0)
    with pytest.raises(ValueError):
        TripSimulator(num_riders=0)
    with pytest.raises(ValueError):
        TripSimulator(cancel_rate=1.5)
    with pytest.raises(ValueError):
        make_simulator().generate_trips(-1)
