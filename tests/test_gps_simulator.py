"""Unit tests for the GPS simulator."""

from collections import Counter, defaultdict
from datetime import UTC, datetime

import pytest

from simulators.gps_simulator import GPSSimulator
from simulators.models import DriverStatus
from simulators.trip_simulator import LAT_RANGE, LNG_RANGE, haversine_km

START = datetime(2026, 11, 1, 8, 0, tzinfo=UTC)


def make_simulator(seed: int = 7, **kwargs) -> GPSSimulator:
    return GPSSimulator(seed=seed, start_time=START, **kwargs)


def test_each_driver_pings_once_per_tick():
    pings = make_simulator(num_drivers=50).generate_pings(20)
    assert len(pings) == 50 * 20
    counts = Counter(ping.driver_id for ping in pings)
    assert set(counts.values()) == {20}


def test_pings_are_sorted_by_time():
    pings = make_simulator(num_drivers=50).generate_pings(20)
    times = [ping.event_ts for ping in pings]
    assert times == sorted(times)


def test_positions_stay_inside_pune_box():
    for ping in make_simulator(num_drivers=100).generate_pings(200):
        assert LAT_RANGE[0] <= ping.lat <= LAT_RANGE[1]
        assert LNG_RANGE[0] <= ping.lng <= LNG_RANGE[1]


def test_speed_and_status_are_valid():
    for ping in make_simulator(num_drivers=100).generate_pings(50):
        assert 0 <= ping.speed_kmph <= 40
        assert ping.status in (DriverStatus.IDLE, DriverStatus.BUSY)


def test_a_driver_pings_about_every_interval():
    pings = make_simulator(num_drivers=5, ping_interval_seconds=5).generate_pings(30)
    by_driver = defaultdict(list)
    for ping in pings:
        by_driver[ping.driver_id].append(ping)
    for driver_pings in by_driver.values():
        for before, after in zip(driver_pings, driver_pings[1:], strict=False):
            gap = (after.event_ts - before.event_ts).total_seconds()
            assert 4.0 <= gap <= 6.0


def test_drivers_move_a_plausible_distance_between_pings():
    pings = make_simulator(num_drivers=5).generate_pings(100)
    by_driver = defaultdict(list)
    for ping in pings:
        by_driver[ping.driver_id].append(ping)
    for driver_pings in by_driver.values():
        for before, after in zip(driver_pings, driver_pings[1:], strict=False):
            assert haversine_km(before.lat, before.lng, after.lat, after.lng) < 0.1


def test_initial_busy_share_is_close_to_the_setting():
    pings = make_simulator(num_drivers=1000, busy_share=0.6).generate_pings(1)
    busy = sum(1 for ping in pings if ping.status == DriverStatus.BUSY)
    assert 0.5 <= busy / len(pings) <= 0.7


def test_same_seed_gives_same_output():
    first = [p.to_dict() for p in make_simulator(seed=5, num_drivers=20).generate_pings(10)]
    second = [p.to_dict() for p in make_simulator(seed=5, num_drivers=20).generate_pings(10)]
    other = [p.to_dict() for p in make_simulator(seed=6, num_drivers=20).generate_pings(10)]
    assert first == second
    assert first != other


def test_to_dict_is_json_friendly():
    data = make_simulator(num_drivers=1).generate_pings(1)[0].to_dict()
    assert data["status"] in ("idle", "busy")
    assert isinstance(data["event_ts"], str)


def test_invalid_arguments_raise_value_error():
    with pytest.raises(ValueError):
        GPSSimulator(num_drivers=0)
    with pytest.raises(ValueError):
        GPSSimulator(ping_interval_seconds=0)
    with pytest.raises(ValueError):
        GPSSimulator(busy_share=1.5)
    with pytest.raises(ValueError):
        make_simulator().generate_pings(-1)
