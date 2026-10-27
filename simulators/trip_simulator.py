"""Trip event simulator for UrbanPulse (ride-hailing in Pune).

Generates the lifecycle of trips as TripEvent objects:
requested -> accepted -> started -> completed, with some trips cancelled.
Pass a seed and a start_time to get identical output on every run.
"""

from __future__ import annotations

import json
import math
import random
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from simulators.models import EventType, TripEvent

# Bounding box that roughly covers Pune city.
LAT_RANGE = (18.43, 18.62)
LNG_RANGE = (73.75, 73.95)

# Simple fare model in INR.
BASE_FARE = 40.0
PER_KM = 12.0
PER_MINUTE = 1.5
MIN_FARE = 60.0

MIN_TRIP_KM = 0.5
EARTH_RADIUS_KM = 6371.0


def haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Return the great-circle distance between two points in kilometres."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = phi2 - phi1
    d_lambda = math.radians(lng2 - lng1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def calculate_fare(distance_km: float, duration_min: float) -> float:
    """Return the fare in INR: base + per-km + per-minute, with a minimum fare."""
    fare = BASE_FARE + PER_KM * distance_km + PER_MINUTE * duration_min
    return round(max(fare, MIN_FARE), 2)


class TripSimulator:
    """Generates trip lifecycle events for a pool of drivers and riders."""

    def __init__(
        self,
        seed: int | None = None,
        num_drivers: int = 500,
        num_riders: int = 5000,
        cancel_rate: float = 0.12,
        start_time: datetime | None = None,
    ) -> None:
        """Create a simulator.

        seed: makes the output repeatable. start_time (timezone-aware) is also
        needed for fully identical timestamps; it defaults to the current UTC time.
        """
        if num_drivers < 1 or num_riders < 1:
            raise ValueError("num_drivers and num_riders must be at least 1")
        if not 0.0 <= cancel_rate <= 1.0:
            raise ValueError("cancel_rate must be between 0 and 1")

        self._rng = random.Random(seed)
        self._drivers = [f"DRV-{i:04d}" for i in range(1, num_drivers + 1)]
        self._riders = [f"RDR-{i:05d}" for i in range(1, num_riders + 1)]
        self._cancel_rate = cancel_rate
        self._clock = start_time or datetime.now(UTC)
        self._trip_counter = 0

    def _random_point(self) -> tuple[float, float]:
        lat = round(self._rng.uniform(*LAT_RANGE), 6)
        lng = round(self._rng.uniform(*LNG_RANGE), 6)
        return lat, lng

    def _random_route(self) -> tuple[tuple[float, float], tuple[float, float], float]:
        """Pick a pickup and a drop point at least MIN_TRIP_KM apart."""
        while True:
            pickup = self._random_point()
            drop = self._random_point()
            distance = haversine_km(*pickup, *drop)
            if distance >= MIN_TRIP_KM:
                return pickup, drop, distance

    def _after(self, moment: datetime, low: float, high: float) -> datetime:
        """Return a time between low and high seconds after the given moment."""
        return moment + timedelta(seconds=self._rng.uniform(low, high))

    def generate_trip(self) -> list[TripEvent]:
        """Generate all events of one trip, in time order."""
        self._trip_counter += 1
        self._clock = self._after(self._clock, 0.2, 2.0)
        requested_at = self._clock

        pickup, drop, distance = self._random_route()
        requested = TripEvent(
            trip_id=f"TRP-{self._trip_counter:08d}",
            event_type=EventType.REQUESTED,
            rider_id=self._rng.choice(self._riders),
            driver_id=None,
            pickup_lat=pickup[0],
            pickup_lng=pickup[1],
            drop_lat=drop[0],
            drop_lng=drop[1],
            fare=None,
            event_ts=requested_at,
        )
        events = [requested]

        cancelled = self._rng.random() < self._cancel_rate
        cancel_after_accept = cancelled and self._rng.random() < 0.5

        if cancelled and not cancel_after_accept:
            cancel_at = self._after(requested_at, 10, 120)
            events.append(replace(requested, event_type=EventType.CANCELLED, event_ts=cancel_at))
            return events

        driver_id = self._rng.choice(self._drivers)
        accepted_at = self._after(requested_at, 5, 60)
        accepted = replace(
            requested,
            event_type=EventType.ACCEPTED,
            driver_id=driver_id,
            event_ts=accepted_at,
        )
        events.append(accepted)

        if cancel_after_accept:
            cancel_at = self._after(accepted_at, 10, 180)
            events.append(replace(accepted, event_type=EventType.CANCELLED, event_ts=cancel_at))
            return events

        started_at = self._after(accepted_at, 60, 300)
        events.append(replace(accepted, event_type=EventType.STARTED, event_ts=started_at))

        speed_kmph = self._rng.uniform(15, 30)
        duration_min = distance / speed_kmph * 60
        completed_at = started_at + timedelta(minutes=duration_min)
        events.append(
            replace(
                accepted,
                event_type=EventType.COMPLETED,
                fare=calculate_fare(distance, duration_min),
                event_ts=completed_at,
            )
        )
        return events

    def generate_trips(self, count: int) -> list[list[TripEvent]]:
        """Generate count trips, each as its own list of events."""
        if count < 0:
            raise ValueError("count must not be negative")
        return [self.generate_trip() for _ in range(count)]

    def generate_events(self, count: int) -> list[TripEvent]:
        """Generate count trips and return all events sorted by event time."""
        events = [event for trip in self.generate_trips(count) for event in trip]
        return sorted(events, key=lambda event: event.event_ts)


def main() -> None:
    """Print the events of three sample trips as JSON lines."""
    simulator = TripSimulator(seed=42)
    for event in simulator.generate_events(3):
        print(json.dumps(event.to_dict()))


if __name__ == "__main__":
    main()
