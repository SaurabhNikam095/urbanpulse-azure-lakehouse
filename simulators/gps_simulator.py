"""GPS ping simulator for UrbanPulse drivers (gps.v1).

Every driver sends one ping per interval. Drivers drift as a random walk inside
the Pune bounding box and switch between idle and busy now and then.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from simulators.models import DriverStatus, GPSEvent
from simulators.trip_simulator import LAT_RANGE, LNG_RANGE

KM_PER_DEGREE_LAT = 111.0
IDLE_SPEED_KMPH = (0.0, 8.0)
BUSY_SPEED_KMPH = (15.0, 40.0)
STATUS_FLIP_PROBABILITY = 0.02


@dataclass
class _DriverState:
    driver_id: str
    lat: float
    lng: float
    heading_deg: float
    status: DriverStatus


class GPSSimulator:
    """Generates GPS pings for a pool of drivers."""

    def __init__(
        self,
        seed: int | None = None,
        num_drivers: int = 500,
        ping_interval_seconds: float = 5.0,
        busy_share: float = 0.6,
        start_time: datetime | None = None,
    ) -> None:
        """Create a simulator.

        busy_share is the share of drivers that start as busy. Pass a seed and a
        timezone-aware start_time for fully repeatable output.
        """
        if num_drivers < 1:
            raise ValueError("num_drivers must be at least 1")
        if ping_interval_seconds <= 0:
            raise ValueError("ping_interval_seconds must be positive")
        if not 0.0 <= busy_share <= 1.0:
            raise ValueError("busy_share must be between 0 and 1")

        self._rng = random.Random(seed)
        self._interval = ping_interval_seconds
        self._clock = start_time or datetime.now(UTC)
        self._drivers = [self._new_driver(i, busy_share) for i in range(1, num_drivers + 1)]

    def _new_driver(self, number: int, busy_share: float) -> _DriverState:
        status = DriverStatus.BUSY if self._rng.random() < busy_share else DriverStatus.IDLE
        return _DriverState(
            driver_id=f"DRV-{number:04d}",
            lat=round(self._rng.uniform(*LAT_RANGE), 6),
            lng=round(self._rng.uniform(*LNG_RANGE), 6),
            heading_deg=self._rng.uniform(0, 360),
            status=status,
        )

    def _step(self, driver: _DriverState) -> float:
        """Move one driver for one interval and return the speed in km/h."""
        if self._rng.random() < STATUS_FLIP_PROBABILITY:
            driver.status = (
                DriverStatus.IDLE if driver.status == DriverStatus.BUSY else DriverStatus.BUSY
            )
        low, high = BUSY_SPEED_KMPH if driver.status == DriverStatus.BUSY else IDLE_SPEED_KMPH
        speed = self._rng.uniform(low, high)

        driver.heading_deg = (driver.heading_deg + self._rng.uniform(-20, 20)) % 360
        distance_km = speed * self._interval / 3600
        heading = math.radians(driver.heading_deg)
        driver.lat += distance_km * math.cos(heading) / KM_PER_DEGREE_LAT
        lng_scale = KM_PER_DEGREE_LAT * math.cos(math.radians(driver.lat))
        driver.lng += distance_km * math.sin(heading) / lng_scale

        lat_ok = LAT_RANGE[0] <= driver.lat <= LAT_RANGE[1]
        lng_ok = LNG_RANGE[0] <= driver.lng <= LNG_RANGE[1]
        if not (lat_ok and lng_ok):
            driver.heading_deg = (driver.heading_deg + 180) % 360
            driver.lat = min(max(driver.lat, LAT_RANGE[0]), LAT_RANGE[1])
            driver.lng = min(max(driver.lng, LNG_RANGE[0]), LNG_RANGE[1])
        return speed

    def generate_pings(self, ticks: int) -> list[GPSEvent]:
        """Generate one ping per driver for each tick, sorted by event time."""
        if ticks < 0:
            raise ValueError("ticks must not be negative")
        events: list[GPSEvent] = []
        for _ in range(ticks):
            self._clock += timedelta(seconds=self._interval)
            for driver in self._drivers:
                speed = self._step(driver)
                jitter = timedelta(milliseconds=self._rng.randint(0, 999))
                events.append(
                    GPSEvent(
                        driver_id=driver.driver_id,
                        lat=round(driver.lat, 6),
                        lng=round(driver.lng, 6),
                        speed_kmph=round(speed, 1),
                        status=driver.status,
                        event_ts=self._clock + jitter,
                    )
                )
        return sorted(events, key=lambda event: event.event_ts)
