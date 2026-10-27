"""Chaos injection: duplicates, late events and bad records.

Works on plain dictionaries (the output of to_dict) so one injector serves the
trips, gps and payments streams. Events are never marked; instead a manifest
records what was injected, so pipeline results can be checked against it.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta

BAD_KINDS: dict[str, tuple[str, ...]] = {
    "trips": ("null_coordinates", "negative_fare", "unknown_event_type", "invalid_latitude"),
    "gps": ("null_coordinates", "invalid_latitude", "unknown_status", "negative_speed"),
    "payments": ("negative_amount", "unknown_method", "missing_trip_id"),
}


@dataclass(frozen=True)
class ChaosConfig:
    """How much chaos to inject. Rates are the share of input events affected.

    A record is bad or late (never both). Duplicates are extra copies of records
    that are not bad.
    """

    duplicate_rate: float = 0.02
    late_rate: float = 0.03
    bad_rate: float = 0.01
    min_delay_seconds: float = 60.0
    max_delay_seconds: float = 600.0

    def __post_init__(self) -> None:
        for name in ("duplicate_rate", "late_rate", "bad_rate"):
            if not 0.0 <= getattr(self, name) <= 1.0:
                raise ValueError(f"{name} must be between 0 and 1")
        if self.late_rate + self.bad_rate > 1.0:
            raise ValueError("late_rate + bad_rate must not exceed 1")
        if not 0 < self.min_delay_seconds <= self.max_delay_seconds:
            raise ValueError("delays must satisfy 0 < min_delay_seconds <= max_delay_seconds")


@dataclass
class ChaosManifest:
    """What the injector did to one stream."""

    stream: str
    input_count: int
    duplicates: int = 0
    late: int = 0
    bad: int = 0
    bad_by_kind: dict[str, int] = field(default_factory=dict)

    @property
    def output_count(self) -> int:
        """Number of records after injection."""
        return self.input_count + self.duplicates


class ChaosInjector:
    """Injects duplicates, late events and bad records into a stream."""

    def __init__(self, config: ChaosConfig | None = None, seed: int | None = None) -> None:
        self._config = config or ChaosConfig()
        self._rng = random.Random(seed)

    def inject(
        self, events: list[dict[str, object]], stream: str
    ) -> tuple[list[dict[str, object]], ChaosManifest]:
        """Return the changed events (in arrival order) and the manifest.

        The input list and its dictionaries are not modified.
        """
        if stream not in BAD_KINDS:
            raise ValueError(f"unknown stream {stream!r}; use one of {sorted(BAD_KINDS)}")

        config = self._config
        manifest = ChaosManifest(stream=stream, input_count=len(events))
        output: list[dict[str, object]] = []

        for original in events:
            record = dict(original)
            roll = self._rng.random()
            is_bad = roll < config.bad_rate
            if is_bad:
                kind = self._rng.choice(BAD_KINDS[stream])
                self._corrupt(record, stream, kind)
                manifest.bad += 1
                manifest.bad_by_kind[kind] = manifest.bad_by_kind.get(kind, 0) + 1
            elif roll < config.bad_rate + config.late_rate:
                self._make_late(record)
                manifest.late += 1

            output.append(record)
            if not is_bad and self._rng.random() < config.duplicate_rate:
                output.append(dict(record))
                manifest.duplicates += 1

        return output, manifest

    def _make_late(self, record: dict[str, object]) -> None:
        """Move event_ts into the past while the record keeps its arrival position."""
        delay = self._rng.uniform(self._config.min_delay_seconds, self._config.max_delay_seconds)
        original = datetime.fromisoformat(str(record["event_ts"]))
        record["event_ts"] = (original - timedelta(seconds=delay)).isoformat()

    def _corrupt(self, record: dict[str, object], stream: str, kind: str) -> None:
        """Damage a record in place according to the chosen kind."""
        if kind == "null_coordinates":
            lat_key, lng_key = ("pickup_lat", "pickup_lng") if stream == "trips" else ("lat", "lng")
            record[lat_key] = None
            record[lng_key] = None
        elif kind == "invalid_latitude":
            lat_key = "pickup_lat" if stream == "trips" else "lat"
            record[lat_key] = round(self._rng.uniform(91, 200), 6)
        elif kind == "negative_fare":
            record["fare"] = -round(self._rng.uniform(10, 500), 2)
        elif kind == "unknown_event_type":
            record["event_type"] = "teleported"
        elif kind == "unknown_status":
            record["status"] = "sleeping"
        elif kind == "negative_speed":
            record["speed_kmph"] = -round(self._rng.uniform(1, 50), 1)
        elif kind == "negative_amount":
            record["amount"] = -round(self._rng.uniform(10, 500), 2)
        elif kind == "unknown_method":
            record["method"] = "BITCOIN"
        elif kind == "missing_trip_id":
            record["trip_id"] = None
        else:
            raise ValueError(f"unknown bad-record kind {kind!r}")
