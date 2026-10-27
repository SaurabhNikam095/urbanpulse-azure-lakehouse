"""Event model for the UrbanPulse trip simulator (trips.v1)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class EventType(StrEnum):
    """Lifecycle stages of a trip."""

    REQUESTED = "requested"
    ACCEPTED = "accepted"
    STARTED = "started"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class TripEvent:
    """One event in the lifecycle of a trip."""

    trip_id: str
    event_type: EventType
    rider_id: str
    driver_id: str | None
    pickup_lat: float
    pickup_lng: float
    drop_lat: float
    drop_lng: float
    fare: float | None
    event_ts: datetime

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-friendly dictionary with the timestamp as ISO 8601 text."""
        return {
            "trip_id": self.trip_id,
            "event_type": self.event_type.value,
            "rider_id": self.rider_id,
            "driver_id": self.driver_id,
            "pickup_lat": self.pickup_lat,
            "pickup_lng": self.pickup_lng,
            "drop_lat": self.drop_lat,
            "drop_lng": self.drop_lng,
            "fare": self.fare,
            "event_ts": self.event_ts.isoformat(),
        }
