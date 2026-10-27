"""Event models for the UrbanPulse simulators (trips.v1, gps.v1, payments.v1)."""

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


class DriverStatus(StrEnum):
    """Whether a driver is free or on a trip."""

    IDLE = "idle"
    BUSY = "busy"


class PaymentMethod(StrEnum):
    """How a rider pays."""

    UPI = "UPI"
    CARD = "CARD"
    CASH = "CASH"
    WALLET = "WALLET"


class PaymentStatus(StrEnum):
    """Outcome of a payment attempt."""

    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


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


@dataclass(frozen=True)
class GPSEvent:
    """One GPS ping from a driver."""

    driver_id: str
    lat: float
    lng: float
    speed_kmph: float
    status: DriverStatus
    event_ts: datetime

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-friendly dictionary with the timestamp as ISO 8601 text."""
        return {
            "driver_id": self.driver_id,
            "lat": self.lat,
            "lng": self.lng,
            "speed_kmph": self.speed_kmph,
            "status": self.status.value,
            "event_ts": self.event_ts.isoformat(),
        }


@dataclass(frozen=True)
class PaymentEvent:
    """One payment attempt for a completed trip. The phone number is synthetic PII."""

    payment_id: str
    trip_id: str
    rider_id: str
    method: PaymentMethod
    amount: float
    status: PaymentStatus
    phone: str
    event_ts: datetime

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-friendly dictionary with the timestamp as ISO 8601 text."""
        return {
            "payment_id": self.payment_id,
            "trip_id": self.trip_id,
            "rider_id": self.rider_id,
            "method": self.method.value,
            "amount": self.amount,
            "status": self.status.value,
            "phone": self.phone,
            "event_ts": self.event_ts.isoformat(),
        }
