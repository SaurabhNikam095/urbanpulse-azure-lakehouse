"""Payment simulator for UrbanPulse (payments.v1).

Creates one payment attempt for every completed trip. Phone numbers are
synthetic and act as the PII test data that Silver must hash.
"""

from __future__ import annotations

import random
from collections.abc import Iterable
from datetime import timedelta

from simulators.models import EventType, PaymentEvent, PaymentMethod, PaymentStatus, TripEvent

DEFAULT_METHOD_WEIGHTS = {
    PaymentMethod.UPI: 0.55,
    PaymentMethod.CASH: 0.20,
    PaymentMethod.CARD: 0.15,
    PaymentMethod.WALLET: 0.10,
}


class PaymentSimulator:
    """Turns completed trips into payment events."""

    def __init__(
        self,
        seed: int | None = None,
        failure_rate: float = 0.04,
        method_weights: dict[PaymentMethod, float] | None = None,
    ) -> None:
        """Create a simulator.

        failure_rate applies to non-cash payments only; cash always succeeds.
        """
        if not 0.0 <= failure_rate <= 1.0:
            raise ValueError("failure_rate must be between 0 and 1")
        weights = method_weights or DEFAULT_METHOD_WEIGHTS
        if not weights or any(w < 0 for w in weights.values()) or sum(weights.values()) <= 0:
            raise ValueError("method_weights must be non-negative and not all zero")

        self._rng = random.Random(seed)
        self._failure_rate = failure_rate
        self._methods = list(weights)
        self._weights = list(weights.values())
        self._phones: dict[str, str] = {}
        self._counter = 0

    def _phone_for(self, rider_id: str) -> str:
        """Return a stable synthetic Indian mobile number for a rider."""
        if rider_id not in self._phones:
            first = self._rng.choice("6789")
            rest = self._rng.randrange(10**9)
            self._phones[rider_id] = f"+91{first}{rest:09d}"
        return self._phones[rider_id]

    def generate_payments(self, trip_events: Iterable[TripEvent]) -> list[PaymentEvent]:
        """Create a payment for each completed trip event, sorted by event time."""
        payments: list[PaymentEvent] = []
        for trip in trip_events:
            if trip.event_type != EventType.COMPLETED:
                continue
            if trip.fare is None:
                raise ValueError(f"completed trip {trip.trip_id} has no fare")

            self._counter += 1
            method = self._rng.choices(self._methods, weights=self._weights)[0]
            failed = method != PaymentMethod.CASH and self._rng.random() < self._failure_rate
            paid_at = trip.event_ts + timedelta(seconds=self._rng.uniform(5, 90))
            payments.append(
                PaymentEvent(
                    payment_id=f"PAY-{self._counter:08d}",
                    trip_id=trip.trip_id,
                    rider_id=trip.rider_id,
                    method=method,
                    amount=trip.fare,
                    status=PaymentStatus.FAILED if failed else PaymentStatus.SUCCESS,
                    phone=self._phone_for(trip.rider_id),
                    event_ts=paid_at,
                )
            )
        return sorted(payments, key=lambda payment: payment.event_ts)
