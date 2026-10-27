"""Unit tests for the payment simulator."""

import re
from collections import Counter
from datetime import UTC, datetime

import pytest

from simulators.models import EventType, PaymentMethod, PaymentStatus
from simulators.payment_simulator import PaymentSimulator
from simulators.trip_simulator import TripSimulator

START = datetime(2026, 11, 1, 8, 0, tzinfo=UTC)
PHONE_PATTERN = re.compile(r"^\+91[6-9]\d{9}$")


def make_trips(count: int = 3000):
    return TripSimulator(seed=7, start_time=START).generate_events(count)


def test_one_payment_per_completed_trip():
    trips = make_trips()
    completed = [t for t in trips if t.event_type == EventType.COMPLETED]
    payments = PaymentSimulator(seed=1).generate_payments(trips)
    assert len(payments) == len(completed)
    assert {p.trip_id for p in payments} == {t.trip_id for t in completed}


def test_payment_matches_its_trip():
    trips = make_trips()
    completed = {t.trip_id: t for t in trips if t.event_type == EventType.COMPLETED}
    for payment in PaymentSimulator(seed=1).generate_payments(trips):
        trip = completed[payment.trip_id]
        assert payment.rider_id == trip.rider_id
        assert payment.amount == trip.fare


def test_payment_happens_shortly_after_the_trip_completes():
    trips = make_trips()
    completed = {t.trip_id: t for t in trips if t.event_type == EventType.COMPLETED}
    for payment in PaymentSimulator(seed=1).generate_payments(trips):
        delay = (payment.event_ts - completed[payment.trip_id].event_ts).total_seconds()
        assert 5 <= delay <= 90


def test_cash_never_fails_and_failure_rate_is_close_to_the_setting():
    payments = PaymentSimulator(seed=1, failure_rate=0.04).generate_payments(make_trips())
    cash = [p for p in payments if p.method == PaymentMethod.CASH]
    non_cash = [p for p in payments if p.method != PaymentMethod.CASH]
    assert cash
    assert all(p.status == PaymentStatus.SUCCESS for p in cash)
    failed = sum(1 for p in non_cash if p.status == PaymentStatus.FAILED)
    assert 0.025 <= failed / len(non_cash) <= 0.055


def test_method_mix_is_close_to_the_default_weights():
    payments = PaymentSimulator(seed=1).generate_payments(make_trips())
    counts = Counter(p.method for p in payments)
    upi_share = counts[PaymentMethod.UPI] / len(payments)
    assert 0.50 <= upi_share <= 0.60
    assert set(counts) == set(PaymentMethod)


def test_phone_numbers_are_valid_and_stable_per_rider():
    payments = PaymentSimulator(seed=1).generate_payments(make_trips())
    phones_by_rider = {}
    for payment in payments:
        assert PHONE_PATTERN.match(payment.phone)
        phones_by_rider.setdefault(payment.rider_id, set()).add(payment.phone)
    assert all(len(phones) == 1 for phones in phones_by_rider.values())


def test_payments_are_sorted_and_ids_are_unique():
    payments = PaymentSimulator(seed=1).generate_payments(make_trips())
    times = [p.event_ts for p in payments]
    assert times == sorted(times)
    assert len({p.payment_id for p in payments}) == len(payments)


def test_same_seed_gives_same_output():
    trips = make_trips(500)
    first = [p.to_dict() for p in PaymentSimulator(seed=3).generate_payments(trips)]
    second = [p.to_dict() for p in PaymentSimulator(seed=3).generate_payments(trips)]
    assert first == second


def test_invalid_arguments_raise_value_error():
    with pytest.raises(ValueError):
        PaymentSimulator(failure_rate=-0.1)
    with pytest.raises(ValueError):
        PaymentSimulator(method_weights={PaymentMethod.UPI: 0.0})
