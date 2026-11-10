"""Seed the CDC source tables (drivers, riders, promo_codes) with synthetic data."""

from __future__ import annotations

import os
import random
import sys
from datetime import date, timedelta

import psycopg

NUM_DRIVERS = 500
NUM_RIDERS = 1000
NUM_PROMOS = 20

CITIES = ["Pune", "Mumbai", "Nashik", "Nagpur", "Aurangabad"]
VEHICLES = ["auto", "mini", "sedan", "suv"]
FIRST = [
    "Aarav",
    "Vivaan",
    "Aditya",
    "Rohan",
    "Kabir",
    "Ishaan",
    "Anaya",
    "Diya",
    "Meera",
    "Saanvi",
]
LAST = [
    "Patil",
    "Deshmukh",
    "Jadhav",
    "Shinde",
    "Kulkarni",
    "Pawar",
    "More",
    "Gaikwad",
    "Joshi",
    "Nikam",
]


def _name(rng: random.Random) -> str:
    return f"{rng.choice(FIRST)} {rng.choice(LAST)}"


def _phone(rng: random.Random) -> str:
    return f"+91{rng.randint(6_000_000_000, 9_999_999_999)}"


def main() -> None:
    rng = random.Random(42)
    today = date.today()

    drivers = [
        (
            f"DRV-{i:04d}",
            _name(rng),
            _phone(rng),
            rng.choice(CITIES),
            rng.choice(VEHICLES),
            round(rng.uniform(3.5, 5.0), 2),
        )
        for i in range(1, NUM_DRIVERS + 1)
    ]
    riders = [
        (
            f"RDR-{i:05d}",
            _name(rng),
            _phone(rng),
            rng.choice(CITIES),
            today - timedelta(days=rng.randint(30, 900)),
        )
        for i in range(1, NUM_RIDERS + 1)
    ]
    promos = [
        (
            f"PROMO{i:02d}",
            rng.choice([5, 10, 15, 20]),
            True,
            today + timedelta(days=rng.randint(30, 180)),
        )
        for i in range(1, NUM_PROMOS + 1)
    ]

    with psycopg.connect(
        host="localhost",
        port=5432,
        dbname="urbanpulse",
        user="upadmin",
        password=os.environ["PG_PASSWORD"],
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM drivers")
            if cur.fetchone()[0] > 0:
                sys.exit("drivers is not empty - refusing to seed twice.")
            cur.executemany(
                "INSERT INTO drivers (driver_id, full_name, phone, city, vehicle_type, rating)"
                " VALUES (%s, %s, %s, %s, %s, %s)",
                drivers,
            )
            cur.executemany(
                "INSERT INTO riders (rider_id, full_name, phone, city, signup_date)"
                " VALUES (%s, %s, %s, %s, %s)",
                riders,
            )
            cur.executemany(
                "INSERT INTO promo_codes (code, discount_pct, active, valid_until)"
                " VALUES (%s, %s, %s, %s)",
                promos,
            )

    print(f"Seeded {len(drivers)} drivers, {len(riders)} riders, {len(promos)} promo codes.")


if __name__ == "__main__":
    main()
