CREATE TABLE drivers (
    driver_id    TEXT PRIMARY KEY,
    full_name    TEXT NOT NULL,
    phone        TEXT NOT NULL,
    city         TEXT NOT NULL,
    vehicle_type TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'idle',
    rating       NUMERIC(3,2) NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE riders (
    rider_id    TEXT PRIMARY KEY,
    full_name   TEXT NOT NULL,
    phone       TEXT NOT NULL,
    city        TEXT NOT NULL,
    signup_date DATE NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE promo_codes (
    code         TEXT PRIMARY KEY,
    discount_pct INTEGER NOT NULL,
    active       BOOLEAN NOT NULL DEFAULT TRUE,
    valid_until  DATE NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE drivers     REPLICA IDENTITY FULL;
ALTER TABLE riders      REPLICA IDENTITY FULL;
ALTER TABLE promo_codes REPLICA IDENTITY FULL;

CREATE PUBLICATION urbanpulse_cdc FOR TABLE drivers, riders, promo_codes;

SELECT pg_create_logical_replication_slot('urbanpulse_slot', 'pgoutput');
