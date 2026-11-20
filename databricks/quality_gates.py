# Databricks notebook source
# MAGIC %md
# MAGIC # UrbanPulse quality gates
# MAGIC Computes metrics from Bronze, Silver and Gold, checks them against the thresholds and
# MAGIC **raises** on any breach, so the run fails. The gate logic is the same block as
# MAGIC `quality/gates.py` (a test keeps the two copies identical).
# MAGIC
# MAGIC Needs: running cluster, external location `urbanpulse-silver`, Gold tables from dbt.

# COMMAND ----------

import json

from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

BRONZE = "abfss://bronze@urbanpulsedevsn095.dfs.core.windows.net/events"
BASE = "abfss://silver@urbanpulsedevsn095.dfs.core.windows.net"
GOLD = "dbw_urbanpulse_dev_sn095.urbanpulse_gold"

# COMMAND ----------

# --- gates:begin ---
THRESHOLDS = {
    "quarantine_rate_max": 0.02,  # quarantine rows / Bronze rows, per topic
    "key_null_rate_max": 0.0,  # null rate on key columns
    "fare_null_rate_max": 0.05,  # null fare on completed trips
    "duplicate_rate_max": 0.0,  # duplicate rows on the ADR-002 keys after Silver
}


def _rate(part, whole):
    return part / whole if whole else 0.0


def evaluate(metrics, thresholds=THRESHOLDS):
    """Return a list of breach messages. An empty list means every gate passed."""
    breaches = []

    for topic, m in metrics.get("quarantine", {}).items():
        rate = _rate(m["quarantine"], m["bronze"])
        limit = thresholds["quarantine_rate_max"]
        if rate > limit:
            breaches.append(f"quarantine rate {topic}: {rate:.2%} > {limit:.2%}")

    for name, m in metrics.get("key_nulls", {}).items():
        rate = _rate(m["nulls"], m["rows"])
        limit = thresholds["key_null_rate_max"]
        if rate > limit:
            breaches.append(f"null rate {name}: {rate:.2%} > {limit:.2%}")

    fare = metrics.get("fare_nulls")
    if fare:
        rate = _rate(fare["nulls"], fare["rows"])
        limit = thresholds["fare_null_rate_max"]
        if rate > limit:
            breaches.append(f"null fare on completed trips: {rate:.2%} > {limit:.2%}")

    for name, m in metrics.get("duplicates", {}).items():
        rate = _rate(m["rows"] - m["distinct_keys"], m["rows"])
        limit = thresholds["duplicate_rate_max"]
        if rate > limit:
            breaches.append(f"duplicate rate {name}: {rate:.2%} > {limit:.2%}")

    for topic, m in metrics.get("reconciliation", {}).items():
        kept = m["silver"] + m["late"] + m["quarantine"]
        if "dups" in m:
            gap = m["bronze"] - (kept + m["dups"])
            if gap != 0:
                breaches.append(f"reconciliation {topic}: {gap} rows unaccounted for")
        elif kept > m["bronze"]:
            breaches.append(f"reconciliation {topic}: output {kept} > Bronze {m['bronze']}")

    return breaches


def assert_gates(metrics, thresholds=THRESHOLDS):
    """Print every breach and raise, so the run fails. Print a pass line otherwise."""
    breaches = evaluate(metrics, thresholds)
    for breach in breaches:
        print("GATE BREACH:", breach)
    if breaches:
        raise RuntimeError(f"{len(breaches)} quality gate(s) breached")
    print("all quality gates passed")


# --- gates:end ---

# COMMAND ----------

# Metrics from what is actually stored
bronze = spark.read.format("delta").load(BRONZE)
quar = spark.read.format("delta").load(f"{BASE}/quarantine")
late = spark.read.format("delta").load(f"{BASE}/late_arrivals")


def silver(name):
    return spark.read.format("delta").load(f"{BASE}/{name}")


def null_stats(df, column):
    return {"rows": df.count(), "nulls": df.filter(F.col(column).isNull()).count()}


def dup_stats(df, keys):
    return {"rows": df.count(), "distinct_keys": df.select(*keys).distinct().count()}


metrics = {"quarantine": {}, "reconciliation": {}, "key_nulls": {}, "duplicates": {}}

for topic in ["trips", "gps", "payments"]:
    b = bronze.filter(F.col("topic") == topic).count()
    q = quar.filter(F.col("topic") == topic).count()
    metrics["quarantine"][topic] = {"bronze": b, "quarantine": q}
    metrics["reconciliation"][topic] = {
        "bronze": b,
        "silver": silver(topic).count(),
        "late": late.filter(F.col("topic") == topic).count(),
        "quarantine": q,
    }

# driver_id is empty before a driver accepts, so it is not a key gate
metrics["key_nulls"]["silver.trips.trip_id"] = null_stats(silver("trips"), "trip_id")
metrics["key_nulls"]["silver.payments.payment_id"] = null_stats(silver("payments"), "payment_id")
metrics["key_nulls"]["silver.payments.trip_id"] = null_stats(silver("payments"), "trip_id")

fact_trips = spark.table(f"{GOLD}.fact_trips")
fact_payments = spark.table(f"{GOLD}.fact_payments")
metrics["key_nulls"]["gold.fact_trips.trip_id"] = null_stats(fact_trips, "trip_id")
metrics["key_nulls"]["gold.fact_payments.payment_id"] = null_stats(fact_payments, "payment_id")

completed = fact_trips.filter(F.col("status") == "completed")
metrics["fare_nulls"] = null_stats(completed, "fare")

metrics["duplicates"]["silver.trips (trip_id, event_type)"] = dup_stats(
    silver("trips"), ["trip_id", "event_type"]
)
metrics["duplicates"]["silver.payments (payment_id)"] = dup_stats(
    silver("payments"), ["payment_id"]
)

print(json.dumps(metrics, indent=2))

# COMMAND ----------

assert_gates(metrics)
