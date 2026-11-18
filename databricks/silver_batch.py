# Databricks notebook source
# MAGIC %md
# MAGIC # UrbanPulse Silver batch (Day 15)
# MAGIC
# MAGIC Bounded batch run. Reads the Bronze Delta table (trips, gps, payments) from ADLS only,
# MAGIC then: decode Avro -> validate -> dedupe (ADR-002 keys) -> 300 s watermark -> hash phone
# MAGIC -> write Delta tables to the silver container (overwrite, so a re-run gives the same result).
# MAGIC
# MAGIC Needs: running cluster, external location `urbanpulse-silver`, secret scope `urbanpulse`
# MAGIC with key `pii-salt`. Reconciliation: Bronze = silver + late + quarantine + duplicates.
# MAGIC
# MAGIC Note: Bronze holds two simulator runs that reuse IDs, so duplicate and late counts are
# MAGIC not the chaos rates from ADR-002.

# COMMAND ----------

# MAGIC %pip install fastavro

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Setup: schemas, decoder, validation rules, Bronze read
import io
import json
from datetime import datetime

import fastavro
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

spark.conf.set("spark.sql.session.timeZone", "UTC")


def parse(d):
    return fastavro.parse_schema(d)


TRIP_FIELDS = [
    {"name": "trip_id", "type": "string"},
    {
        "name": "event_type",
        "type": {
            "type": "enum",
            "name": "TripEventType",
            "symbols": ["requested", "accepted", "started", "completed", "cancelled"],
        },
    },
    {"name": "rider_id", "type": "string"},
    {"name": "driver_id", "type": ["null", "string"], "default": None},
    {"name": "pickup_lat", "type": "double"},
    {"name": "pickup_lng", "type": "double"},
    {"name": "drop_lat", "type": "double"},
    {"name": "drop_lng", "type": "double"},
    {"name": "fare", "type": ["null", "double"], "default": None},
    {"name": "event_ts", "type": "string"},
]

SCHEMAS = {
    "trips_v1": parse(
        {
            "type": "record",
            "name": "TripEvent",
            "namespace": "com.urbanpulse.trips",
            "fields": TRIP_FIELDS,
        }
    ),
    "trips_v2": parse(
        {
            "type": "record",
            "name": "TripEvent",
            "namespace": "com.urbanpulse.trips",
            "fields": TRIP_FIELDS
            + [{"name": "surge_multiplier", "type": ["double", "null"], "default": 1.0}],
        }
    ),
    "gps": parse(
        {
            "type": "record",
            "name": "GPSEvent",
            "namespace": "com.urbanpulse.gps",
            "fields": [
                {"name": "driver_id", "type": "string"},
                {"name": "lat", "type": "double"},
                {"name": "lng", "type": "double"},
                {"name": "speed_kmph", "type": "double"},
                {
                    "name": "status",
                    "type": {"type": "enum", "name": "DriverStatus", "symbols": ["idle", "busy"]},
                },
                {"name": "event_ts", "type": "string"},
            ],
        }
    ),
    "payments": parse(
        {
            "type": "record",
            "name": "PaymentEvent",
            "namespace": "com.urbanpulse.payments",
            "fields": [
                {"name": "payment_id", "type": "string"},
                {"name": "trip_id", "type": "string"},
                {"name": "rider_id", "type": "string"},
                {
                    "name": "method",
                    "type": {
                        "type": "enum",
                        "name": "PaymentMethod",
                        "symbols": ["UPI", "CARD", "CASH", "WALLET"],
                    },
                },
                {"name": "amount", "type": "double"},
                {
                    "name": "status",
                    "type": {
                        "type": "enum",
                        "name": "PaymentStatus",
                        "symbols": ["SUCCESS", "FAILED"],
                    },
                },
                {"name": "phone", "type": "string"},
                {"name": "event_ts", "type": "string"},
            ],
        }
    ),
}


def try_decode(payload, schema):
    buf = io.BytesIO(bytes(payload))
    try:
        rec = fastavro.schemaless_reader(buf, schema)
        return rec if buf.tell() == len(payload) else None
    except Exception:
        return None


RULES = {
    "trips": {
        "lat": ("pickup_lat", "drop_lat"),
        "lng": ("pickup_lng", "drop_lng"),
        "nonneg": ("fare",),
    },
    "gps": {"lat": ("lat",), "lng": ("lng",), "nonneg": ("speed_kmph",)},
    "payments": {"lat": (), "lng": (), "nonneg": ("amount",)},
}


def check(topic, payload):
    p = bytes(payload)
    keys = ["trips_v1", "trips_v2"] if topic == "trips" else [topic]
    rec = None
    for k in keys:
        rec = try_decode(p, SCHEMAS[k])
        if rec is not None:
            break
    if rec is None:
        return (None, "DECODE_ERROR")
    r = RULES[topic]
    for n in r["lat"]:
        if not -90 <= rec[n] <= 90:
            return (None, "RANGE_LAT")
    for n in r["lng"]:
        if not -180 <= rec[n] <= 180:
            return (None, "RANGE_LNG")
    for n in r["nonneg"]:
        if rec[n] is not None and rec[n] < 0:
            return (None, "NEGATIVE_VALUE")
    try:
        datetime.fromisoformat(rec["event_ts"])
    except ValueError:
        return (None, "BAD_TIMESTAMP")
    return (json.dumps(rec), None)


check_udf = F.udf(
    check, StructType([StructField("rec_json", StringType()), StructField("reason", StringType())])
)

df = spark.read.format("delta").load(
    "abfss://bronze@urbanpulsedevsn095.dfs.core.windows.net/events"
)
src = df.filter(F.col("topic").isin("trips", "gps", "payments"))
print("setup OK, rows:", src.count())

# COMMAND ----------

# Silver: write test, PII hash, decode + validate, dedupe, watermark, write, reconcile
import hashlib
import hmac
from functools import reduce

from pyspark.sql import Window

BASE = "abfss://silver@urbanpulsedevsn095.dfs.core.windows.net"

# 0) write-access test: raises and stops here if it fails
dbutils.fs.put(f"{BASE}/_test/ok.txt", "ok", True)
dbutils.fs.rm(f"{BASE}/_test", True)
print("write access OK")

# 1) PII hash, same method as ingestion/pii.py (HMAC-SHA256 hex, value trimmed)
SALT = dbutils.secrets.get("urbanpulse", "pii-salt").encode("utf-8")


def _hash(v):
    if v is None:
        return None
    v = v.strip()
    return hmac.new(SALT, v.encode("utf-8"), hashlib.sha256).hexdigest() if v else None


hash_udf = F.udf(_hash, StringType())

# 2) decode and validate
res = (
    src.withColumn("r", check_udf("topic", "payload")).select(
        "topic",
        "partition",
        "offset",
        "ingest_time",
        "source_timestamp",
        F.col("r.rec_json").alias("rec_json"),
        F.col("r.reason").alias("reason"),
        F.base64("payload").alias("payload_b64"),
    )
).cache()
valid = res.filter(F.col("reason").isNull())

DDL = {
    "trips": "trip_id string, event_type string, rider_id string, driver_id string, pickup_lat double, pickup_lng double, drop_lat double, drop_lng double, fare double, event_ts string",
    "gps": "driver_id string, lat double, lng double, speed_kmph double, status string, event_ts string",
    "payments": "payment_id string, trip_id string, rider_id string, method string, amount double, status string, phone string, event_ts string",
}
KEYS = {
    "trips": ["trip_id", "event_type"],
    "payments": ["payment_id"],
    "gps": ["driver_id", "event_ts"],
}
ORDER = [F.col("ingest_time"), F.col("source_timestamp"), F.col("_partition"), F.col("_offset")]

# 3) dedupe, then 300 s watermark (ADR-002), then write
late_parts, dups = [], {}
for topic in ["trips", "gps", "payments"]:
    t = (
        valid.filter(F.col("topic") == topic)
        .withColumn("j", F.from_json("rec_json", DDL[topic]))
        .select(
            F.col("partition").alias("_partition"),
            F.col("offset").alias("_offset"),
            "ingest_time",
            "source_timestamp",
            "j.*",
        )
        .withColumn("event_ts", F.to_timestamp("event_ts"))
    )
    if topic == "payments":
        t = t.withColumn("phone_hash", hash_udf("phone")).drop("phone")

    t = t.withColumn("rn", F.row_number().over(Window.partitionBy(*KEYS[topic]).orderBy(*ORDER)))
    dups[topic] = t.filter("rn > 1").count()
    first = t.filter("rn = 1").drop("rn")

    w = Window.orderBy(*ORDER).rowsBetween(Window.unboundedPreceding, -1)
    first = first.withColumn("_prev_max", F.max("event_ts").over(w))
    first = first.withColumn(
        "_is_late",
        F.col("_prev_max").isNotNull()
        & (F.col("event_ts") < F.col("_prev_max") - F.expr("INTERVAL 300 SECONDS")),
    )

    keep = [
        c
        for c in first.columns
        if c not in ("ingest_time", "source_timestamp", "_prev_max", "_is_late")
    ]
    data_cols = [c for c in keep if c not in ("_partition", "_offset")]

    (
        first.filter(~F.col("_is_late"))
        .select(*keep)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{BASE}/{topic}")
    )

    late_parts.append(
        first.filter("_is_late").select(
            F.lit(topic).alias("topic"),
            "_partition",
            "_offset",
            "event_ts",
            F.to_json(F.struct(*data_cols)).alias("record_json"),
            (F.col("_prev_max").cast("double") - F.col("event_ts").cast("double")).alias(
                "_late_by_seconds"
            ),
            (F.col("_prev_max") - F.expr("INTERVAL 300 SECONDS")).alias("_watermark"),
        )
    )

(
    reduce(lambda a, b: a.unionByName(b), late_parts)
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{BASE}/late_arrivals")
)

(
    res.filter(F.col("reason").isNotNull())
    .select(
        "topic",
        F.col("partition").alias("_partition"),
        F.col("offset").alias("_offset"),
        "ingest_time",
        F.col("reason").alias("_error_reason"),
        F.when(F.col("topic") != "payments", F.col("payload_b64")).alias("payload_b64"),
    )
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{BASE}/quarantine")
)

# 4) reconcile from what was actually written
late_b = spark.read.format("delta").load(f"{BASE}/late_arrivals")
quar_b = spark.read.format("delta").load(f"{BASE}/quarantine")
print(f"{'topic':9} {'bronze':>6} {'silver':>6} {'late':>5} {'quar':>5} {'dups':>5}  conserved")
for topic in ["trips", "gps", "payments"]:
    b = src.filter(F.col("topic") == topic).count()
    s = spark.read.format("delta").load(f"{BASE}/{topic}").count()
    l = late_b.filter(F.col("topic") == topic).count()
    q = quar_b.filter(F.col("topic") == topic).count()
    d = dups[topic]
    print(f"{topic:9} {b:>6} {s:>6} {l:>5} {q:>5} {d:>5}  {b == s + l + q + d}")

print("payments columns:", spark.read.format("delta").load(f"{BASE}/payments").columns)
