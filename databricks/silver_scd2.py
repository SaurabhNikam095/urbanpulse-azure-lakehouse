# Databricks notebook source
# MAGIC %md
# MAGIC # UrbanPulse Silver SCD2: dim_driver and dim_rider
# MAGIC Bounded run. Reads the CDC events (topic `cdc`) from Bronze, builds SCD Type 2 versions
# MAGIC with the same rules as `ingestion/scd2.py`, and MERGEs them into Delta tables in the
# MAGIC silver container. Re-running gives the same result.
# MAGIC
# MAGIC Needs: running cluster, external location `urbanpulse-silver`, secret scope `urbanpulse`
# MAGIC (key `pii-salt`).

# COMMAND ----------

# Setup: paths, salt, hashing functions
import hashlib
import hmac
import json

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

spark.conf.set("spark.sql.session.timeZone", "UTC")

BRONZE = "abfss://bronze@urbanpulsedevsn095.dfs.core.windows.net/events"
BASE = "abfss://silver@urbanpulsedevsn095.dfs.core.windows.net"
SALT = dbutils.secrets.get("urbanpulse", "pii-salt").encode("utf-8")

# table -> (business key, plain tracked fields, Delta table name)
SPECS = {
    "drivers": ("driver_id", ["name", "vehicle_type", "rating", "status"], "dim_driver"),
    "riders": ("rider_id", ["name", "status"], "dim_rider"),
}


def _hash_value(value, lower=False):
    """HMAC-SHA256 hex of the trimmed value; None for null or empty."""
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    if lower:
        value = value.lower()
    return hmac.new(SALT, value.encode("utf-8"), hashlib.sha256).hexdigest()


def _row_hash(tbl, name, vehicle_type, rating, status, phone_hash, email_hash):
    """Same payload and method as ingestion/scd2.py compute_row_hash."""
    if tbl == "drivers":
        fields = {"name": name, "vehicle_type": vehicle_type, "rating": rating, "status": status}
    else:
        fields = {"name": name, "status": status}
    fields["phone_hash"] = phone_hash
    fields["email_hash"] = email_hash
    text = json.dumps(fields, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


hash_phone = F.udf(lambda v: _hash_value(v), StringType())
hash_email = F.udf(lambda v: _hash_value(v, True), StringType())
row_hash = F.udf(_row_hash, StringType())

# COMMAND ----------

# Decode the Debezium JSON envelope, dedupe replays, rebuild missing first versions
raw = spark.read.format("delta").load(BRONZE).filter("topic = 'cdc'")
s = F.col("payload").cast("string")


def j(col, path):
    return F.get_json_object(col, path)


dec = raw.select(
    j(s, "$.source.table").alias("tbl"),
    j(s, "$.op").alias("op"),
    j(s, "$.source.ts_ms").cast("long").alias("ts"),
    j(s, "$.source.lsn").cast("long").alias("lsn"),
    j(s, "$.after").alias("after_j"),
    j(s, "$.before").alias("before_j"),
    F.col("partition"),
    F.col("offset"),
)
print("cdc rows by table (promo_codes is not a dimension and is ignored):")
dec.groupBy("tbl").count().orderBy("tbl").show()

dec = dec.filter(F.col("tbl").isin(*SPECS.keys()))

# exact replays of the same change are applied once
dedup = dec.dropDuplicates(["tbl", "op", "ts", "lsn", "after_j", "before_j"])

rec = F.coalesce("after_j", "before_j")
key = F.when(F.col("tbl") == "drivers", j(rec, "$.driver_id")).otherwise(j(rec, "$.rider_id"))
ev = dedup.withColumn("key", key).withColumn("rebuilt", F.lit(False))

# a key whose first event is an update gets its first version from the before image
order = ["ts", "lsn", "partition", "offset"]
w_first = Window.partitionBy("tbl", "key").orderBy(*order)
first = ev.withColumn("rn", F.row_number().over(w_first)).filter(
    (F.col("rn") == 1) & (F.col("op") == "u") & F.col("before_j").isNotNull()
)
rebuilt = first.select(
    "tbl",
    "key",
    F.lit("c").alias("op"),
    (F.to_timestamp(j("before_j", "$.updated_at")).cast("double") * 1000).cast("long").alias("ts"),
    F.lit(0).cast("long").alias("lsn"),
    F.col("before_j").alias("after_j"),
    F.lit(None).cast("string").alias("before_j"),
    F.lit(-1).cast("int").alias("partition"),
    F.lit(-1).cast("long").alias("offset"),
    F.lit(True).alias("rebuilt"),
)
cols = ["tbl", "key", "op", "ts", "lsn", "after_j", "before_j", "partition", "offset", "rebuilt"]
events = ev.select(*cols).unionByName(rebuilt.select(*cols))

# COMMAND ----------

# States with hashed PII and row_hash; keep only real changes; compute validity windows
w = Window.partitionBy("tbl", "key").orderBy(*order)

st = (
    events.select(
        "tbl",
        "key",
        "op",
        "ts",
        "lsn",
        "partition",
        "offset",
        "rebuilt",
        (F.col("op") == "d").alias("is_deleted"),
        j("after_j", "$.full_name").alias("name"),
        j("after_j", "$.vehicle_type").alias("vehicle_type"),
        j("after_j", "$.rating").alias("rating"),
        j("after_j", "$.status").alias("status"),
        hash_phone(j("after_j", "$.phone")).alias("phone_hash"),
        hash_email(j("after_j", "$.email")).alias("email_hash"),
    )
    .withColumn(
        "row_hash",
        F.when(
            ~F.col("is_deleted"),
            row_hash("tbl", "name", "vehicle_type", "rating", "status", "phone_hash", "email_hash"),
        ),
    )
    .withColumn("sig", F.coalesce("row_hash", F.lit("DELETED")))
    .withColumn("prev_sig", F.lag("sig").over(w))
    .cache()
)

# a new version only when the hash changes; a delete closes the row and adds none
kept = st.filter(F.col("prev_sig").isNull() | (F.col("sig") != F.col("prev_sig")))
versions = kept.withColumn("valid_to", F.lead("ts").over(w)).filter(~F.col("is_deleted"))


def dim_frame(tbl):
    key_name, plain, _ = SPECS[tbl]
    return versions.filter(F.col("tbl") == tbl).select(
        F.col("key").alias(key_name),
        *[F.col(c) for c in plain],
        "phone_hash",
        "email_hash",
        "row_hash",
        F.col("ts").alias("valid_from"),
        "valid_to",
        F.col("valid_to").isNull().alias("is_current"),
        F.col("lsn").alias("source_lsn"),
        F.col("rebuilt").alias("rebuilt_create"),
        (F.col("ts") / 1000).cast("timestamp").alias("valid_from_ts"),
        (F.col("valid_to") / 1000).cast("timestamp").alias("valid_to_ts"),
    )


def merge_dim(df, path, key_name):
    """Upsert on (business key, valid_from); rows not in the result are removed."""
    if not DeltaTable.isDeltaTable(spark, path):
        df.limit(0).write.format("delta").save(path)
    target = DeltaTable.forPath(spark, path)
    cond = f"t.{key_name} = s.{key_name} AND t.valid_from = s.valid_from"
    merge = target.alias("t").merge(df.alias("s"), cond)
    merge = merge.whenMatchedUpdateAll().whenNotMatchedInsertAll()
    try:
        merge = merge.whenNotMatchedBySourceDelete()
    except AttributeError:
        print("whenNotMatchedBySourceDelete not available on this runtime, skipped")
    merge.execute()


for tbl, (key_name, _, name) in SPECS.items():
    merge_dim(dim_frame(tbl), f"{BASE}/{name}", key_name)
    print(f"{name}: merged")

# COMMAND ----------

# Reconciliation from what was actually written
hdr = (
    f"{'table':8} {'raw':>5} {'dups':>5} {'rebuilt':>7} {'nochg':>6} {'deletes':>7} {'versions':>8}"
)
print(hdr + "  conserved")
for tbl, (_key_name, _, name) in SPECS.items():
    d = spark.read.format("delta").load(f"{BASE}/{name}")
    raw_n = dec.filter(F.col("tbl") == tbl).count()
    ded_n = dedup.filter(F.col("tbl") == tbl).count()
    reb_n = events.filter((F.col("tbl") == tbl) & F.col("rebuilt")).count()
    ev_n = st.filter(F.col("tbl") == tbl).count()
    kept_n = kept.filter(F.col("tbl") == tbl).count()
    del_n = kept.filter((F.col("tbl") == tbl) & F.col("is_deleted")).count()
    out_n = d.count()
    nochg = ev_n - kept_n
    ok = (ev_n == ded_n + reb_n) and (ev_n == nochg + del_n + out_n)
    line = f"{tbl:8} {raw_n:>5} {raw_n - ded_n:>5} {reb_n:>7} {nochg:>6} {del_n:>7} {out_n:>8}"
    print(f"{line}  {ok}")

print()
for _tbl, (key_name, _, name) in SPECS.items():
    d = spark.read.format("delta").load(f"{BASE}/{name}")
    keys = d.select(key_name).distinct().count()
    cur = d.filter("is_current")
    cur_keys = cur.select(key_name).distinct().count()
    multi_cur = cur.groupBy(key_name).count().filter("count > 1").count()
    wk = Window.partitionBy(key_name).orderBy("valid_from")
    overlaps = (
        d.withColumn("nxt", F.lead("valid_from").over(wk))
        .filter(F.col("valid_to") > F.col("nxt"))
        .count()
    )
    print(
        f"{name}: keys={keys} current_rows={cur.count()} keys_with_>1_current={multi_cur} "
        f"keys_without_current(deleted)={keys - cur_keys} overlaps={overlaps}"
    )
    print(f"  columns: {d.columns}")
    assert "phone" not in d.columns and "email" not in d.columns, "raw PII column found"

# COMMAND ----------

# Demo: before and after of a changed driver
dd = spark.read.format("delta").load(f"{BASE}/dim_driver")
multi = dd.groupBy("driver_id").count().filter("count > 1").orderBy("driver_id")
print("drivers with more than one version:")
multi.show()
ids = [r["driver_id"] for r in multi.collect()]
demo_id = "DRV-0001" if "DRV-0001" in ids else ids[0]
print(f"history of {demo_id}:")
(
    dd.filter(F.col("driver_id") == demo_id)
    .orderBy("valid_from")
    .select(
        "driver_id",
        "name",
        "status",
        "rating",
        F.substring("phone_hash", 1, 10).alias("phone_hash"),
        "valid_from_ts",
        "valid_to_ts",
        "is_current",
        "rebuilt_create",
    )
    .show(truncate=False)
)

# COMMAND ----------

# Idempotency: run the same MERGE again, counts must not change
before = {
    name: spark.read.format("delta").load(f"{BASE}/{name}").count() for *_, name in SPECS.values()
}
for tbl, (key_name, _, name) in SPECS.items():
    merge_dim(dim_frame(tbl), f"{BASE}/{name}", key_name)
after = {
    name: spark.read.format("delta").load(f"{BASE}/{name}").count() for *_, name in SPECS.values()
}
print("before:", before)
print("after: ", after)
print("idempotent:", before == after)
