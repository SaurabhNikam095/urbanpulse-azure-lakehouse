# Databricks notebook source
# ruff: noqa: E402, F821
# MAGIC %md
# MAGIC # UrbanPulse: Event Hubs to Bronze (Delta)
# MAGIC Reads trips, gps and payments from the Event Hubs Kafka endpoint and appends
# MAGIC the raw records to one Bronze Delta table. The checkpoint path is fixed, so a
# MAGIC restarted job continues from the last committed offsets.

# COMMAND ----------

NAMESPACE = "evhns-urbanpulse-dev-sn095"
STORAGE_ACCOUNT = "urbanpulsedevsn095"
TOPICS = "trips,gps,payments"
SECRET_SCOPE = "urbanpulse"
SECRET_KEY = "eventhubs-connection-string"

BRONZE_ROOT = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
BRONZE_PATH = f"{BRONZE_ROOT}/events"
CHECKPOINT_PATH = f"{BRONZE_ROOT}/_checkpoints/events"

# COMMAND ----------

from pyspark.sql import functions as F

connection_string = dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY)
jaas_config = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="$ConnectionString" password="{connection_string}";'
)

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{NAMESPACE}.servicebus.windows.net:9093")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", jaas_config)
    .option("subscribe", TOPICS)
    .option("startingOffsets", "earliest")
    .option("includeHeaders", "true")
    .option("includeHeaders", "true")
    .option("maxOffsetsPerTrigger", 100)
    .load()
)

bronze = raw.select(
    F.col("value").alias("payload"),
    F.col("headers"),
    F.col("topic"),
    F.col("partition"),
    F.col("offset"),
    F.col("timestamp").alias("source_timestamp"),
    F.current_timestamp().alias("ingest_time"),
)

# COMMAND ----------

query = (
    bronze.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start(BRONZE_PATH)
)

# COMMAND ----------

# Check: row count per topic in Bronze.
spark.read.format("delta").load(BRONZE_PATH).groupBy("topic").count().show()

# COMMAND ----------

# Stop: run this cell to stop the stream. A running stream keeps the cluster
# alive and billing, so always stop it when you are done.
for active in spark.streams.active:
    active.stop()
