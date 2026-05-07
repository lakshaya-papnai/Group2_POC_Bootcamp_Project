"""
OmniRoute — Streaming Job 1: Kafka Ingestor (Kafka → Bronze S3)
================================================================

PURPOSE:
  This job is an ultra-lightweight Kafka consumer whose ONLY job is to persist
  raw telemetry messages to S3 Bronze as JSON. It contains ZERO business logic —
  no joins, no deduplication, no database writes.

WHY SEPARATE FROM THE PROCESSOR?
  1. Fault Isolation — If the downstream processor crashes (Postgres down, SCD2 table
     missing, OOM on a large join), THIS job keeps running and never loses a Kafka message.
     Without this, a crash in the monolithic consumer means Kafka offsets advance but data
     is lost (Kafka retention is finite).
  2. Independent Scaling — The ingestor only needs minimal resources (1-2 executors).
     The processor can be scaled independently based on its heavier workload.
  3. Replayability — Bronze acts as an immutable audit log. If business logic changes
     (e.g., speed threshold changes from 110 to 100), we can replay from Bronze
     without needing Kafka to still have the messages.

BRONZE FORMAT: DELTA
  - Delta is chosen because it supports concurrent streaming writers AND readers.
    Writing JSON creates a `_spark_metadata` log that causes the downstream
    processor stream to hang on discovery (the "nested streaming sink" trap).
  - Delta's transaction log (_delta_log) is explicitly designed for this producer-
    consumer streaming pattern and fully solves the S3 consistency/discovery problem.
  - Partitioned by ingestion_date (UTC) for efficient downstream partition pruning.

WHY NO coalesce(1)?
  - coalesce(1) forces all data to a single partition, creating a bottleneck on one
    executor core. In streaming, this directly limits throughput.
  - It also defeats Spark's parallelism — the whole point of distributed processing.
  - Small files are handled by the downstream processor's OPTIMIZE compaction instead,
    which runs asynchronously and doesn't block ingestion.

DEPLOYMENT:
  spark-submit --master yarn --deploy-mode cluster \\
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
    ingestor.py --brokers <kafka-broker>:9092

CHECKPOINT:
  Uses a SEPARATE checkpoint from the processor so both streams track their own
  offsets independently.
"""

import argparse
import logging

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

# -------------------------------
# LOGGING
# -------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("omniroute-ingestor")

# -------------------------------
# KAFKA MESSAGE SCHEMA
# Matches the JSON structure emitted by the EC2 telemetry producer.
# We define it explicitly (rather than using schema inference) because
# streaming sources REQUIRE a fixed schema — Spark cannot infer on-the-fly.
# IMPORTANT: speed is DoubleType — producer emits decimal values like 95.4.
# Using IntegerType would silently truncate 135.5 → 135, causing precision
# loss and potentially wrong speed_flag decisions near the 110 km/h threshold.
# -------------------------------
schema = StructType([
    StructField("vin",             StringType(),  True),
    StructField("driver_id",       StringType(),  True),
    StructField("speed",           DoubleType(),  True),
    StructField("lat",             DoubleType(),  True),
    StructField("long",            DoubleType(),  True),
    StructField("event_timestamp", StringType(),  True),
])


def main():
    parser = argparse.ArgumentParser(description="Kafka → Bronze S3 Ingestor")
    parser.add_argument("--brokers",  required=True, help="Kafka bootstrap servers")
    parser.add_argument("--topic",    default="telemetry_stream", help="Kafka topic name")
    parser.add_argument("--trigger",  default="1 minute",
                        help="Micro-batch trigger interval. Shorter = lower latency, "
                             "longer = fewer files. 1 min is a good balance for Bronze.")
    args = parser.parse_args()

    # ── SPARK SESSION ─────────────────────────────────────────────
    # Delta extensions ARE required — we write Delta format to Bronze.
    # UTC timezone ensures ingestion_date partitions align globally regardless of
    # where the EMR cluster is physically located.
    spark = (
        SparkSession.builder
        .appName("omniroute-ingestor")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── S3 PATHS ──────────────────────────────────────────────────
    bronze_path = "s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-grp2-bronze/raw/telemetry_stream"
    # SEPARATE checkpoint from processor — each stream must track its own Kafka offsets.
    checkpoint  = "s3://ttn-de-bootcamp-bronze-us-east-1/8834_Lakshaya_bronze/checkpoints/ingestor/"

    # ── KAFKA SOURCE ──────────────────────────────────────────────
    # startingOffsets = "latest": only consume NEW messages from this point forward.
    #   - "earliest" would replay the entire Kafka topic on first run, which is dangerous
    #     if the topic has days of backlog — it would flood Bronze with duplicates.
    # failOnDataLoss = "false": if Kafka segments are deleted (retention policy), the stream
    #   continues from the next available offset instead of crashing.
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.brokers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── PARSE JSON ────────────────────────────────────────────────
    # Minimal transformation: parse the Kafka value (binary → string → struct),
    # and handle malformed event_timestamp by falling back to the Kafka broker timestamp.
    # WHY FALLBACK? The producer may send events with missing or garbage timestamps.
    # The broker timestamp (_kafka_ts) is always reliable because Kafka sets it on receipt.
    parsed = (
        raw
        .select(
            F.from_json(F.col("value").cast("string"), schema).alias("d"),
            F.col("timestamp").alias("_kafka_ts")
        )
        .select(
            "d.vin", "d.driver_id", "d.speed", "d.lat", "d.long",
            F.coalesce(
                F.to_timestamp(F.col("d.event_timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                F.col("_kafka_ts")
            ).alias("event_timestamp")
        )
        # Add ingestion_date for partitioning — this is the UTC date the message was
        # consumed, NOT the event date. This ensures all messages in a single micro-batch
        # land in the same partition, making downstream discovery simple.
        .withColumn("ingestion_date", F.to_date(F.current_timestamp()).cast("string"))
    )

    # ── WRITE TO BRONZE (append-only DELTA) ────────────────────────
    # WHY DELTA instead of JSON? Writing JSON via Structured Streaming creates a
    # `_spark_metadata` log. When a downstream stream reads from this path, it gets
    # trapped trying to resolve S3 consistency via this metadata log, causing
    # discovery hangs (the "nested streaming sink" trap).
    # Delta Lake provides native ACID streaming sinks/sources, bypassing this entirely.
    # NOTE: partitionBy is specified here. Delta will create ingestion_date= subfolders.
    query = (
        parsed.writeStream
        .format("delta")
        .outputMode("append")
        .partitionBy("ingestion_date")
        .option("checkpointLocation", checkpoint)
        .option("path", bronze_path)
        .trigger(processingTime=args.trigger)
        .start()
    )

    log.info("Ingestor started — consuming Kafka → Bronze S3")
    query.awaitTermination()


if __name__ == "__main__":
    main()
