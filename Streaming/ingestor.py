"""
Streaming Job 1: Kafka Ingestor (Kafka → Bronze S3)
================================================================
PURPOSE:
  This job is an ultra-lightweight Kafka consumer whose ONLY job is to persist
  raw telemetry messages to S3 Bronze as JSON. It contains ZERO business logic —
  no joins, no deduplication, no database writes.  
"""

import argparse
import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("omniroute-ingestor")

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

    
    bronze_path = "s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-grp2-bronze/raw/telemetry_stream"
    checkpoint  = "s3://ttn-de-bootcamp-bronze-us-east-1/8834_Lakshaya_bronze/checkpoints/ingestor/"

     # startingOffsets = "latest": only consume NEW messages from this point forward.
      raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.brokers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

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
        # Add ingestion_date for partitioning 
        .withColumn("ingestion_date", F.to_date(F.current_timestamp()).cast("string"))
    )

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
