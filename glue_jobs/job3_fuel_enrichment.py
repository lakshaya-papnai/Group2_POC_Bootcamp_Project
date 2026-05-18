import argparse
import sys
import os
import boto3
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass  # On AWS Glue, environment variables are injected as Job Parameters
from config import cfg
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, trim, length, 
    row_number, desc, to_timestamp
)
from pyspark.sql.types import *
from pyspark.sql.window import Window


# -------------------------------
# PARAMETERS
# -------------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--ingestion_date", required=True)
args, _ = parser.parse_known_args()

INGESTION_DATE = args.ingestion_date

print(f"Starting job_fuel_enrichment (CLEANSE ONLY) for date: {INGESTION_DATE}")

# -------------------------------
# SPARK SESSION
# -------------------------------
spark = SparkSession.builder \
    .appName("job_fuel_enrichment") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------
# S3 CONFIG  (from config.py / .env)
# -------------------------------
BUCKET         = cfg.BRONZE_BUCKET
RAW_KEY        = cfg.RAW_FUEL_TRANSACTIONS_KEY

RAW_PATH        = cfg.RAW_FUEL_TRANSACTIONS_PATH
PROCESSED_PATH  = cfg.PROCESSED_FUEL_RECEIPTS_PATH
FACT_FUEL_PATH  = cfg.FACT_FUEL_PATH

s3 = boto3.client("s3")

def verified_delete(raw_key: str, processed_prefix: str) -> None:
    """Delete raw_key ONLY after confirming processed_prefix has at least one file."""
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=processed_prefix, MaxKeys=1)
    if response.get("KeyCount", 0) == 0:
        raise RuntimeError(
            f"SAFETY CHECK FAILED: processed prefix '{processed_prefix}' is empty. "
            f"Refusing to delete raw key '{raw_key}'. Investigate the Spark write."
        )
    print(f"Verified: processed data exists at s3://{BUCKET}/{processed_prefix}")
    s3.delete_object(Bucket=BUCKET, Key=raw_key)
    print(f"RAW file deleted: {raw_key}")

# -------------------------------
# 1. INGEST RAW → PROCESSED 
# -------------------------------
def ingest_fuel_receipts():
    print("Reading RAW fuel receipts...")

    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("vin", StringType(), True),
        StructField("fuel_liters", DoubleType(), True),
        StructField("odometer_reading", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])

    df = spark.read.option("header", True).option("delimiter", "\x01").schema(schema).csv(RAW_PATH)

    clean_df = df.filter(
        # String checks
        (col("transaction_id").isNotNull()) & (length(trim(col("transaction_id"))) > 0) &
        (col("vin").isNotNull()) & (length(trim(col("vin"))) > 0) &
        (col("timestamp").isNotNull()) & (length(trim(col("timestamp"))) > 0) &
        # Numeric physics checks (Fuel cannot be 0, Odometer cannot be negative)
        (col("fuel_liters").isNotNull()) & (col("fuel_liters") > 0.0) &
        (col("odometer_reading").isNotNull()) & (col("odometer_reading") >= 0.0)
    )

    if clean_df.rdd.isEmpty():        
        return False

    clean_df = clean_df.withColumn("ingestion_date", lit(INGESTION_DATE))

    clean_df.write \
        .mode("overwrite") \
        .partitionBy("ingestion_date") \
        .option("delimiter", "\x01") \
        .csv(PROCESSED_PATH)
        
    return True

# -------------------------------
# 2. BUILD SILVER TABLE (CLEANSE & DEDUP ONLY)
# -------------------------------
def build_fact_fuel():
    print(f" Reading processed fuel receipts for {INGESTION_DATE}")

    path = f"{PROCESSED_PATH}/ingestion_date={INGESTION_DATE}/"
    df = spark.read.option("header", True).option("delimiter", "\x01").csv(path)

    if df.rdd.isEmpty():
        raise Exception("Processed fuel receipts missing")

    df = df.withColumn("fuel_liters", col("fuel_liters").cast("double")) \
           .withColumn("odometer_reading", col("odometer_reading").cast("double"))

    df = df.withColumn("event_timestamp", to_timestamp(col("timestamp")))
    df = df.filter(col("event_timestamp").isNotNull())

    if df.rdd.isEmpty():   
        return

    # ---------------------------------------------------------
    # DETERMINISTIC DEDUPLICATION
    # ---------------------------------------------------------
    dedup_window = Window.partitionBy("transaction_id").orderBy(desc("odometer_reading"))
    df = df.withColumn("rn", row_number().over(dedup_window)) \
           .filter(col("rn") == 1) \
           .drop("rn")

   
    df = df.select(
        "transaction_id",
        "vin",
        "event_timestamp",
        "odometer_reading",
        "fuel_liters",
        lit(INGESTION_DATE).alias("ingestion_date")
    ).withColumn("ingestion_timestamp", current_timestamp())

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("ingestion_date") \
        .save(FACT_FUEL_PATH)

# -------------------------------
# MAIN
# -------------------------------
if __name__ == "__main__":

    try:
        has_data = ingest_fuel_receipts()
        
        if has_data:
            build_fact_fuel()
            processed_s3_prefix = PROCESSED_PATH.replace(f"s3://{BUCKET}/", "") + f"/ingestion_date={INGESTION_DATE}/"
            verified_delete(RAW_KEY, processed_s3_prefix)
 
    except Exception as e:
        import traceback
        print(f"FAILED: {e}")
        print("Detailed Error Traceback:")
        print(traceback.format_exc())
        raise
