import argparse
import sys
import boto3
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
# S3 CONFIG
# -------------------------------
BUCKET = "ttn-de-bootcamp-bronze-us-east-1"
BASE = "poc-bootcamp-grp2-bronze"

RAW_KEY = f"{BASE}/raw/fuel_transactions/fuel_transactions.csv"
RAW_PATH = f"s3://{BUCKET}/{RAW_KEY}"

PROCESSED_PATH = f"s3://{BUCKET}/{BASE}/processed/fuel_receipts"

SILVER_BASE    = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-grp2-silver"
FACT_FUEL_PATH = f"{SILVER_BASE}/fact_fuel_transactions"

s3 = boto3.client("s3")

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

    df = spark.read.option("header", True).schema(schema).csv(RAW_PATH)

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
        .option("header", True) \
        .csv(PROCESSED_PATH)
        
    return True

# -------------------------------
# 2. BUILD SILVER TABLE (CLEANSE & DEDUP ONLY)
# -------------------------------
def build_fact_fuel():
    print(f" Reading processed fuel receipts for {INGESTION_DATE}")

    path = f"{PROCESSED_PATH}/ingestion_date={INGESTION_DATE}/"
    df = spark.read.option("header", True).csv(path)

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
            s3.delete_object(Bucket=BUCKET, Key=RAW_KEY)
 
    except Exception as e:
        print(f"FAILED: {e}")
        raise