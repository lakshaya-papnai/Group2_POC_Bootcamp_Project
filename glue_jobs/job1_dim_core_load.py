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
    col, dayofweek, current_timestamp, lit, 
    row_number, desc, trim, length
)
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# -------------------------------
# PARAMETERS
# -------------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--ingestion_date", required=True)
args, _ = parser.parse_known_args()

INGESTION_DATE = args.ingestion_date

print(f"Starting job_dim_core_load for {INGESTION_DATE}")

# -------------------------------
# SPARK SESSION
# -------------------------------
spark = SparkSession.builder \
    .appName("job_dim_core_load") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------
# S3 CONFIG  (from config.py / .env)
# -------------------------------
BUCKET         = cfg.BRONZE_BUCKET
RAW_KEY        = cfg.RAW_VEHICLE_REGISTRY_KEY

RAW_PATH        = cfg.RAW_VEHICLE_REGISTRY_PATH
PROCESSED_PATH  = cfg.PROCESSED_VEHICLE_REGISTRY_PATH
ZONES_PATH      = f"{cfg.BRONZE_BASE}/raw/restricted_zones/restricted_zones.json"

DIM_VEHICLE_PATH = cfg.DIM_VEHICLE_PATH
DIM_ZONES_PATH   = cfg.DIM_ZONES_PATH
DIM_DATE_PATH    = cfg.DIM_DATE_PATH

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
# 1. INGEST RAW → PROCESSED (VEHICLE )
# -------------------------------
def ingest_vehicle_registry():
    print("Reading RAW vehicle_registry...")

    schema = StructType([
        StructField("vin", StringType(), True),
        StructField("model", StringType(), True),
        StructField("mfg_year", IntegerType(), True),
        StructField("fuel_type", StringType(), True),
        StructField("baseline_kmpl", DoubleType(), True)
    ])

    df = spark.read.option("header", True).option("delimiter", "\x01").schema(schema).csv(RAW_PATH)

    # ---------------------------------------------------------
    # CLEAN DATA CHECKING: VEHICLE REGISTRY
    # ---------------------------------------------------------
    clean_df = df.filter(
        # String checks
        (col("vin").isNotNull()) & (length(trim(col("vin"))) > 0) &
        (col("model").isNotNull()) & (length(trim(col("model"))) > 0) &
        (col("fuel_type").isNotNull()) & (length(trim(col("fuel_type"))) > 0) &
        # Numeric checks
        (col("mfg_year").isNotNull()) & (col("mfg_year") >= 1980) & (col("mfg_year") <= 2026) &
        (col("baseline_kmpl").isNotNull()) & (col("baseline_kmpl") > 0.0)
    )

    if clean_df.rdd.isEmpty():
        print("RAW vehicle_registry is empty or 100% corrupt. Skipping processed write.")
        return

    clean_df = clean_df.withColumn("ingestion_date", lit(INGESTION_DATE))

    print("Writing strictly validated data to PROCESSED Bronze...")
    clean_df.write \
        .mode("overwrite") \
        .partitionBy("ingestion_date") \
        .option("delimiter", "\x01") \
        .csv(PROCESSED_PATH)

    print("Cleaning RAW file...")
    # PROCESSED_PATH is s3://bucket/prefix/...  strip the s3://bucket/ to get the S3 key prefix
    processed_s3_prefix = PROCESSED_PATH.replace(f"s3://{BUCKET}/", "") + f"/ingestion_date={INGESTION_DATE}/"
    verified_delete(RAW_KEY, processed_s3_prefix)

# -------------------------------
# 2. DIM VEHICLE (BRONZE PROCESSED -> SILVER DIM_VEHICLE)
# -------------------------------
def load_dim_vehicle():
    path = f"{PROCESSED_PATH}/ingestion_date={INGESTION_DATE}/"
    print(f"Reading processed partition {INGESTION_DATE}")

    try:
        df = spark.read.option("header", True).option("delimiter", "\x01").csv(path)
    except Exception as e:
        import traceback
        print(f"[load_dim_vehicle] Could not read processed partition '{path}': {e}")
        print("Detailed Error Traceback:")
        print(traceback.format_exc())
        print("Skipping dim_vehicle load for this run.")
        return

    if df.rdd.isEmpty():
        return

    # Cast types properly for mathematical sorting
    df = df.withColumn("mfg_year", col("mfg_year").cast("int")) \
           .withColumn("baseline_kmpl", col("baseline_kmpl").cast("double"))

    # Deterministic Deduplication: Keep newest mfg_year, then highest kmpl
    window_spec = Window.partitionBy("vin").orderBy(desc("mfg_year"), desc("baseline_kmpl"))

    df = df.withColumn("rn", row_number().over(window_spec)) \
           .filter(col("rn") == 1) \
           .drop("rn")

    df = df.withColumn("ingestion_timestamp", current_timestamp()) \
           .withColumn("source_partition", lit(INGESTION_DATE))

    print("Writing dim_vehicle to Silver...")
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(DIM_VEHICLE_PATH)

# -------------------------------
# 3. DIM ZONES (this will run daily)
# -------------------------------
def load_dim_zones():
    print("Processing restricted zones...")

    schema = StructType([
        StructField("zone_name", StringType(), True),
        StructField("min_lat", FloatType(), True),
        StructField("max_lat", FloatType(), True),
        StructField("min_long", FloatType(), True),
        StructField("max_long", FloatType(), True)
    ])

    df = spark.read.option("multiline", "true").schema(schema).json(ZONES_PATH)

    # ---------------------------------------------------------
    #  NULL DATA checking: restricted_zones.json
    # ---------------------------------------------------------
    clean_df = df.filter(
        # String checks
        (col("zone_name").isNotNull()) & (length(trim(col("zone_name"))) > 0) &
        # Latitude limits (-90 to 90) and logical bounds
        (col("min_lat").isNotNull()) & (col("min_lat") >= -90.0) & (col("min_lat") <= 90.0) &
        (col("max_lat").isNotNull()) & (col("max_lat") >= -90.0) & (col("max_lat") <= 90.0) &
        (col("min_lat") <= col("max_lat")) &
        # Longitude limits (-180 to 180) and logical bounds
        (col("min_long").isNotNull()) & (col("min_long") >= -180.0) & (col("min_long") <= 180.0) &
        (col("max_long").isNotNull()) & (col("max_long") >= -180.0) & (col("max_long") <= 180.0) &
        (col("min_long") <= col("max_long"))
    )

    if clean_df.rdd.isEmpty():
        print("Restricted Zones data is missing or corrupt. Skipping zones load.")
        return

    clean_df = clean_df.withColumn("ingestion_timestamp", current_timestamp())

    print("Writing clean dim_restricted_zones to Silver...")
    clean_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(DIM_ZONES_PATH)

# -------------------------------
# 4. DIM DATE 
# -------------------------------
def load_dim_date():
    if not DeltaTable.isDeltaTable(spark, DIM_DATE_PATH):
        print("Creating dim_date...")

        df = spark.sql("""
            SELECT explode(sequence(
                to_date('2020-01-01'),
                to_date('2035-12-31'),
                interval 1 day
            )) AS date
        """)

        df = df.withColumn(
            "is_weekend",
            dayofweek(col("date")).isin([1, 7])
        ).withColumn("created_at", current_timestamp())

        df.write.format("delta").mode("overwrite").save(DIM_DATE_PATH)
    else:
        print("dim_date already exists.")

# -------------------------------
# MAIN
# -------------------------------
if __name__ == "__main__":
    try:
        ingest_vehicle_registry()
        load_dim_vehicle()
        load_dim_zones()
        load_dim_date()

        print("JOB 1 SUCCESS")

    except Exception as e:
        import traceback
        print(f"JOB 1 FAILED: {e}")
        print("Detailed Error Traceback:")
        print(traceback.format_exc())
        raise
