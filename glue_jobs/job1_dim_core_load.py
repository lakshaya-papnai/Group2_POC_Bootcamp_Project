import argparse
import sys
import boto3
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
# S3 CONFIG
# -------------------------------
BUCKET = "ttn-de-bootcamp-bronze-us-east-1"
BASE = "poc-bootcamp-grp2-bronze"
RAW_KEY = f"{BASE}/raw/vehicle_registry/vehicle_registry.csv" 

RAW_PATH = f"s3://{BUCKET}/{RAW_KEY}"
PROCESSED_PATH = f"s3://{BUCKET}/{BASE}/processed/vehicle_registry"
ZONES_PATH = f"s3://{BUCKET}/{BASE}/raw/restricted_zones/restricted_zones.json"

SILVER_BASE      = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-grp2-silver"
DIM_VEHICLE_PATH = f"{SILVER_BASE}/dim_vehicle"
DIM_ZONES_PATH   = f"{SILVER_BASE}/dim_restricted_zones"
DIM_DATE_PATH    = f"{SILVER_BASE}/dim_date"

s3 = boto3.client("s3")

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

    df = spark.read.option("header", True).schema(schema).csv(RAW_PATH)

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
        .option("header", True) \
        .csv(PROCESSED_PATH)

    print("Cleaning RAW file...")
    s3.delete_object(Bucket=BUCKET, Key=RAW_KEY)
    print("RAW file deleted")

# -------------------------------
# 2. DIM VEHICLE (BRONZE PROCESSED -> SILVER DIM_VEHICLE)
# -------------------------------
def load_dim_vehicle():
    path = f"{PROCESSED_PATH}/ingestion_date={INGESTION_DATE}/"
    print(f"Reading processed partition {INGESTION_DATE}")

    try:
        df = spark.read.option("header", True).csv(path)
    except Exception:
        print("No processed data found for today. Skipping dim_vehicle load.")
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

    print("💾 Writing dim_vehicle to Silver...")
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
        print(f"JOB 1 FAILED: {e}")
        raise
