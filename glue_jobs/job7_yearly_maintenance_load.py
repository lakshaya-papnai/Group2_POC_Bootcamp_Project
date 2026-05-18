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
from pyspark.sql.functions import col, to_date, current_timestamp, lit, trim, length, row_number, desc
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

parser = argparse.ArgumentParser()
parser.add_argument("--ingestion_date", required=True)
args, _ = parser.parse_known_args()

INGESTION_DATE = args.ingestion_date
INGESTION_YEAR = INGESTION_DATE[:4]

spark = SparkSession.builder \
    .appName("job_yearly_maintenance_load") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# S3 CONFIG  (from config.py / .env)
BUCKET               = cfg.BRONZE_BUCKET
RAW_KEY              = cfg.RAW_MAINTENANCE_KEY

RAW_PATH             = cfg.RAW_MAINTENANCE_PATH
PROCESSED_PATH       = cfg.PROCESSED_MAINTENANCE_PATH
DIM_MAINTENANCE_PATH = cfg.DIM_MAINTENANCE_PATH

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
def ingest_maintenance_logs():

    schema = StructType([
        StructField("vin", StringType(), True),
        StructField("service_date", StringType(), True),
        StructField("service_type", StringType(), True)
    ])

    df = spark.read.option("header", True).option("delimiter", "\x01").schema(schema).csv(RAW_PATH)

    #  Missing & Whitespace checks
    clean_df = df.filter(
        (col("vin").isNotNull()) & (length(trim(col("vin"))) > 0) &
        (col("service_date").isNotNull()) & (length(trim(col("service_date"))) > 0) &
        (col("service_type").isNotNull()) & (length(trim(col("service_type"))) > 0)
    )

    if clean_df.rdd.isEmpty():
        return False

    clean_df = clean_df.withColumn("ingestion_year", lit(INGESTION_YEAR))


    clean_df.write \
        .mode("overwrite") \
        .partitionBy("ingestion_year") \
        .option("delimiter", "\x01") \
        .csv(PROCESSED_PATH)
        
    return True

# -------------------------------
# 2. BUILD SILVER TABLE
# -------------------------------
def build_dim_maintenance():
    
    path = f"{PROCESSED_PATH}/ingestion_year={INGESTION_YEAR}/"
    df = spark.read.option("header", True).option("delimiter", "\x01").csv(path)

    if df.rdd.isEmpty():
        raise Exception("Processed maintenance_logs missing")
        
    df = df.withColumn("parsed_date", to_date(col("service_date")))   

    df = df.filter(col("parsed_date").isNotNull())

    if df.rdd.isEmpty():        
        return

#deterministic deduplication
    window_spec = Window.partitionBy("vin", "parsed_date").orderBy(desc("service_type"))
    
    df = df.withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .drop("rn", "service_date") \
        .withColumnRenamed("parsed_date", "service_date") \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("ingestion_year", lit(INGESTION_YEAR))

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("ingestion_year") \
        .save(DIM_MAINTENANCE_PATH)

if __name__ == "__main__":
    try:
        has_data = ingest_maintenance_logs()
        
        if has_data:
            build_dim_maintenance()
            processed_s3_prefix = PROCESSED_PATH.replace(f"s3://{BUCKET}/", "") + f"/ingestion_year={INGESTION_YEAR}/"
            verified_delete(RAW_KEY, processed_s3_prefix)

    except Exception as e:
        import traceback
        print(f"FAILED: {e}")
        print("Detailed Error Traceback:")
        print(traceback.format_exc())
        raise
