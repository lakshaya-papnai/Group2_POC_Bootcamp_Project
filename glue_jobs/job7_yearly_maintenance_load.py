import argparse
import sys
import boto3
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


BUCKET = "ttn-de-bootcamp-bronze-us-east-1"
BASE = "poc-bootcamp-grp2-bronze"

RAW_KEY = f"{BASE}/raw/maintenance_logs/maintenance_schedules.csv"
RAW_PATH = f"s3://{BUCKET}/{RAW_KEY}"

PROCESSED_PATH = f"s3://{BUCKET}/{BASE}/processed/maintenance_logs"

SILVER_BASE          = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-grp2-silver"
DIM_MAINTENANCE_PATH = f"{SILVER_BASE}/dim_maintenance_schedule"

s3 = boto3.client("s3")

# -------------------------------
# 1. INGEST RAW → PROCESSED 
# -------------------------------
def ingest_maintenance_logs():

    schema = StructType([
        StructField("vin", StringType(), True),
        StructField("service_date", StringType(), True),
        StructField("service_type", StringType(), True)
    ])

    df = spark.read.option("header", True).schema(schema).csv(RAW_PATH)

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
        .option("header", True) \
        .csv(PROCESSED_PATH)
        
    return True

# -------------------------------
# 2. BUILD SILVER TABLE
# -------------------------------
def build_dim_maintenance():
    
    path = f"{PROCESSED_PATH}/ingestion_year={INGESTION_YEAR}/"
    df = spark.read.option("header", True).csv(path)

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
            s3.delete_object(Bucket=BUCKET, Key=RAW_KEY)

    except Exception as e:
        print(f"FAILED: {e}")
        raise