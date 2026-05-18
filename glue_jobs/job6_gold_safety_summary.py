import argparse
import os
import traceback
import psycopg2
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass  # On AWS Glue, environment variables are injected as Job Parameters
from config import cfg
from utils import execute_postgres_upsert
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, when, to_date, lit,
    to_json, collect_list, struct, row_number,
    sum as F_sum, upper, current_timestamp
)
from pyspark.sql.window import Window

parser = argparse.ArgumentParser()
parser.add_argument("--report_date",  required=True)
parser.add_argument("--db_host",      required=True)
parser.add_argument("--db_name",      required=True)
parser.add_argument("--db_user",      required=True)
parser.add_argument("--db_password",  required=True)
parser.add_argument("--db_port",      default="5432")
args, _ = parser.parse_known_args()

REPORT_DATE = args.report_date
DB_HOST = args.db_host
DB_NAME = args.db_name
DB_USER = args.db_user
DB_PASS = args.db_password
DB_PORT = args.db_port

# S3 paths  (from config.py / .env)
SILVER_SAFETY_PATH = cfg.FACT_SAFETY_PATH
GOLD_PATH          = cfg.GOLD_SAFETY_SUMMARY_PATH

def write_to_s3(df):
    print(f"Writing to Gold S3 (partition: {REPORT_DATE})")
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("report_date") \
        .option("replaceWhere", f"report_date = '{REPORT_DATE}'") \
        .save(GOLD_PATH)

def write_to_postgres(df):
    print("Writing to Postgres → gold.safety_compliance_summary")

    staging_table = "gold.safety_compliance_summary_stg"
    target_table  = "gold.safety_compliance_summary"

    jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    props = {
        "user":     DB_USER,
        "password": DB_PASS,
        "driver":   "org.postgresql.Driver"
    }

    pg_connect_kwargs = {
        "host": DB_HOST, 
        "port": DB_PORT, 
        "database": DB_NAME,
        "user": DB_USER, 
        "password": DB_PASS, 
        "connect_timeout": 10,
        "options": "-c statement_timeout=30000"
    }
    upsert_sql = f"""
    INSERT INTO {target_table}
        (report_date, total_violations, speed_violations, zone_violations, top_10_drivers, updated_at)
    SELECT
        report_date, total_violations, speed_violations, zone_violations,
        CAST(top_10_drivers AS JSONB),
        CURRENT_TIMESTAMP
    FROM {staging_table}
    ON CONFLICT (report_date)
    DO UPDATE SET
        total_violations  = EXCLUDED.total_violations,
        speed_violations  = EXCLUDED.speed_violations,
        zone_violations   = EXCLUDED.zone_violations,
        top_10_drivers    = EXCLUDED.top_10_drivers,
        updated_at        = CURRENT_TIMESTAMP;
    """

    execute_postgres_upsert(
        df=df,
        jdbc_url=jdbc_url,
        staging_table=staging_table,
        upsert_sql=upsert_sql,
        pg_connect_kwargs=pg_connect_kwargs,
        jdbc_props=props
    )

def main():
    print(f"Starting Gold Safety Summary for {REPORT_DATE}")

    spark = SparkSession.builder \
        .appName("job_gold_safety_summary") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"Loading safety violations for ingestion_date={REPORT_DATE}")
    df = spark.read.format("delta").load(SILVER_SAFETY_PATH)

    daily_df = df.filter(col("ingestion_date") == lit(REPORT_DATE))
    daily_count = daily_df.count()
    print(f"ingestion_date='{REPORT_DATE}' violations: {daily_count} rows")

    if daily_count == 0:
        spark.stop()
        return

    #  violation_type can be "SPEED", "GEOFENCE", or "SPEED_AND_GEOFENCE".

    agg_df = daily_df.groupBy().agg(
        count("*").cast("int").alias("total_violations"),
        F_sum(when(upper(col("violation_type")).contains("SPEED"),    1).otherwise(0)).cast("int").alias("speed_violations"),
        F_sum(when(upper(col("violation_type")).contains("GEOFENCE"), 1).otherwise(0)).cast("int").alias("zone_violations")
    ).withColumn("report_date", to_date(lit(REPORT_DATE)))

    window_spec = Window.orderBy(col("strikes").desc(), col("driver_id"))

    top_drivers_df = (
        daily_df
        .groupBy("driver_id")
        .agg(F_sum("strike_count").cast("int").alias("strikes"))
        .withColumn("rank", row_number().over(window_spec))
        .filter(col("rank") <= 10)
        .groupBy()
        .agg(to_json(collect_list(struct("driver_id", "strikes"))).alias("top_10_drivers"))
    )
    # Cross-join single-row aggregates and add lineage timestamp
    final_df = (
        agg_df
        .crossJoin(top_drivers_df)
        .withColumn("ingestion_timestamp", current_timestamp())
    )

    write_to_s3(final_df)
    write_to_postgres(final_df)
    spark.stop()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print(f"JOB 6 FAILED: {e}")
        print("Detailed Error Traceback:")
        print(traceback.format_exc())
        raise
