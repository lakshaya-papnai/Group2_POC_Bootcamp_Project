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
from pyspark.sql.functions import col, countDistinct, current_timestamp, lit, to_date

# -------------------------------
# PARAMETERS  (Glue Job args )
# -------------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--snapshot_date", required=True)
parser.add_argument("--db_host",       required=True)
parser.add_argument("--db_name",       required=True)
parser.add_argument("--db_user",       required=True)
parser.add_argument("--db_password",   required=True)
parser.add_argument("--db_port",       default="5432")
args, _ = parser.parse_known_args()

SNAPSHOT_DATE = args.snapshot_date
DB_HOST = args.db_host
DB_NAME = args.db_name
DB_USER = args.db_user
DB_PASS = args.db_password
DB_PORT = args.db_port

# -------------------------------
# PATHS  (from config.py / .env)
# -------------------------------
SILVER_SCD2_PATH    = cfg.SCD2_PATH
SILVER_VEHICLE_PATH = cfg.DIM_VEHICLE_PATH
GOLD_PATH           = cfg.GOLD_FLEET_SNAPSHOT_PATH

# -------------------------------
# WRITE S3  (idempotent replaceWhere)
# -------------------------------
def write_to_s3(df):    
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("snapshot_date") \
        .option("replaceWhere", f"snapshot_date = '{SNAPSHOT_DATE}'") \
        .save(GOLD_PATH)

# -------------------------------
# WRITE POSTGRES 
# -------------------------------
def write_to_postgres(df):
    
    staging_table = "gold.active_fleet_snapshot_stg"
    target_table  = "gold.active_fleet_snapshot"

    jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    props = {
        "user":     DB_USER,
        "password": DB_PASS,
        "driver":   "org.postgresql.Driver"
    }

    upsert_sql = f"""
    INSERT INTO {target_table} (model, no_of_active_vehicles, snapshot_date, snapshot_time)
    SELECT model, no_of_active_vehicles, snapshot_date, snapshot_time
    FROM {staging_table}
    ON CONFLICT (model, snapshot_date)
    DO UPDATE SET
        no_of_active_vehicles = EXCLUDED.no_of_active_vehicles,
        snapshot_time         = EXCLUDED.snapshot_time;
    """

    pg_connect_kwargs = {
        "host": DB_HOST,
        "port": DB_PORT,
        "database": DB_NAME,
        "user": DB_USER,
        "password": DB_PASS,
        "connect_timeout": 10
    }

    execute_postgres_upsert(
        df=df,
        jdbc_url=jdbc_url,
        staging_table=staging_table,
        upsert_sql=upsert_sql,
        pg_connect_kwargs=pg_connect_kwargs,
        jdbc_props=props
    )

# -------------------------------
# MAIN PIPELINE
# -------------------------------
def main():

    spark = SparkSession.builder \
        .appName("job_gold_fleet_snapshot") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    scd2_df    = spark.read.format("delta").load(SILVER_SCD2_PATH)
    vehicle_df = spark.read.format("delta").load(SILVER_VEHICLE_PATH)

    # all IN-TRANSIT vehicles, grouped by model
    active_df = scd2_df.filter(col("status") == "IN-TRANSIT")
    joined_df = active_df.join(vehicle_df, "vin", "inner")

    final_df = (
        joined_df
        .groupBy("model")
        .agg(countDistinct("vin").alias("no_of_active_vehicles"))
        .withColumn("snapshot_date", to_date(lit(SNAPSHOT_DATE)))
        .withColumn("snapshot_time", current_timestamp())
    )

    write_to_s3(final_df)
    write_to_postgres(final_df)

    spark.stop()
    
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print(f"JOB 5 FAILED: {e}")
        print("Detailed Error Traceback:")
        print(traceback.format_exc())
        raise
