import argparse
import psycopg2
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
# PATHS
# -------------------------------
SILVER_SCD2_PATH    = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-grp2-silver/dim_asset_history_scd2"
SILVER_VEHICLE_PATH = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-grp2-silver/dim_vehicle"
GOLD_PATH           = "s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-grp2-gold/active_fleet_snapshot"

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

    # TRUNCATE staging before Spark write
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASS, connect_timeout=10
    )
    cur = conn.cursor()
    cur.execute(f" TRUNCATE TABLE {staging_table};")
    conn.commit()
    cur.close()
    conn.close()

    # APPEND into staging via Spark JDBC
    df.write.mode("append").jdbc(url=jdbc_url, table=staging_table, properties=props)

    # UPSERT staging → target    
    upsert_sql = f"""
    INSERT INTO {target_table} (model, no_of_active_vehicles, snapshot_date, snapshot_time)
    SELECT model, no_of_active_vehicles, snapshot_date, snapshot_time
    FROM {staging_table}
    ON CONFLICT (model, snapshot_date)
    DO UPDATE SET
        no_of_active_vehicles = EXCLUDED.no_of_active_vehicles,
        snapshot_time         = EXCLUDED.snapshot_time;
    """

    conn = None
    cur  = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME,
            user=DB_USER, password=DB_PASS, connect_timeout=10
        )
        cur = conn.cursor()
        cur.execute(upsert_sql)
        conn.commit()
        
    except Exception as e:
        if conn:
            conn.rollback()        
        raise

    finally:
        if cur:  cur.close()
        if conn: conn.close()

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
    main()