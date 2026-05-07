import argparse
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, when, to_date, lit,
    to_json, collect_list, struct, row_number,
    sum as F_sum, upper, current_timestamp
)
from pyspark.sql.window import Window

# -------------------------------
# PARAMETERS  (Glue Job args )
# -------------------------------
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

# -------------------------------
# PATHS
# -------------------------------
SILVER_SAFETY_PATH = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-grp2-silver/fact_safety_violations"
GOLD_PATH          = "s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-grp2-gold/safety_compliance_summary"

# -------------------------------
# WRITE S3  (idempotent replaceWhere)
# -------------------------------
def write_to_s3(df):
    print(f"💾 Writing to Gold S3 (partition: {REPORT_DATE})")
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("report_date") \
        .option("replaceWhere", f"report_date = '{REPORT_DATE}'") \
        .save(GOLD_PATH)

# -------------------------------
# WRITE POSTGRES  (truncate-stage → append → upsert; gold schema)
# -------------------------------
def write_to_postgres(df):
    print("📡 Writing to Postgres → gold.safety_compliance_summary")

    staging_table = "gold.safety_compliance_summary_stg"
    target_table  = "gold.safety_compliance_summary"

    jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    props = {
        "user":     DB_USER,
        "password": DB_PASS,
        "driver":   "org.postgresql.Driver"
    }

    # Step 1: TRUNCATE staging before Spark write
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASS, connect_timeout=10
    )
    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE {staging_table};")
    conn.commit()
    cur.close()
    conn.close()

    # Step 2: APPEND into staging (top_10_drivers written as TEXT, cast to JSONB in step 3)
    df.write.mode("append").jdbc(url=jdbc_url, table=staging_table, properties=props)

    # Step 3: UPSERT with JSONB cast  (BRD §6.5.2)
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

    conn = None
    cur  = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME,
            user=DB_USER, password=DB_PASS, connect_timeout=10,
            options="-c statement_timeout=30000"
        )
        cur = conn.cursor()
        cur.execute(upsert_sql)
        conn.commit()
        print("✅ Postgres UPSERT successful")
    except Exception as e:
        if conn: conn.rollback()
        print(f"❌ Postgres UPSERT failed: {e}")
        raise
    finally:
        if cur:  cur.close()
        if conn: conn.close()

# -------------------------------
# MAIN PIPELINE
# -------------------------------
def main():
    print(f"🚀 Starting Gold Safety Summary for {REPORT_DATE}")

    spark = SparkSession.builder \
        .appName("job_gold_safety_summary") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"📥 Loading safety violations for ingestion_date={REPORT_DATE}")
    df = spark.read.format("delta").load(SILVER_SAFETY_PATH)

    # ── FILTER ON INGESTION DATE (not event_timestamp) ───────────────────
    # Source telemetry timestamps are historical (old data).
    # consumer.py stamps ingestion_date = UTC date the streaming job ran.
    # We filter on ingestion_date so Job 6 aligns with the batch run, not event time.
    daily_df = df.filter(col("ingestion_date") == lit(REPORT_DATE))
    daily_count = daily_df.count()
    print(f"📊 ingestion_date='{REPORT_DATE}' violations: {daily_count} rows")

    if daily_count == 0:
        print(f"⏭️ No safety violations ingested on {REPORT_DATE}. "
              f"Ensure the streaming consumer ran and wrote Silver data for this date.")
        spark.stop()
        return

    print("🧮 Calculating Aggregations & KPIs...")

    # BRD §6.5.2: total, speed, zone violation counts
    # IMPORTANT: violation_type can be "SPEED", "GEOFENCE", or "SPEED_AND_GEOFENCE".
    # Using .contains() instead of strict equality ensures SPEED_AND_GEOFENCE is
    # counted in BOTH speed_violations AND zone_violations. This guarantees:
    #   speed_violations + zone_violations >= total_violations (combined counts in both)
    # Without this, SPEED_AND_GEOFENCE events vanish from both breakdown columns.
    agg_df = daily_df.groupBy().agg(
        count("*").cast("int").alias("total_violations"),
        F_sum(when(upper(col("violation_type")).contains("SPEED"),    1).otherwise(0)).cast("int").alias("speed_violations"),
        F_sum(when(upper(col("violation_type")).contains("GEOFENCE"), 1).otherwise(0)).cast("int").alias("zone_violations")
    ).withColumn("report_date", to_date(lit(REPORT_DATE)))

    print("🏆 Generating Top 10 Drivers JSON Array...")

    # BRD §6.5.2: Top 10 drivers by strike count
    # driver_id tiebreaker → deterministic ranking when two drivers share the same count
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
    print("🎯 job_gold_safety_summary completed successfully")


if __name__ == "__main__":
    main()