import argparse
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lag, when, lit, current_timestamp,
    sum as _sum, to_date, row_number, desc
)
from pyspark.sql.window import Window

# -------------------------------
# PARAMETERS
# -------------------------------
parser = argparse.ArgumentParser()

parser.add_argument("--ingestion_date", required=True)
parser.add_argument("--db_host", required=True)
parser.add_argument("--db_name", required=True)
parser.add_argument("--db_user", required=True)
parser.add_argument("--db_password", required=True)

args, _ = parser.parse_known_args()

INGESTION_DATE = args.ingestion_date
DB_HOST = args.db_host
DB_NAME = args.db_name
DB_USER = args.db_user
DB_PASS = args.db_password

# -------------------------------
# PATHS
# -------------------------------
SILVER_FUEL_PATH = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-grp2-silver/fact_fuel_transactions"
DIM_VEHICLE_PATH = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-grp2-silver/dim_vehicle"
DIM_DATE_PATH = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-grp2-silver/dim_date"
DIM_MAINTENANCE_PATH = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-grp2-silver/dim_maintenance_schedule"

GOLD_PATH = "s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-grp2-gold/fuel_efficiency_audit"

# -------------------------------
# MAIN
# -------------------------------
def main():

    spark = SparkSession.builder \
        .appName("job_gold_fuel_audit") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # -------------------------------
    # LOAD
    # -------------------------------
    fuel_df = spark.read.format("delta").load(SILVER_FUEL_PATH)
    vehicle_df = spark.read.format("delta").load(DIM_VEHICLE_PATH)
    date_df = spark.read.format("delta").load(DIM_DATE_PATH)
    maintenance_df = spark.read.format("delta").load(DIM_MAINTENANCE_PATH)

    fuel_df = fuel_df.withColumn("transaction_date", to_date(col("event_timestamp")))

    # ── FILTER TO THIS INGESTION BATCH ───────────────────────────────────
    # Source data is historical (event timestamps are old dates).
    # Job 3 stamps every row with ingestion_date = the date the job ran.
    # We filter on ingestion_date (processing batch key), NOT transaction_date
    # (the actual event date from the old data) to get this run's records.
    batch_df = fuel_df.filter(col("ingestion_date") == INGESTION_DATE)
    batch_count = batch_df.count()
    print(f"📊 ingestion_date='{INGESTION_DATE}' batch size: {batch_count} rows")

    if batch_count == 0:
        print(f"⚠️ No fuel records for ingestion_date={INGESTION_DATE}. "
              f"Ensure Job 3 ran successfully for this date first.")
        spark.stop()
        return

    # Get the distinct VINs present in this batch
    batch_vins = batch_df.select("vin").distinct()

    # Keep ALL historical Silver rows for those VINs so the LAG window
    # can compute distance from the previous odometer reading
    fuel_df = fuel_df.join(batch_vins, "vin", "inner")

    # ── LAG LOGIC (needs full VIN history for correct prev_odometer) ───────
    window_spec = Window.partitionBy("vin") \
        .orderBy("event_timestamp", "odometer_reading")

    fuel_df = fuel_df \
        .withColumn("prev_odometer", lag("odometer_reading").over(window_spec)) \
        .withColumn("distance_driven", col("odometer_reading") - col("prev_odometer"))

    # ── FILTER: keep only THIS BATCH's rows with valid distance ────────────
    # After LAG, filter back to ingestion_date=INGESTION_DATE rows only.
    # This ensures prev_odometer used full history but results are for this batch.
    fuel_df = fuel_df \
        .filter(col("ingestion_date") == INGESTION_DATE) \
        .filter(col("prev_odometer").isNotNull()) \
        .filter(
            col("distance_driven").isNotNull() &
            (col("distance_driven") > 0) &
            (col("distance_driven") <= 2000)
        )
    print(f"=== after LAG + validity filter: {fuel_df.count()} rows ===")

    # ── JOIN DIM_VEHICLE ─────────────────────────────────────────────
    fuel_df = fuel_df.join(vehicle_df, "vin") \
        .filter(col("baseline_kmpl").isNotNull())
    print(f"=== after dim_vehicle join: {fuel_df.count()} rows ===")

    # ── JOIN DIM_DATE on transaction_date (actual event date) ──────────────
    # We still use transaction_date here to exclude weekends in the vehicle's
    # actual operating calendar (BRD requirement).
    date_for_join = date_df.select(col("date").alias("transaction_date"), "is_weekend")
    fuel_df = fuel_df.join(date_for_join, "transaction_date") \
        .filter(col("is_weekend") == False)
    print(f"=== after dim_date (weekday filter): {fuel_df.count()} rows ===")

    # ── MAINTENANCE ANTI-JOIN ──────────────────────────────────────
    fuel_df = fuel_df.join(
        maintenance_df.select(
            col("vin").alias("m_vin"),
            col("service_date")
        ),
        (fuel_df.vin == col("m_vin")) &
        (fuel_df.transaction_date == col("service_date")),
        "left_anti"
    )

    # -------------------------------
    # AGGREGATION
    # -------------------------------
    agg_df = fuel_df.groupBy(
        "vin",
        "model",
        "transaction_date",
        "baseline_kmpl"
    ).agg(
        _sum("distance_driven").alias("total_distance"),
        _sum("fuel_liters").alias("total_fuel")
    )

    final_df = agg_df \
        .filter(col("total_fuel") > 0) \
        .withColumn("km_per_liter", col("total_distance") / col("total_fuel")) \
        .withColumn("threshold_kmpl", col("baseline_kmpl") * lit(0.88)) \
        .withColumn(
            "status",
            when(col("km_per_liter") < col("threshold_kmpl"), "FLAGGED")
            .otherwise("OK")
        ) \
        .select(
            "vin",
            "model",
            col("transaction_date").alias("audit_date"),
            "km_per_liter",
            "baseline_kmpl",
            "threshold_kmpl",
            "status"
        ) \
        .withColumn("ingestion_timestamp", current_timestamp())

    # ── DEDUP: keep ONE row per VIN (the latest audit_date) ───────────────
    # BRD Gold layer = "current status" of each vehicle.
    # The batch may contain many transaction_dates for the same VIN;
    # we keep only the most recent one so the Gold table has 1 row per VIN.
    # ROW_NUMBER is deterministic because audit_date is a date (no ties).
    latest_window = Window.partitionBy("vin").orderBy(desc("audit_date"))
    final_df = (
        final_df
        .withColumn("_rn", row_number().over(latest_window))
        .filter(col("_rn") == 1)
        .drop("_rn")
        # Stamp the batch key so S3 partitioning is by ingestion run, not event date.
        .withColumn("ingestion_date", lit(INGESTION_DATE))
    )

    # Cache final_df: it is used TWICE below (S3 write + JDBC write).
    # Without cache(), Spark recomputes the entire DAG from Silver for each write action,
    # and the second computation races with spark.stop() → SparkContext cancellation errors.
    final_df = final_df.cache()
    row_count = final_df.count()
    print(f"📊 final_df row count for {INGESTION_DATE}: {row_count}")

    if row_count == 0:
        print(f"⚠️ No qualifying fuel transactions for {INGESTION_DATE}.")
        print("Possible causes: (1) job3 has not yet run for this date, "
              "(2) all records were weekends/maintenance days, "
              "(3) no vehicles had a valid previous odometer reading.")
        print("🎯 Exiting cleanly — no Gold write needed.")
        spark.stop()
        return

    # ── WRITE S3 (partition by ingestion_date = 1 folder per run) ───────────
    print(f"💾 Writing {row_count} rows to Gold S3 (ingestion_date={INGESTION_DATE})...")
    final_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("ingestion_date") \
        .option("replaceWhere", f"ingestion_date = '{INGESTION_DATE}'") \
        .save(GOLD_PATH)
    print("✅ Gold S3 write successful.")

    # ── WRITE POSTGRES (truncate → append → upsert) ─────────────────
    print("📡 Writing to Postgres...")
    jdbc_url = f"jdbc:postgresql://{DB_HOST}:5432/{DB_NAME}"
    props = {"user": DB_USER, "password": DB_PASS, "driver": "org.postgresql.Driver"}
    staging_table = "gold.fuel_efficiency_audit_stg"
    target_table  = "gold.fuel_efficiency_audit"

    upsert_sql = f"""
    INSERT INTO {target_table}
        (vin, model, audit_date, km_per_liter, baseline_kmpl, threshold_kmpl, status, ingestion_timestamp)
    SELECT vin, model, audit_date, km_per_liter, baseline_kmpl, threshold_kmpl, status, ingestion_timestamp
    FROM {staging_table}
    ON CONFLICT (vin)
    DO UPDATE SET
        model               = EXCLUDED.model,
        audit_date          = EXCLUDED.audit_date,
        km_per_liter        = EXCLUDED.km_per_liter,
        threshold_kmpl      = EXCLUDED.threshold_kmpl,
        status              = EXCLUDED.status,
        ingestion_timestamp = EXCLUDED.ingestion_timestamp;
    """

    conn, cur = None, None
    try:
        # Step 1: TRUNCATE staging before Spark writes
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur  = conn.cursor()
        cur.execute(f"TRUNCATE TABLE {staging_table};")
        conn.commit()
        cur.close()
        conn.close()
        conn, cur = None, None

        # Step 2: Spark JDBC append into staging.
        # ingestion_date is only an S3 partition key — Postgres tables don't have this column.
        pg_df = final_df.drop("ingestion_date")
        pg_df.write.mode("append").jdbc(url=jdbc_url, table=staging_table, properties=props)

        # Step 3: UPSERT staging → target
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur  = conn.cursor()
        cur.execute(upsert_sql)
        conn.commit()
        print("✅ Postgres UPSERT successful.")
    except Exception as e:
        if conn: conn.rollback()
        print(f"❌ Postgres write failed: {e}")
        raise
    finally:
        if cur:  cur.close()
        if conn: conn.close()

    final_df.unpersist()   # release cached memory after both writes are done
    spark.stop()
    print("🎯 Job completed successfully")


if __name__ == "__main__":
    main()