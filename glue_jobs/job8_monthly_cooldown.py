"""
OmniRoute — Job 8: Monthly Cooldown (Rollover) + Rate Deduction Report
======================================================================

Runs on the 1st of every month via omniroute_monthly_dag.

This job performs three operations IN ORDER:

  1. REPORT GENERATION (BRD §6.5.1)
     Reads PREVIOUS month's Gold data and FULL OUTER JOINs with active SCD2
     fleet to produce a Monthly Driver Rate Deduction Report (.txt).
     This captures the month-end state BEFORE any rollover happens.

  2. ROLLOVER (Monthly Cooldown)
     Creates NEW month rows in the Gold Delta table:
       - ACTIVE drivers  → strike_count=0, rate restored to base_rate
       - SUSPENDED drivers → carried forward with strike_count=10
     History is NEVER overwritten. May's partition stays intact forever.

  3. POSTGRES SYNC
     Inserts the new-month rollover rows into Postgres using the staging
     table pattern (TRUNCATE staging → JDBC write → UPSERT to target).

WHY ROLLOVER INSTEAD OF UPDATE?
  An in-place UPDATE would overwrite May's rows, destroying historical records.
  With rollover, each month is an immutable partition. HR can always query
  any past month to see the exact state at that time.
"""

import argparse
import boto3
import psycopg2
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# -------------------------------
# PARAMETERS
# -------------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--execution_date", required=True)
parser.add_argument("--db_host",        required=True)
parser.add_argument("--db_name",        required=True)
parser.add_argument("--db_user",        required=True)
parser.add_argument("--db_password",    required=True)
parser.add_argument("--db_port",        default="5432")
args, _ = parser.parse_known_args()

EXECUTION_DATE = args.execution_date
SUSPENSION_THRESHOLD = 10

# Derive months
exec_dt      = datetime.strptime(EXECUTION_DATE, "%Y-%m-%d")
prev_month   = (exec_dt - timedelta(days=1)).strftime("%Y-%m")   # e.g., "2026-05"
new_month    = exec_dt.strftime("%Y-%m")                         # e.g., "2026-06"

# S3 Paths
GOLD_DELTA_PATH = "s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-grp2-gold/driver_safety_status"
SCD2_PATH       = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-grp2-silver/dim_asset_history_scd2"
GOLD_BUCKET     = "ttn-de-bootcamp-gold-us-east-1"
REPORT_PREFIX   = "poc-bootcamp-grp2-gold/fleet_reports"

# Postgres
JDBC_URL = f"jdbc:postgresql://{args.db_host}:{args.db_port}/{args.db_name}"
JDBC_PROPS = {
    "user":     args.db_user,
    "password": args.db_password,
    "driver":   "org.postgresql.Driver"
}
PG_CONNECT = {
    "host":     args.db_host,
    "port":     args.db_port,
    "database": args.db_name,
    "user":     args.db_user,
    "password": args.db_password,
}


# ─────────────────────────────────────────────────────────────────
# POSTGRES SYNC (staging table pattern, same as processor.py)
# ─────────────────────────────────────────────────────────────────
def sync_to_postgres(df):
    """Write rollover rows to Postgres via staging table upsert."""
    if df.rdd.isEmpty():
        print("No rows to sync to Postgres.")
        return

    stg_table = "gold.driver_safety_status_stg"
    tgt_table = "gold.driver_safety_status"

    pg_df = df.select("driver_id", "base_rate", "strike_count",
                      "current_adjusted_rate", "status", "month")

    # Step 1: Truncate staging
    conn = psycopg2.connect(**PG_CONNECT, connect_timeout=15)
    try:
        cur = conn.cursor()
        cur.execute(f"TRUNCATE TABLE {stg_table};")
        conn.commit()
        cur.close()
    finally:
        conn.close()

    # Step 2: Spark JDBC write to staging
    pg_df.write.mode("append").jdbc(url=JDBC_URL, table=stg_table, properties=JDBC_PROPS)

    # Step 3: Upsert staging → target
    upsert_sql = f"""
    INSERT INTO {tgt_table}
        (driver_id, base_rate, strike_count, current_adjusted_rate, status, month)
    SELECT driver_id, base_rate, strike_count, current_adjusted_rate, status, month
    FROM   {stg_table}
    ON CONFLICT (driver_id, month)
    DO UPDATE SET
        strike_count          = EXCLUDED.strike_count,
        current_adjusted_rate = EXCLUDED.current_adjusted_rate,
        status                = EXCLUDED.status,
        base_rate             = EXCLUDED.base_rate,
        updated_at            = NOW();
    """

    conn = psycopg2.connect(**PG_CONNECT, connect_timeout=15)
    try:
        cur = conn.cursor()
        cur.execute(upsert_sql)
        conn.commit()
        print(f"✅ Postgres UPSERT complete — {tgt_table}")
        cur.close()
    except Exception as e:
        conn.rollback()
        print(f"❌ Postgres UPSERT failed: {e}")
        raise
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────
# REPORT GENERATION (BRD §6.5.1)
# ─────────────────────────────────────────────────────────────────
def generate_report(report_df):
    """Format and upload Monthly Driver Rate Deduction Report (.txt) to S3."""
    s3_key = f"{REPORT_PREFIX}/{prev_month}/driver_rate_deduction_report.txt"

    rows = (
        report_df
        .withColumn("total_rate_deduction",
                     F.round(F.col("base_rate") - F.col("current_adjusted_rate"), 2))
        .select("driver_id", "base_rate", "strike_count",
                "current_adjusted_rate", "total_rate_deduction", "status")
        .collect()
    )

    # Sort: suspended first, then by strikes desc, then driver_id asc
    rows.sort(key=lambda r: (0 if r.status == "SUSPENDED" else 1,
                             -r.strike_count, r.driver_id))

    active_rows    = [r for r in rows if r.status == "ACTIVE"]
    suspended_rows = [r for r in rows if r.status == "SUSPENDED"]

    sep  = "=" * 72
    sep2 = "-" * 72

    lines = [
        sep,
        "  OMNI ROUTE SMART LOGISTICS ENGINE",
        "  MONTHLY DRIVER RATE DEDUCTION REPORT",
        f"  Report Month  : {prev_month}",
        f"  Generated At  : {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
        f"  Total Drivers : {len(rows)}  "
        f"(Active: {len(active_rows)}  |  Suspended: {len(suspended_rows)})",
        sep, "",
        "SECTION A — SUSPENDED DRIVERS  (excluded from monthly cooldown)",
        sep2,
    ]

    hdr = (f"  {'Driver ID':<20} {'Base Rate':>10} {'Strikes':>8} "
           f"{'Deduction':>12} {'Final Rate':>12} {'Status':<12}")
    div = f"  {'-'*20} {'-'*10} {'-'*8} {'-'*12} {'-'*12} {'-'*12}"

    if suspended_rows:
        lines += [hdr, div]
        for r in suspended_rows:
            lines.append(
                f"  {r.driver_id:<20} {r.base_rate:>10.2f} {r.strike_count:>8} "
                f"{r.total_rate_deduction:>12.2f} {r.current_adjusted_rate:>12.2f} "
                f"{r.status:<12}")
    else:
        lines.append("  No suspended drivers this month.")

    lines += ["",
              "SECTION B — ACTIVE DRIVERS  (strikes reset to 0 next month)",
              sep2]

    if active_rows:
        lines += [hdr, div]
        for r in active_rows:
            lines.append(
                f"  {r.driver_id:<20} {r.base_rate:>10.2f} {r.strike_count:>8} "
                f"{r.total_rate_deduction:>12.2f} {r.current_adjusted_rate:>12.2f} "
                f"{r.status:<12}")
    else:
        lines.append("  No active drivers on record.")

    lines += ["", sep, "  END OF REPORT", sep, ""]
    report_text = "\n".join(lines)

    s3 = boto3.client("s3")
    s3.put_object(Bucket=GOLD_BUCKET, Key=s3_key,
                  Body=report_text.encode("utf-8"), ContentType="text/plain")
    print(f"✅ Report uploaded → s3://{GOLD_BUCKET}/{s3_key}")
    print(f"\nREPORT PREVIEW:\n{report_text[:800]}")


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    print(f"🚀 Job 8: Monthly Cooldown for execution_date={EXECUTION_DATE}")
    print(f"   Previous month : {prev_month}")
    print(f"   New month      : {new_month}")

    spark = (
        SparkSession.builder
        .appName("omniroute-job8-monthly-cooldown")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    # ── A. READ PREVIOUS MONTH'S GOLD DATA ───────
    has_prev_data = False
    prev_gold_df  = None

    if DeltaTable.isDeltaTable(spark, GOLD_DELTA_PATH):
        all_gold = spark.read.format("delta").load(GOLD_DELTA_PATH)
        prev_gold_df = all_gold.filter(F.col("month") == prev_month)

        if prev_gold_df.count() > 0:
            has_prev_data = True
            print(f"📖 Found {prev_gold_df.count()} driver(s) in Gold for {prev_month}.")
            
    if not has_prev_data:
        print(f"⏭️  No previous month data found for {prev_month}. Skipping report generation and cooldown/rollover.")
        print("🏁 Job 8 complete.")
        spark.stop()
        return

    # ── B. READ ACTIVE FLEET FROM SCD2 ────────────────────────────
    scd2_df = (
        spark.read.format("delta").load(SCD2_PATH)
        .filter(F.col("status") == "IN-TRANSIT")
        .groupBy("driver_id")
        .agg(F.max("rate").alias("scd2_base_rate"))
    )
    print(f"📖 Found {scd2_df.count()} active driver(s) in SCD2.")

    # ── C. GENERATE REPORT (BEFORE rollover — captures month-end state) ──
    print("📊 Generating Monthly Report...")
    # FULL OUTER JOIN: Gold (prev month) ⟷ SCD2 (active fleet)
    # This captures:
    #   - Drivers with violations (in Gold)
    #   - Drivers who never violated (only in SCD2)
    #   - Drivers who left the fleet (only in Gold)
    gold_for_report = prev_gold_df.select(
        F.col("driver_id").alias("g_driver_id"),
        F.col("base_rate").alias("g_base_rate"),
        "strike_count", "current_adjusted_rate", "status"
    )

    report_df = (
        gold_for_report.join(scd2_df,
                             gold_for_report.g_driver_id == scd2_df.driver_id,
                             "full_outer")
        .withColumn("driver_id",
                    F.coalesce(F.col("g_driver_id"), F.col("driver_id")))
        .withColumn("base_rate",
                    F.coalesce(F.col("g_base_rate"), F.col("scd2_base_rate")))
        .withColumn("strike_count",
                    F.coalesce(F.col("strike_count"), F.lit(0)))
        .withColumn("current_adjusted_rate",
                    F.coalesce(F.col("current_adjusted_rate"), F.col("scd2_base_rate")))
        .withColumn("status",
                    F.coalesce(F.col("status"), F.lit("ACTIVE")))
        .select("driver_id", "base_rate", "strike_count",
                "current_adjusted_rate", "status")
    )

    generate_report(report_df)

    # ── D. ROLLOVER — CREATE NEW MONTH ROWS ───────────────────────
    print(f"🔄 Creating rollover rows for {new_month}...")

    # EXCEPTION PATTERN: Only rollover drivers who had violations last month.
    # ACTIVE drivers (who had 1-9 strikes) reset to 0 strikes.
    # SUSPENDED drivers (10 strikes) carry forward as SUSPENDED.
    
    # 4. Apply the exact BRD Cooldown Rules
    rollover_df = (
        prev_gold_df
        .withColumn("month", F.lit(new_month))
        # ACTIVE → reset to 0 strikes, restore base_rate
        .withColumn("strike_count",
                    F.when(F.col("status") == "ACTIVE", F.lit(0))
                     .otherwise(F.col("strike_count")))
        .withColumn("current_adjusted_rate",
                    F.when(F.col("status") == "ACTIVE", F.col("base_rate"))
                     .otherwise(F.col("current_adjusted_rate")))
        .withColumn("last_batch_id", F.lit(0))
        .select("driver_id", "base_rate", "strike_count",
                "current_adjusted_rate", "status", "month", "last_batch_id")
    )

    active_count    = rollover_df.filter(F.col("status") == "ACTIVE").count()
    suspended_count = rollover_df.filter(F.col("status") == "SUSPENDED").count()
    print(f"   RESET TO 0 STRIKES  : {active_count} driver(s)")
    print(f"   SUSPENDED (carried) : {suspended_count} driver(s)")

    # Write to Gold Delta — use MERGE for idempotency.
    # If Job 8 reruns, or streaming already wrote some June rows,
    # MERGE only inserts rows that don't already exist.
    if DeltaTable.isDeltaTable(spark, GOLD_DELTA_PATH):
        target = DeltaTable.forPath(spark, GOLD_DELTA_PATH)
        target.alias("t").merge(
            rollover_df.alias("s"),
            "t.driver_id = s.driver_id AND t.month = s.month"
        ).whenNotMatchedInsert(values={
            "driver_id":             F.col("s.driver_id"),
            "base_rate":             F.col("s.base_rate"),
            "strike_count":          F.col("s.strike_count"),
            "current_adjusted_rate": F.col("s.current_adjusted_rate"),
            "status":                F.col("s.status"),
            "month":                 F.col("s.month"),
            "last_batch_id":         F.col("s.last_batch_id"),
        }).execute()
    else:
        rollover_df.write.format("delta") \
            .mode("overwrite").partitionBy("month") \
            .save(GOLD_DELTA_PATH)

    print("✅ Gold Delta rollover complete.")

    # ── E. SYNC TO POSTGRES ───────────────────────────────────
    print("🔄 Syncing rollover to Postgres...")
    sync_to_postgres(rollover_df)

    print("🏁 Job 8 complete.")
    spark.stop()


if __name__ == "__main__":
    main()
