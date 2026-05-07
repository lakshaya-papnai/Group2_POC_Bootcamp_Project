"""
OmniRoute — Streaming Job 2: Safety Processor (Bronze S3 → Silver → Gold → Postgres)
=====================================================================================

PURPOSE:
  This job reads raw telemetry from the Bronze S3 path (written by ingestor.py),
  applies all business logic (geofence joins, violation detection, deduplication),
  and writes results to Silver Delta, Gold Delta, and PostgreSQL.

WHY READ FROM BRONZE S3 INSTEAD OF KAFKA?
  1. Decoupling — If this processor crashes (OOM, Postgres timeout, SCD2 missing),
     the ingestor keeps pulling from Kafka. No messages are lost.
  2. Replayability — Bronze is immutable. We can restart this processor from any
     point in time without needing Kafka to retain messages.
  3. Independent Scaling — This job is CPU/memory heavy (joins, window functions,
     DB writes). It can be scaled separately from the lightweight ingestor.

SOURCE: spark.readStream on the Bronze Delta table written by ingestor.py.
  - maxFilesPerTrigger controls how many Delta file versions are consumed per micro-batch.
    This prevents the processor from being overwhelmed if the ingestor wrote a large
    backlog while the processor was down.
  - Reading from Delta (not JSON) solves the "nested streaming sink" trap where Spark
    hangs on S3 file discovery when the source is a Structured Streaming JSON sink.
    Delta's _delta_log is designed for concurrent streaming producers and consumers.

DEPLOYMENT:
  spark-submit --master yarn --deploy-mode cluster \\
    --packages io.delta:delta-spark_2.12:3.2.0 \\
    processor.py --pg_host <host> ...

CHECKPOINT:
  Uses a SEPARATE checkpoint from the ingestor. This processor tracks which Bronze
  Delta commits it has already consumed, completely independent of Kafka offsets.
"""

import argparse
import logging
import psycopg2

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# -------------------------------
# LOGGING
# -------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("omniroute-processor")

# -------------------------------
# BRONZE SCHEMA
# Delta is self-describing — schema is stored in _delta_log.
# We no longer need to declare bronze_schema explicitly when reading Delta.
# Kept here as a reference for what fields ingestor.py writes.
# -------------------------------
# vin, driver_id, speed, lat, long, event_timestamp, ingestion_date


# ─────────────────────────────────────────────────────────────────
# POSTGRES UPSERT (gold.driver_safety_status)
# Uses a staging-table pattern to avoid row-level locking issues:
#   1. TRUNCATE staging table (idempotent cleanup)
#   2. Spark APPEND writes into staging (fast bulk insert)
#   3. SQL UPSERT moves staging → target with ON CONFLICT
# This is safer than direct Spark JDBC upsert because Spark's JDBC
# writer doesn't natively support ON CONFLICT / MERGE.
# ─────────────────────────────────────────────────────────────────
def upsert_postgres(df, pg_conf):
    if df.rdd.isEmpty():
        return

    if "last_batch_id" in df.columns:
        df = df.drop("last_batch_id")

    # Safety net: Gold Delta may have physical file-level duplicates before OPTIMIZE runs.
    # dropDuplicates ensures the staging table never has two rows with the same PK,
    # which would cause "ON CONFLICT DO UPDATE cannot affect row a second time".
    df = df.dropDuplicates(["driver_id", "month"])

    jdbc_url   = pg_conf["url"]
    jdbc_props = pg_conf["props"]
    stg_table  = "gold.driver_safety_status_stg"
    tgt_table  = "gold.driver_safety_status"

    # ── Step 1: TRUNCATE staging BEFORE Spark writes ──────────────
    # If TRUNCATE were after the UPSERT, a crash between steps would
    # leave stale data in staging, causing duplicates on the next run.
    conn, cur = None, None
    try:
        conn = psycopg2.connect(**pg_conf["connect"])
        cur  = conn.cursor()
        cur.execute(f"TRUNCATE TABLE {stg_table};")
        conn.commit()
    finally:
        if cur:  cur.close()
        if conn: conn.close()

    # ── Step 2: APPEND deduplicated rows into staging ─────────────
    df.write.mode("append").jdbc(url=jdbc_url, table=stg_table, properties=jdbc_props)

    # ── Step 3: UPSERT staging → target ──────────────────────────
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

    conn, cur = None, None
    try:
        conn = psycopg2.connect(**pg_conf["connect"])
        cur  = conn.cursor()
        cur.execute(upsert_sql)
        conn.commit()
        log.info("Postgres UPSERT successful")
    except Exception as e:
        if conn: conn.rollback()
        log.error(f"Postgres UPSERT failed: {e}")
        raise
    finally:
        if cur:  cur.close()
        if conn: conn.close()


# ─────────────────────────────────────────────────────────────────
# GOLD S3 DELTA UPSERT
# Idempotent init + merge for the Gold S3 Delta table.
# Ensures that if a batch is re-run, strikes are not double-counted.
# ─────────────────────────────────────────────────────────────────
def upsert_gold_delta(spark, updates, gold_path, batch_id):
    if not DeltaTable.isDeltaTable(spark, gold_path):
        log.info(f"Initializing Gold Delta Table (Batch {batch_id})...")
        init_df = (
            updates
            .withColumn("strike_count", F.least(F.col("batch_strikes"), F.lit(10)))
            .withColumn(
                "current_adjusted_rate",
                F.greatest(
                    F.col("base_rate") * (1 - 0.05 * F.least(F.col("batch_strikes"), F.lit(10))),
                    F.lit(0.0)
                )
            )
            .withColumn(
                "status",
                F.when(F.col("batch_strikes") >= 10, "SUSPENDED").otherwise("ACTIVE")
            )
            .withColumn("last_batch_id", F.col("batch_id"))
            .select("driver_id", "base_rate", "strike_count",
                    "current_adjusted_rate", "status", "month", "last_batch_id")
        )
        init_df.write.format("delta").mode("overwrite").partitionBy("month").save(gold_path)

    else:
        target = DeltaTable.forPath(spark, gold_path)

        target.alias("t").merge(
            updates.alias("s"),
            "t.driver_id = s.driver_id AND t.month = s.month"
        ).whenMatchedUpdate(
            condition="s.batch_id > t.last_batch_id",   # idempotency guard
            set={
                "strike_count": F.least(F.col("t.strike_count") + F.col("s.batch_strikes"), F.lit(10)),
                "base_rate":    F.col("s.base_rate"),
                "current_adjusted_rate": F.greatest(
                    F.col("s.base_rate") * (
                        1 - 0.05 * F.least(F.col("t.strike_count") + F.col("s.batch_strikes"), F.lit(10))
                    ),
                    F.lit(0.0)
                ),
                "status": F.when(
                    (F.col("t.strike_count") + F.col("s.batch_strikes")) >= 10,
                    "SUSPENDED"
                ).otherwise("ACTIVE"),
                "last_batch_id": F.col("s.batch_id")
            }
        ).whenNotMatchedInsert(values={
            "driver_id":             F.col("s.driver_id"),
            "base_rate":             F.col("s.base_rate"),
            "strike_count":          F.least(F.col("s.batch_strikes"), F.lit(10)),
            "current_adjusted_rate": F.greatest(
                F.col("s.base_rate") * (1 - 0.05 * F.least(F.col("s.batch_strikes"), F.lit(10))),
                F.lit(0.0)
            ),
            "status":        F.when(F.col("s.batch_strikes") >= 10, "SUSPENDED").otherwise("ACTIVE"),
            "month":         F.col("s.month"),
            "last_batch_id": F.col("s.batch_id")
        }).execute()


def main():
    parser = argparse.ArgumentParser(description="Bronze → Silver → Gold Processor")
    parser.add_argument("--pg_host",  required=True)
    parser.add_argument("--pg_db",    required=True)
    parser.add_argument("--pg_user",  required=True)
    parser.add_argument("--pg_pass",  required=True)
    parser.add_argument("--pg_port",  default="5432")
    parser.add_argument("--trigger",  default="2 minutes",
                        help="Micro-batch interval. 2 min balances latency vs. file count.")
    args = parser.parse_args()

    # ── SPARK SESSION ─────────────────────────────────────────────
    # Delta Lake extensions are REQUIRED because this job writes to Delta tables
    # (Silver fact_safety_violations and Gold driver_safety_status).
    spark = (
        SparkSession.builder
        .appName("omniroute-processor")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions",        "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── S3 PATHS ──────────────────────────────────────────────────
    bronze_path = "s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-grp2-bronze/raw/telemetry_stream"
    silver_path = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-grp2-silver/fact_safety_violations"
    gold_path   = "s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-grp2-gold/driver_safety_status"
    scd2_path   = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-grp2-silver/dim_asset_history_scd2"
    zones_path  = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-grp2-silver/dim_restricted_zones"

    # SEPARATE checkpoint from the ingestor — this processor tracks which Bronze
    # files it has consumed, independently of Kafka offsets.
    checkpoint  = "s3://ttn-de-bootcamp-bronze-us-east-1/8834_Lakshaya_bronze/checkpoints/processor_v2/"

    pg_conf = {
        "url":  f"jdbc:postgresql://{args.pg_host}:{args.pg_port}/{args.pg_db}",
        "props": {"user": args.pg_user, "password": args.pg_pass, "driver": "org.postgresql.Driver"},
        "connect": {
            "host": args.pg_host, "database": args.pg_db,
            "user": args.pg_user, "password": args.pg_pass, "connect_timeout": 10
        }
    }

    # ── STATIC REFERENCE DATA  (broadcast — small tables) ────────
    # broadcast() hint tells Spark to replicate this small table to every executor,
    # avoiding an expensive shuffle-based join. Restricted zones is typically <100 rows.
    zones_df = F.broadcast(spark.read.format("delta").load(zones_path))

    # ── READ STREAM FROM BRONZE S3 ───────────────────────────────
    # maxFilesPerTrigger = 10: limits how many Bronze JSON files are consumed per
    # micro-batch. This prevents the processor from being overwhelmed if a large
    # backlog accumulated while it was offline. It also provides natural backpressure.
    # The processor will catch up gradually instead of OOM-ing on a massive batch.
    bronze_stream = (
        spark.readStream
        .format("delta")
        # startingVersion=0 ensures that on first startup the processor reads ALL
        # historical Bronze commits, not just new ones. Without this, if the processor
        # starts after the ingestor has already written several batches, it would skip
        # them. The checkpoint then takes over for subsequent runs.
        .option("startingVersion", "0")
        .option("maxFilesPerTrigger", 10)
        .load(bronze_path)
    )

    # ── FOREACH BATCH ─────────────────────────────────────────────
    def process_batch(batch_df, batch_id):

        if batch_df.rdd.isEmpty():
            return

        # Defensive cast: ingestor writes speed as DoubleType but old Bronze data
        # may have been written as IntegerType. Cast ensures consistent comparisons
        # in speed_flag (speed > 110) regardless of stored type.
        batch_df = batch_df.withColumn("speed", F.col("speed").cast("double"))

        # ── 1. VALIDATION ────────────────────────────────────────
        # Filter out rows with any null critical fields.
        # These could be malformed Kafka messages that the ingestor persisted as-is
        # (Bronze stores everything, validation happens here in the processor).
        df = batch_df.filter(
            # 1. Identity Constraints: Not null and not just empty spaces
            # We also TRIM the columns permanently to ensure clean joins with SCD2
            F.col("vin").isNotNull() & (F.length(F.trim(F.col("vin"))) > 0) &
            F.col("driver_id").isNotNull() & (F.length(F.trim(F.col("driver_id"))) > 0) &

            # 2. Physics Constraints: Speed cannot be negative or absurdly high
            # (Assuming commercial trucks/vans rarely exceed 160km/h, capped at 200 for safety)
            F.col("speed").isNotNull() & (F.col("speed") >= 0) & (F.col("speed") <= 200) &

            # 3. Spatial Constraints: Valid Earth coordinates
            F.col("lat").isNotNull()  & (F.col("lat") >= -90.0)  & (F.col("lat") <= 90.0) &
            F.col("long").isNotNull() & (F.col("long") >= -180.0) & (F.col("long") <= 180.0) &
            # Critical IoT Fix: Drop "Null Island" (0.0, 0.0) which is a common default for broken GPS
            ~((F.col("lat") == 0.0) & (F.col("long") == 0.0)) &
            F.col("event_timestamp").isNotNull()
        )
        if df.rdd.isEmpty():
            return

        # ── 2. ACTIVE ASSET VALIDATION (SCD2 inner join) ────────
        # PROBLEM: Without this, "ghost" records — vin/driver_id combos that
        # don't exist in the SCD2 table (typos, decommissioned vehicles, test data)
        # — would pollute the Silver table and inflate violation counts.
        # SOLUTION: Inner join against the active SCD2 snapshot. Only telemetry from
        # known, currently-active (IN-TRANSIT) vehicle-driver assignments passes through.
        # We also extract the base_rate here to avoid a second SCD2 read at Step 6.
        active_assets = (
            spark.read.format("delta").load(scd2_path)
            .filter(F.col("status") == "IN-TRANSIT")
            .groupBy("vin", "driver_id")
            .agg(F.max("rate").alias("base_rate"))
        )

        df = df.join(
            active_assets,
            on=["vin", "driver_id"],
            how="inner"     # DROP any vin/driver_id not found in active SCD2
        )
        if df.rdd.isEmpty():
            log.info(f"Batch {batch_id}: no records matched active assets. Skipping.")
            return

        # ── 3. VIOLATION DETECTION ───────────────────────────────
        # Speed threshold: 110 km/h
        df = df.withColumn("speed_flag", F.col("speed") > 110)

        # Geofence check: join telemetry coordinates against restricted zone bounding boxes.
        # "left" join ensures non-violating rows are preserved for the speed-only check.
        df = df.join(
            zones_df,
            (df.lat  >= zones_df.min_lat)  & (df.lat  <= zones_df.max_lat) &
            (df.long >= zones_df.min_long) & (df.long <= zones_df.max_long),
            "left"
        ).withColumn("zone_flag", F.col("zone_name").isNotNull())

        # Keep only rows that triggered at least one violation.
        violations = df.filter(F.col("speed_flag") | F.col("zone_flag"))
        if violations.rdd.isEmpty():
            return

        # ── 4. 2-MINUTE WINDOW DEDUP + MERGED VIOLATION TYPE ─────
        #
        # WHY 2 MINUTES INSTEAD OF 30 SECONDS?
        #   A 30-second window is too aggressive for real-world driving. A driver
        #   speeding through a geofence zone often generates a burst of events over
        #   60-90 seconds. A 2-minute window treats this as a single continuous
        #   infraction instead of 3-4 separate strikes, which is fairer and aligns
        #   with how fleet managers expect violations to be counted.
        #
        # THE DATA LOSS PROBLEM:
        #   If we simply take row_number()==1 ordered by speed_flag, a GEOFENCE
        #   event at t=0:00 and a SPEED event at t=1:30 within the same 2-min
        #   window would DROP the GEOFENCE event entirely. The driver committed
        #   BOTH violations, but only SPEED gets recorded.
        #
        # SOLUTION — MERGED FLAGS:
        #   1. Bucket events into 2-minute tumbling windows.
        #   2. Compute max(speed_flag) and max(zone_flag) across the ENTIRE window
        #      per (driver_id, vin). This captures whether EITHER flag was true
        #      at ANY point during the window, regardless of which row we keep.
        #   3. Assign violation_type from the merged flags:
        #      - Both flags true anywhere in window → SPEED_AND_GEOFENCE
        #      - Only speed flag true              → SPEED
        #      - Only zone flag true               → GEOFENCE
        #   4. Finally, dedup with row_number()==1 to keep one row per window,
        #      but the violation_type already reflects the full window's activity.

        # Step 4a: Bucket into 2-minute tumbling windows
        violations = violations.withColumn(
            "bucket_2m",
            F.window(F.col("event_timestamp"), "2 minutes").getField("start")
        )

        # Step 4b: Compute window-level merged flags
        bucket_w = Window.partitionBy("driver_id", "vin", "bucket_2m")
        violations = violations.withColumn(
            "window_speed_flag", F.max("speed_flag").over(bucket_w)
        ).withColumn(
            "window_zone_flag", F.max("zone_flag").over(bucket_w)
        )

        # Step 4c: Assign violation_type from MERGED flags (not per-row flags)
        violations = violations.withColumn(
            "violation_type",
            F.when(F.col("window_speed_flag") & F.col("window_zone_flag"), F.lit("SPEED_AND_GEOFENCE"))
             .when(F.col("window_speed_flag"), F.lit("SPEED"))
             .otherwise(F.lit("GEOFENCE"))
        )

        # Step 4d: Dedup — keep one row per (driver_id, vin, 2-min bucket)
        dedup_w = Window.partitionBy("driver_id", "vin", "bucket_2m").orderBy(
            F.col("speed_flag").desc(), "event_timestamp"
        )
        violations = (
            violations
            .withColumn("rn", F.row_number().over(dedup_w))
            .filter(F.col("rn") == 1)
            .drop("rn", "bucket_2m", "window_speed_flag", "window_zone_flag")
        )

        # ── 5. SILVER WRITE  (fact_safety_violations) ────────────
        # ingestion_date = today's UTC date (NOT event_timestamp).
        # Job 6 (batch safety summary) filters on ingestion_date so it processes
        # exactly one day's worth of streaming output per batch run.
        silver_df = violations.select(
            "driver_id",
            "vin",
            "event_timestamp",
            F.date_format("event_timestamp", "yyyy-MM").alias("month"),
            "violation_type",
            F.lit(1).cast("int").alias("strike_count"),
            F.to_date(F.current_timestamp()).cast("string").alias("ingestion_date")
        )
        # NO coalesce(1) — let Spark write with natural parallelism.
        # Small files are handled by OPTIMIZE compaction below.

        silver_df.write.format("delta").mode("append") \
            .partitionBy("ingestion_date") \
            .save(silver_path)
        log.info(f"Batch {batch_id}: violation events written to Silver.")

        # Periodic compaction on Silver: OPTIMIZE merges many small files into fewer
        # large files. This dramatically improves read performance for Job 6 (batch),
        # which scans the entire ingestion_date partition.
        # Every 20 batches ≈ every 40 minutes at a 2-minute trigger interval.
        if batch_id % 20 == 0 and batch_id > 0:
            log.info(f"Running OPTIMIZE on Silver (Batch {batch_id})...")
            spark.sql(f"OPTIMIZE delta.`{silver_path}`")

        # ── 6. AGGREGATE PER (driver_id, month) FOR GOLD ─────────
        # Sum strikes per driver per month — this is the granularity of the Gold table.
        batch_agg = silver_df.groupBy("driver_id", "month").agg(
            F.sum("strike_count").alias("batch_strikes")
        )

        # No need to read SCD2 again — active_assets already has (vin, driver_id, rate)
              
        rates_df = (   #iska kaam hai sirf join krke base rate provide krna 
            active_assets
            .groupBy("driver_id")
            .agg(F.max("base_rate").alias("base_rate"))
        )

        updates = (
            batch_agg.join(rates_df, "driver_id", "inner") 
            .withColumn("batch_id",  F.lit(batch_id))
            .dropDuplicates(["driver_id", "month"])  # merge source must be unique on PK
        )   

        # ── 7. GOLD DELTA — IDEMPOTENT INIT + MERGE ──────────────
        upsert_gold_delta(spark, updates, gold_path, batch_id)

        # Periodic compaction on Gold: same reasoning as Silver OPTIMIZE.
        # Gold table is smaller but read by the Postgres sync step, so keeping
        # file count low reduces Spark scan time for the join below.
        if batch_id % 20 == 0 and batch_id > 0:
            log.info(f"Running OPTIMIZE on Gold (Batch {batch_id})...")
            spark.sql(f"OPTIMIZE delta.`{gold_path}`")

        # ── 8. POSTGRES SYNC  (push only rows updated this batch) ─
        # Instead of syncing the entire Gold table, we only push the rows that were
        # affected by this batch. This minimizes Postgres write load and network transfer.
        changed_keys = updates.select("driver_id", "month").distinct()
        changed_df   = (
            spark.read.format("delta").load(gold_path)
            .join(changed_keys, ["driver_id", "month"], "inner")
        )

        upsert_postgres(changed_df, pg_conf)
        log.info(f"Batch {batch_id} processed successfully.")

    # ── START STREAM ──────────────────────────────────────────────
    query = (
        bronze_stream.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=args.trigger)
        .start()
    )

    log.info("Processor started — consuming Bronze S3 → Silver → Gold → Postgres")
    query.awaitTermination()


if __name__ == "__main__":
    main()
