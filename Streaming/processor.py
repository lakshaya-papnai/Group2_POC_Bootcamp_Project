"""
Streaming Job 2: Safety Processor (Bronze S3 → Silver → Gold → Postgres)
=====================================================================================

PURPOSE:
  This job reads raw telemetry from the Bronze S3 path (written by ingestor.py),
  applies all business logic (geofence joins, violation detection, deduplication),
  and writes results to Silver Delta, Gold Delta, and PostgreSQL.
  
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("omniroute-processor")

def upsert_postgres(df, pg_conf):
    if df.rdd.isEmpty():
        return

    if "last_batch_id" in df.columns:
        df = df.drop("last_batch_id")

    df = df.dropDuplicates(["driver_id", "month"])

    jdbc_url   = pg_conf["url"]
    jdbc_props = pg_conf["props"]
    stg_table  = "gold.driver_safety_status_stg"
    tgt_table  = "gold.driver_safety_status"

    conn, cur = None, None
    try:
        conn = psycopg2.connect(**pg_conf["connect"])
        cur  = conn.cursor()
        cur.execute(f"TRUNCATE TABLE {stg_table};")
        conn.commit()
    finally:
        if cur:  cur.close()
        if conn: conn.close()

    df.write.mode("append").jdbc(url=jdbc_url, table=stg_table, properties=jdbc_props)
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

    bronze_path = "s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-grp2-bronze/raw/telemetry_stream"
    silver_path = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-grp2-silver/fact_safety_violations"
    gold_path   = "s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-grp2-gold/driver_safety_status"
    scd2_path   = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-grp2-silver/dim_asset_history_scd2"
    zones_path  = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-grp2-silver/dim_restricted_zones"
    checkpoint  = "s3://ttn-de-bootcamp-bronze-us-east-1/8834_Lakshaya_bronze/checkpoints/processor_v2/"

    pg_conf = {
        "url":  f"jdbc:postgresql://{args.pg_host}:{args.pg_port}/{args.pg_db}",
        "props": {"user": args.pg_user, "password": args.pg_pass, "driver": "org.postgresql.Driver"},
        "connect": {
            "host": args.pg_host, "database": args.pg_db,
            "user": args.pg_user, "password": args.pg_pass, "connect_timeout": 10
        }
    }
    zones_df = F.broadcast(spark.read.format("delta").load(zones_path))
    bronze_stream = (
        spark.readStream
        .format("delta")        
        .option("startingVersion", "0")
        .option("maxFilesPerTrigger", 10)
        .load(bronze_path)
    )
 
    def process_batch(batch_df, batch_id):

        if batch_df.rdd.isEmpty():
            return
          
        batch_df = batch_df.withColumn("speed", F.col("speed").cast("double"))

        df = batch_df.filter(
            F.col("vin").isNotNull() & (F.length(F.trim(F.col("vin"))) > 0) &
            F.col("driver_id").isNotNull() & (F.length(F.trim(F.col("driver_id"))) > 0) &
            F.col("speed").isNotNull() & (F.col("speed") >= 0) & (F.col("speed") <= 200) &
            F.col("lat").isNotNull()  & (F.col("lat") >= -90.0)  & (F.col("lat") <= 90.0) &
            F.col("long").isNotNull() & (F.col("long") >= -180.0) & (F.col("long") <= 180.0) &   
            ~((F.col("lat") == 0.0) & (F.col("long") == 0.0)) &
            F.col("event_timestamp").isNotNull()
        )
        if df.rdd.isEmpty():
            return

        active_assets = (
            spark.read.format("delta").load(scd2_path)
            .filter(F.col("status") == "IN-TRANSIT")
            .groupBy("vin", "driver_id")
            .agg(F.max("rate").alias("base_rate"))
        )

        df = df.join(
            active_assets,
            on=["vin", "driver_id"],
            how="inner"    
        )
        if df.rdd.isEmpty():
            log.info(f"Batch {batch_id}: no records matched active assets. Skipping.")
            return

        # VIOLATION DETECTION
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

        #  Bucket into 2-minute tumbling windows
        violations = violations.withColumn(
            "bucket_2m",
            F.window(F.col("event_timestamp"), "2 minutes").getField("start")
        )

        bucket_w = Window.partitionBy("driver_id", "vin", "bucket_2m")
        violations = violations.withColumn(
            "window_speed_flag", F.max("speed_flag").over(bucket_w)
        ).withColumn(
            "window_zone_flag", F.max("zone_flag").over(bucket_w)
        )

        violations = violations.withColumn(
            "violation_type",
            F.when(F.col("window_speed_flag") & F.col("window_zone_flag"), F.lit("SPEED_AND_GEOFENCE"))
             .when(F.col("window_speed_flag"), F.lit("SPEED"))
             .otherwise(F.lit("GEOFENCE"))
        )

        #  Dedup — keep one row per (driver_id, vin, 2-min bucket)
        dedup_w = Window.partitionBy("driver_id", "vin", "bucket_2m").orderBy(
            F.col("speed_flag").desc(), "event_timestamp"
        )
        violations = (
            violations
            .withColumn("rn", F.row_number().over(dedup_w))
            .filter(F.col("rn") == 1)
            .drop("rn", "bucket_2m", "window_speed_flag", "window_zone_flag")
        )

        #SILVER WRITE  (fact_safety_violations)
      
        silver_df = violations.select(
            "driver_id",
            "vin",
            "event_timestamp",
            F.date_format("event_timestamp", "yyyy-MM").alias("month"),
            "violation_type",
            F.lit(1).cast("int").alias("strike_count"),
            F.to_date(F.current_timestamp()).cast("string").alias("ingestion_date")
        )

        silver_df.write.format("delta").mode("append") \
            .partitionBy("ingestion_date") \
            .save(silver_path)
        log.info(f"Batch {batch_id}: violation events written to Silver.")

      if batch_id % 20 == 0 and batch_id > 0:
            log.info(f"Running OPTIMIZE on Silver (Batch {batch_id})...")
            spark.sql(f"OPTIMIZE delta.`{silver_path}`")

        # AGGREGATE PER (driver_id, month) FOR GOLD       
        batch_agg = silver_df.groupBy("driver_id", "month").agg(
            F.sum("strike_count").alias("batch_strikes")
        )
             
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

         upsert_gold_delta(spark, updates, gold_path, batch_id)

        if batch_id % 20 == 0 and batch_id > 0:
            log.info(f"Running OPTIMIZE on Gold (Batch {batch_id})...")
            spark.sql(f"OPTIMIZE delta.`{gold_path}`")

        changed_keys = updates.select("driver_id", "month").distinct()
        changed_df   = (
            spark.read.format("delta").load(gold_path)
            .join(changed_keys, ["driver_id", "month"], "inner")
        )

        upsert_postgres(changed_df, pg_conf)
        log.info(f"Batch {batch_id} processed successfully.")

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
