import argparse
import sys
import boto3
import psycopg2
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, row_number,
    desc, from_unixtime, when, trim, length, lead, coalesce, year
)
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# -------------------------------
# PARAMETERS
# -------------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--ingestion_date", required=True)
parser.add_argument("--db_host",     required=True)
parser.add_argument("--db_name",     required=True)
parser.add_argument("--db_user",     required=True)
parser.add_argument("--db_password", required=True)
parser.add_argument("--db_port",     default="5432")
args, _ = parser.parse_known_args()

INGESTION_DATE = args.ingestion_date

print(f"Starting VIN-based SCD2 job for {INGESTION_DATE}")

# -------------------------------
# SPARK SESSION
# -------------------------------
spark = SparkSession.builder \
    .appName("job_asset_scd2_engine") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------
# S3 CONFIG
# -------------------------------
BUCKET = "ttn-de-bootcamp-bronze-us-east-1"
BASE   = "poc-bootcamp-grp2-bronze"

RAW_PREFIX     = f"{BASE}/raw/vehicle_assignment/"
PROCESSED_BASE = f"s3://{BUCKET}/{BASE}/processed/vehicle_assignment"

SILVER_PATH = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-grp2-silver/dim_asset_history_scd2"
DIM_VEHICLE_PATH = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-grp2-silver/dim_vehicle"

s3 = boto3.client("s3")

# -------------------------------
# POSTGRES CONFIG (from Glue Job params)
# -------------------------------
JDBC_URL = f"jdbc:postgresql://{args.db_host}:{args.db_port}/{args.db_name}"

DB_PROPERTIES = {
    "user":     args.db_user,
    "password": args.db_password,
    "driver":   "org.postgresql.Driver"
}

# -------------------------------
# RAW SCHEMA 
# -------------------------------
schema = StructType([
    StructField("vin",             StringType(), True),
    StructField("driver_id",       StringType(), True),
    StructField("start_timestamp", StringType(), True),
    StructField("end_timestamp",   StringType(), True),  
    StructField("daily_rate",      DoubleType(), True),
    StructField("region",          StringType(), True)
])

# -------------------------------
# HELPER function: parse a unix-ts string column → date format
# Handles both seconds (10-digit) and milliseconds (13-digit)
# -------------------------------
def unix_to_date(col_name):
    ts_long = col(col_name).cast("long")
    return from_unixtime(
        when(ts_long > 1_000_000_000_000, ts_long / 1000).otherwise(ts_long)
    ).cast("date")

# -------------------------------
# S3 LISTING (Pagination removed )
# -------------------------------
def list_raw_files():
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=RAW_PREFIX)

    files = []
    if "Contents" in response:
        for obj in response["Contents"]:
            key = obj["Key"]
            if key.lower().endswith(".csv"):
                files.append(key)

    print(f"Detected files: {files}")
    return files

# -------------------------------
# SORT logic: base file first, then incrementals in numeric order
# -------------------------------
def sort_files(files):
    def extract_order(f):
        fname = f.split("/")[-1].lower()
        if fname == "vehicle_assignment.csv":
            return -1
        match = re.search(r"vehicle_assignment_(\d+)\.csv", fname)
        return int(match.group(1)) if match else 9999

    sorted_files = sorted(files, key=extract_order)
    print(f"Sorted order: {sorted_files}")
    return sorted_files

# -------------------------------
# REAL-TIME VALIDATION: exclude SUSPENDED drivers 
# -------------------------------
def filter_suspended_drivers(df):
    try:
        suspended_df = (
            spark.read.jdbc(url=JDBC_URL, table="gold.driver_safety_status",
                            properties=DB_PROPERTIES)
            .filter(col("status") == "SUSPENDED")
            .select("driver_id")
        )
        result = df.join(suspended_df, on="driver_id", how="left_anti")
        print("Suspended-driver filter applied from Postgres.")
        return result
    except Exception as e:
        print(f"Skipping Postgres validation (will retry next run): {e}")
        return df

# -----------------------------------------------------------------
# INGEST RAW → PROCESSED
# Returns a cleaned DataFrame with columns:
#   vin, driver_id, start_date, end_date (nullable), rate,
#   has_source_end_date (bool), ingestion_timestamp
# -----------------------------------------------------------------
def ingest_file(key):
    path      = f"s3://{BUCKET}/{key}"
    file_name = key.split("/")[-1]
    print(f"Processing file: {file_name}")

    raw_df    = spark.read.option("header", True).schema(schema).csv(path)

    # ── DATA CLEANING ──────────────────────────────────────────────
    df = raw_df.filter(
        (col("vin").isNotNull())         & (length(trim(col("vin")))       > 0) &
        (col("driver_id").isNotNull())   & (length(trim(col("driver_id"))) > 0) &
        (col("start_timestamp").isNotNull())                                    &
        (col("daily_rate").isNotNull())  & (col("daily_rate") > 0)
    )

    # ── VALIDATE VIN AGAINST DIM_VEHICLE ───────────────────────────
    if DeltaTable.isDeltaTable(spark, DIM_VEHICLE_PATH):
        dim_vehicle_df = spark.read.format("delta").load(DIM_VEHICLE_PATH).select("vin").distinct()
        df = df.join(dim_vehicle_df, on="vin", how="inner")
        print("Applied VIN validation from dim_vehicle.")
    else:
        print("⚠️ dim_vehicle not found. Skipping VIN validation.")

    clean_count = df.count()
    print(f"Rows after cleaning & validation:  {clean_count}")

    if clean_count == 0:
        print(f"Skipped empty/invalid file: {file_name}")
        return None

    # ── PARSE unix TIMESTAMPS to date ────────────────────────────────────
    df = (
        df
        .withColumn("start_date", unix_to_date("start_timestamp"))
        .withColumn(
            "end_date",
            when(
                col("end_timestamp").isNotNull() & (length(trim(col("end_timestamp"))) > 0),
                unix_to_date("end_timestamp")
            ).otherwise(lit(None).cast("date"))
        )
        # Flag: does the SOURCE data provide an explicit end date for this record?
        .withColumn("has_source_end_date", col("end_date").isNotNull())
        .withColumn("rate", col("daily_rate"))
        .withColumn("ingestion_timestamp", current_timestamp())
        .drop("start_timestamp", "end_timestamp", "daily_rate")
        #dropping coz we only need dates not timestamps , in final processed table
    )

    # Drop rows where start_date couldn't be parsed
    # because string dates like abcd gets converted to null by unix_to_date function.
    # RANGE CHECK: Filter out invalid dates (e.g., Year 9000 problem or < 1950)
    df = df.filter(
        col("start_date").isNotNull() &
        (year(col("start_date")) >= 1950) &
        (year(col("start_date")) <= 2050) &
        (
            col("end_date").isNull() |
            ((year(col("end_date")) >= 1950) & (year(col("end_date")) <= 2050))
        )
    )

    # ── CONFLICT RESOLUTION : highest rate wins if duplicate (vin,start_date) is found ──
    # meaning:- ek vehicle pe 2 assignments same date pe, to zyada rate wali assignment rakho
    
    window_dedup = Window.partitionBy("vin", "start_date").orderBy(desc("rate"))
    df = (
        df
        .withColumn("rn", row_number().over(window_dedup))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    df = df.select(
        "vin", "driver_id", "start_date", "end_date",
        "has_source_end_date", "rate", "region", "ingestion_timestamp"
    )

    output_path = f"{PROCESSED_BASE}/ingestion_date={INGESTION_DATE}/{file_name}"
    df.write.mode("overwrite").option("header", True).csv(output_path)
 
    return df

# -----------------------------------------------------------------
# INITIALIZE SCD2 TABLE  (called only when scd2_table does not yet exist)
#   1. Order by start_date per VIN
#   2. Fill end_date for ARCHIVED rows using LEAD(start_date) if the
#      source didn't supply one
#   3. Only the LATEST row per VIN (no source end_date) → IN-TRANSIT
#   4. All others → ARCHIVED
# -----------------------------------------------------------------
def initialize_scd2(df):
    df = filter_suspended_drivers(df)

    if df.rdd.isEmpty():
        print("All rows filtered out (suspended drivers). Skipping init.")
        return

    # Window ordered chronologically per VIN
    w_vin_asc  = Window.partitionBy("vin").orderBy("start_date") # used for closing records having no end_date but we know they are closed because new(another) start date records are there for same vin
    w_vin_desc = Window.partitionBy("vin").orderBy(desc("start_date")) # used to find latest record (mark it as "in-transit" if no end_date)

    df = (
        df
        .withColumn(
            "derived_end_date",
            when(   # Determine end_date: prefer what the source gave us,
                col("has_source_end_date"),
                col("end_date")          # explicit from source
            ).otherwise( # otherwise: if not available, derive from the start_date of the NEXT record for this VIN.
                lead("start_date").over(w_vin_asc)   # implicit from next row
            )
        )
        .withColumn("rn_latest", row_number().over(w_vin_desc)) #  latest row per VIN with no end_date → IN-TRANSIT , everything else → ARCHIVED
        .withColumn(
            "status",
            when(
                (col("rn_latest") == 1) & col("derived_end_date").isNull(),
                lit("IN-TRANSIT")
            ).otherwise(lit("ARCHIVED"))
        )
        .drop("rn_latest", "end_date", "has_source_end_date")
        .withColumnRenamed("derived_end_date", "end_date")
        .select("vin", "driver_id", "rate", "start_date", "end_date", "status", "region", "ingestion_timestamp")
    )

    df.write.format("delta").mode("overwrite").save(SILVER_PATH)
    print(f"SCD2 table initialized.")

# -----------------------------------------------------------------
# APPLY SCD2  (called for every subsequent / incremental file)
#
# Incremental files contain NEW assignments only.
# "Continuity": when a new assignment arrives for a VIN,
#   • close (ARCHIVE) the current IN-TRANSIT row (end_date = new start_date)
#   • insert the new row as IN-TRANSIT

# KEY FIX: We must ensure ONE source row per VIN in the merge.
#   If the incremental file somehow has multiple dates for the same VIN,
#   keep only the LATEST date (highest rate tiebreak) — that is the
#   assignment we are activating right now.
# -----------------------------------------------------------------
def apply_scd2(df):
    df = filter_suspended_drivers(df)

    if df.rdd.isEmpty():
        return

    # ── GUARANTEE: exactly ONE row per VIN going into the merge ───
    # (BRD conflict resolution: highest rate on the latest date wins)
    window_one_per_vin = Window.partitionBy("vin").orderBy(desc("start_date"), desc("rate"))
    df = (
        df
        .withColumn("rn", row_number().over(window_one_per_vin))
        .filter(col("rn") == 1)
        .drop("rn", "end_date", "has_source_end_date")
        .withColumn("end_date", lit(None).cast("date"))
        .withColumn("status", lit("IN-TRANSIT"))
    )

    delta_table = DeltaTable.forPath(spark, SILVER_PATH)

    # ── STANDARD SCD2 MERGE PATTERN ───────────────────────────────
    # staging_df has two rows per changed VIN:
    #   Row A  merge_key = vin   → matches existing IN-TRANSIT → ARCHIVE it
    #   Row B  merge_key = NULL  → no match          → INSERT new IN-TRANSIT row
    
    target_df  = delta_table.toDF()

    changes_df = (
        df.alias("s")
        .join(
            target_df.filter(col("status") == "IN-TRANSIT").alias("t"),
            col("s.vin") == col("t.vin"),
            "inner"
        )
        .where(
            (col("s.driver_id") != col("t.driver_id")) |
            (col("s.rate")      != col("t.rate"))
        )
        .selectExpr(
            "NULL       as merge_key",
            "s.vin", "s.driver_id", "s.rate",
            "s.start_date", "s.end_date", "s.status",
            "s.region", "s.ingestion_timestamp"
        )
    )

    base_df = df.selectExpr(
        "vin        as merge_key",
        "vin", "driver_id", "rate",
        "start_date", "end_date", "status",
        "region", "ingestion_timestamp"
    )

    staging_df = base_df.unionByName(changes_df)

    (
        delta_table.alias("t")
        .merge(
            staging_df.alias("s"),
            "t.vin = s.merge_key"
        )
        # When the existing IN-TRANSIT row sees a change → ARCHIVE it
        .whenMatchedUpdate(
            condition="""
                t.status = 'IN-TRANSIT' AND
                (t.driver_id != s.driver_id OR t.rate != s.rate)
            """,
            set={
                "status":   "'ARCHIVED'",
                "end_date": "s.start_date"
            }
        )
        # Insert the new IN-TRANSIT row (only for changed VINs via merge_key = NULL,
        # and brand-new VINs that never existed in the table)
        .whenNotMatchedInsert(
            values={
                "vin":                 "s.vin",
                "driver_id":           "s.driver_id",
                "rate":                "s.rate",
                "start_date":          "s.start_date",
                "end_date":            "s.end_date",
                "status":              "s.status",
                "region":              "s.region",
                "ingestion_timestamp": "s.ingestion_timestamp"
            }
        )
        .execute()
    )

# -------------------------------
# GOLD POSTGRES WRITE
# PK conflict (vin, start_date) updates all non-key columns.
# -------------------------------
def write_to_postgres_gold():
    print("Writing SCD2 history to gold.asset_history_scd2 (Postgres)...")

    gold_df = (
        spark.read.format("delta").load(SILVER_PATH)
        .select(
            "vin", "driver_id",
            col("start_date").cast("date"),
            col("end_date").cast("date"),
            col("rate").alias("daily_rate"),
            "status", "region", "ingestion_timestamp"
        )
    )

    # ── Stage into temp table ────────────────────────────────────────────
    gold_df.write.jdbc(
        url=JDBC_URL,
        table="gold.asset_history_scd2_stg",
        mode="overwrite",
        properties={**DB_PROPERTIES, "truncate": "true"}
    )
    print(f" Staged rows into asset_history_scd2_stg")

    # ── UPSERT: stg → main (PK: vin, start_date) ────────────────────────
    upsert_sql = """
        INSERT INTO gold.asset_history_scd2
            (vin, driver_id, start_date, end_date, daily_rate,
             status, region, ingestion_timestamp)
        SELECT
            vin, driver_id, start_date, end_date, daily_rate,
            status, region, ingestion_timestamp
        FROM gold.asset_history_scd2_stg
        ON CONFLICT (vin, start_date) DO UPDATE SET
            driver_id           = EXCLUDED.driver_id,
            end_date            = EXCLUDED.end_date,
            daily_rate          = EXCLUDED.daily_rate,
            status              = EXCLUDED.status,
            region              = EXCLUDED.region,
            ingestion_timestamp = EXCLUDED.ingestion_timestamp;
        TRUNCATE gold.asset_history_scd2_stg;
    """

    conn = psycopg2.connect(
        host=args.db_host, database=args.db_name,
        user=args.db_user, password=args.db_password,
        connect_timeout=15
    )
    try:
        cur = conn.cursor()
        cur.execute(upsert_sql)
        conn.commit()
        print(f"gold.asset_history_scd2 upserted: {cur.rowcount} rows affected")
        cur.close()
    except Exception as e:
        conn.rollback()
        print(f"Postgres gold write failed: {e}")
        raise
    finally:
        conn.close()


# -------------------------------
# MAIN
# -------------------------------
if __name__ == "__main__":
    try:
        files = list_raw_files()
        if not files:
            print("No files to process")
            sys.exit(0)

        files = sort_files(files)
        table_exists = DeltaTable.isDeltaTable(spark, SILVER_PATH)

        for file in files:
            df = ingest_file(file)

            if df is None:
                print(f"emoving corrupt/empty file: {file}")
                s3.delete_object(Bucket=BUCKET, Key=file)
                continue

            if not table_exists:
                initialize_scd2(df)
                table_exists = True
            else:
                apply_scd2(df)

            # Atomic S3 delete: only after the merge succeeds
            print(f"uccess! Deleting from raw: {file}")
            s3.delete_object(Bucket=BUCKET, Key=file)

       
        # Gold Postgres write (runs once after all files are processed)
        write_to_postgres_gold()


    except Exception as e:
        print(f"FAILED: {e}")
        raise