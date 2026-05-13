# OmniRoute: Enterprise Logistics Data Platform

![Architecture](https://img.shields.io/badge/Architecture-Medallion%20|%20Lambda-blueviolet?style=for-the-badge)
![AWS](https://img.shields.io/badge/AWS-S3%20|%20Glue%20|%20EMR-orange?style=for-the-badge&logo=amazon-aws)
![Spark](https://img.shields.io/badge/Apache%20Spark-Structured%20Streaming-E25A1C?style=for-the-badge&logo=apache-spark)
![Airflow](https://img.shields.io/badge/Orchestration-Apache%20Airflow-017CEE?style=for-the-badge&logo=apache-airflow)

**OmniRoute** is a production-grade logistics analytics platform that processes massive telemetry streams alongside batch organizational data through a **Medallion Architecture** (Bronze → Silver → Gold). It delivers real-time driver safety monitoring, automated fuel efficiency audits, fleet history tracking via SCD Type 2, and executive dashboards via Power BI.

---

## Table of Contents

1. [System Architecture](#system-architecture)
2. [Infrastructure & Tech Stack](#infrastructure--tech-stack)
3. [Data Sources & Schemas](#data-sources--schemas)
4. [Medallion Data Architecture](#medallion-data-architecture)
5. [Batch Pipeline — AWS Glue Jobs (Job 1–8)](#batch-pipeline--aws-glue-jobs)
6. [Real-Time Streaming Pipeline](#real-time-streaming-pipeline)
7. [Airflow Orchestration](#airflow-orchestration)
8. [PostgreSQL Gold Schema](#postgresql-gold-schema)
9. [Business Logic Deep Dive](#business-logic-deep-dive)
10. [Power BI Dashboards](#power-bi-dashboards)
11. [Project Structure](#project-structure)

---

## System Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                     │
│  ┌─────────────┐  ┌──────────────────┐  ┌────────────────────────────┐  │
│  │ CSV Files   │  │ JSON Files       │  │ Kafka (EC2 Broker)         │  │
│  │ • registry  │  │ • zones          │  │ • telemetry_stream topic   │  │
│  │ • assign    │  │ • telemetry      │  │ • 1-sec GPS/speed events   │  │
│  │ • fuel      │  │                  │  │                            │  │
│  │ • maint     │  │                  │  │                            │  │
│  └──────┬──────┘  └────────┬─────────┘  └─────────────┬──────────────┘  │
│         │                  │                           │                 │
│         ▼                  ▼                           ▼                 │
│  ┌─────────────────────────────────┐    ┌──────────────────────────────┐ │
│  │    BRONZE LAYER (S3)            │    │   BRONZE LAYER (S3)          │ │
│  │    Raw → Processed (CSV)        │    │   ingestor.py → Delta       │ │
│  │    Schema enforcement           │    │   Zero business logic       │ │
│  │    Data quality firewall        │    │   Fault-isolated from proc  │ │
│  └──────────────┬──────────────────┘    └────────────┬─────────────────┘ │
│                 │                                     │                  │
│         AWS Glue Jobs (Batch)              processor.py (Streaming)      │
│                 │                                     │                  │
│                 ▼                                     ▼                  │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │                    SILVER LAYER (S3 Delta Lake)                     │ │
│  │  dim_vehicle │ dim_date │ dim_zones │ dim_maintenance               │ │
│  │  dim_asset_history_scd2 │ fact_fuel_transactions                    │ │
│  │  fact_safety_violations (streaming)                                 │ │
│  └──────────────────────────┬──────────────────────────────────────────┘ │
│                             │                                            │
│                             ▼                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │                     GOLD LAYER (S3 Delta + PostgreSQL)              │ │
│  │  fuel_efficiency_audit │ active_fleet_snapshot                       │ │
│  │  safety_compliance_summary │ driver_safety_status                    │ │
│  │  driver_rate_deduction_report.txt (S3)                              │ │
│  └──────────────────────────┬──────────────────────────────────────────┘ │
│                             │                                            │
│                             ▼                                            │
│               ┌──────────────────────────┐                               │
│               │     POWER BI DASHBOARDS  │                               │
│               │  Connected via PostgreSQL │                               │
│               └──────────────────────────┘                               │
└──────────────────────────────────────────────────────────────────────────┘
```

**Orchestration:** 3 Apache Airflow DAGs (Daily, Monthly, Yearly) with S3 sensors, cross-DAG dependencies, and fault-tolerant branching.

---

## Infrastructure & Tech Stack

| Component | Technology | Purpose |
|:--|:--|:--|
| **Object Storage** | Amazon S3 (3 buckets: bronze, silver, gold) | Medallion layer storage |
| **Batch Processing** | AWS Glue (PySpark) | 8 ETL jobs across all layers |
| **Streaming Ingestion** | Spark Structured Streaming on EMR | Kafka → Bronze → Silver → Gold |
| **Message Broker** | Apache Kafka (self-hosted EC2) | 1-second telemetry ingestion |
| **Table Format** | Delta Lake | ACID transactions, time travel, MERGE |
| **Orchestration** | Apache Airflow (EC2) | DAG scheduling, sensors, branching |
| **Reporting DB** | PostgreSQL (EC2) | Gold layer for Power BI |
| **Dashboards** | Microsoft Power BI | Executive & operational reporting |

**Why Delta Lake over raw Parquet?** Delta provides ACID transactions (no partial writes), schema enforcement, and the MERGE command required for SCD2 updates. Without Delta, the SCD2 engine in Job 2 and the streaming processor would need complex custom logic to handle concurrent reads/writes.

**Why PostgreSQL instead of querying S3 directly?** Power BI performs best with a relational backend for live dashboards. S3 + Delta is optimized for batch analytics, not low-latency BI queries. PostgreSQL acts as a serving layer that is refreshed via idempotent upserts after every batch/streaming run.

---

## Data Sources & Schemas

### 1. Vehicle Registry (`vehicle_registry.csv`)
| Column | Type | Description |
|:--|:--|:--|
| vin | String | Vehicle Identification Number (PK) |
| model | String | e.g., "Scania R-Series" |
| mfg_year | Integer | Manufacturing year (validated: 1980–2026) |
| fuel_type | String | Diesel, LNG, CNG, etc. |
| baseline_kmpl | Double | Manufacturer-rated km/liter |

### 2. Vehicle Assignment (`vehicle_assignment.csv`, `vehicle_assignment_1.csv`, ...)
| Column | Type | Description |
|:--|:--|:--|
| vin | String | FK → vehicle_registry |
| driver_id | String | e.g., "DRV_05112" |
| start_timestamp | String | Unix epoch (seconds or milliseconds) |
| end_timestamp | String | Unix epoch, nullable (null = ongoing) |
| daily_rate | Double | Driver's daily pay rate |
| region | String | Operating region (West, South, etc.) |

**Why Unix timestamps?** Upstream dispatch systems emit both seconds (10-digit) and milliseconds (13-digit). The `unix_to_date()` helper normalizes both formats automatically.

### 3. Fuel Transactions (`fuel_transactions.csv`) — ~87 MB
| Column | Type | Description |
|:--|:--|:--|
| transaction_id | String | Unique receipt ID |
| vin | String | FK → vehicle_registry |
| fuel_liters | Double | Fuel volume (must be > 0) |
| odometer_reading | Double | Vehicle odometer at fill-up |
| timestamp | String | Human-readable datetime |

### 4. Restricted Zones (`restricted_zones.json`)
| Field | Type | Description |
|:--|:--|:--|
| zone_name | String | e.g., "High_Risk_Pass_A" |
| min_lat / max_lat | Float | Latitude bounding box (-90 to 90) |
| min_long / max_long | Float | Longitude bounding box (-180 to 180) |

### 5. Maintenance Logs (`maintenance_logs.csv`)
| Column | Type | Description |
|:--|:--|:--|
| vin | String | FK → vehicle_registry |
| service_date | String | Date of scheduled service |
| service_type | String | e.g., "Oil Change", "Alternator Service" |

### 6. Telemetry Stream (`telemetry_messages.json` → Kafka)
| Field | Type | Description |
|:--|:--|:--|
| vin | String | Vehicle sending telemetry |
| driver_id | String | Driver currently operating |
| speed | Double | Current speed in km/h |
| lat / long | Double | GPS coordinates |
| event_timestamp | String | ISO-8601 UTC timestamp |

---

## Medallion Data Architecture

### Bronze Layer (Raw & Processed)
The ingestion zone. Raw files land in `s3://bronze/raw/` and pass through a **Data Quality Firewall** that enforces:
- **Not-null checks** on all critical columns
- **Whitespace trimming** (empty strings like `"   "` are rejected)
- **Range validation** (mfg_year 1980–2026, fuel_liters > 0, lat -90 to 90)
- **Explicit schema casting** (no `inferSchema` — avoids double-read performance penalty)

Cleaned data is written to `s3://bronze/processed/` partitioned by `ingestion_date`.

**Raw file cleanup:** After successful processing, the raw file is deleted from S3 to prevent double-processing on reruns.

### Silver Layer (Conformed Dimensions & Facts)
The enterprise truth layer stored as **Delta Lake** tables:

| Table | Type | Source | Key Logic |
|:--|:--|:--|:--|
| `dim_vehicle` | Dimension | vehicle_registry.csv | Deduped by VIN (newest mfg_year wins) |
| `dim_date` | Dimension | Generated via SQL | 2020-01-01 to 2035-12-31, includes `is_weekend` |
| `dim_restricted_zones` | Dimension | restricted_zones.json | Geometric bounds validated |
| `dim_maintenance_schedule` | Dimension | maintenance_logs.csv | Yearly load, deduped by (vin, date) |
| `dim_asset_history_scd2` | SCD Type 2 | vehicle_assignment*.csv | Full driver-vehicle timeline with ARCHIVED/IN-TRANSIT |
| `fact_fuel_transactions` | Fact | fuel_transactions.csv | Deduped by transaction_id |
| `fact_safety_violations` | Fact | Streaming processor | Speeding & geofence violations |

### Gold Layer (Business Aggregations)
Refined datasets for BI dashboards, written to both **S3 Delta** and **PostgreSQL**:

| Table | Source Jobs | Business Purpose |
|:--|:--|:--|
| `fuel_efficiency_audit` | Job 4 | KMPL calculation, FLAGGED/OK status per vehicle |
| `active_fleet_snapshot` | Job 5 | Daily count of IN-TRANSIT vehicles by model |
| `safety_compliance_summary` | Job 6 | Daily violation KPIs + Top 10 worst drivers (JSONB) |
| `driver_safety_status` | Streaming + Job 8 | Monthly strike tracking, ACTIVE/SUSPENDED status |

---

## Batch Pipeline — AWS Glue Jobs

### Job 1: `job1_dim_core_load.py` — Dimension Bootstrap
**Layer:** Bronze → Silver | **Schedule:** Daily (DAG 1, 00:00 UTC)

**What it does:** Loads three foundational dimension tables that every other job depends on.

**Step-by-step flow:**
1. **Ingest Vehicle Registry:** Reads `vehicle_registry.csv` from raw S3 with an explicit schema (no `inferSchema` — avoids a double-read). Applies strict data quality filters (VIN not empty, mfg_year 1980–2026, baseline_kmpl > 0). Writes clean data to `processed/` partitioned by `ingestion_date`. Deletes the raw file to prevent reprocessing.
2. **Build dim_vehicle:** Reads only today's processed partition (partition pruning). Deduplicates by VIN using a Window function: `ORDER BY desc(mfg_year), desc(baseline_kmpl)` — newest vehicle with best fuel rating wins. Writes to Silver as Delta with `overwriteSchema=true`.
3. **Build dim_restricted_zones:** Reads `restricted_zones.json`. Validates geographic bounds (lat -90 to 90, long -180 to 180, min ≤ max). Writes to Silver as Delta.
4. **Build dim_date:** **One-time only.** Checks `isDeltaTable()` — if table exists, skips entirely. Generates every date from 2020-01-01 to 2035-12-31 using `sequence()`. Pre-computes `is_weekend` flag (used by Job 4 to exclude weekend fuel transactions).

**Key design decisions:**
- **Why explicit schema?** `inferSchema=True` forces Spark to read the file twice (once for schema inference, once for data). Explicit schemas eliminate this overhead.
- **Why Window dedup instead of dropDuplicates?** `dropDuplicates` is non-deterministic — Spark picks an arbitrary row. Window functions guarantee the newest, most accurate record survives.
- **Why delete raw files?** Prevents accidental double-processing if Airflow retries the DAG.
- **Why dim_date is one-time?** Calendar dates never change. Recreating this table daily wastes cluster compute for zero benefit.

---

### Job 2: `job2_asset_history_scd2.py` — SCD Type 2 Engine
**Layer:** Bronze → Silver → Gold (Postgres) | **Schedule:** Daily (DAG 1)

This is the most complex job. It maintains a **Slowly Changing Dimension Type 2** table that tracks exactly which driver operated which vehicle, at what rate, during what time period.

**Step-by-step flow:**

1. **List & Sort Raw Files:** Lists all `vehicle_assignment*.csv` files in S3. Sorts them: base file first (`vehicle_assignment.csv` → order -1), then incrementals by numeric suffix (`_1`, `_2`, ...). **Why sort?** SCD2 depends on chronological ordering. Processing file `_3` before `_1` would corrupt the timeline.

2. **Ingest Each File:**
   - Applies data quality filters (VIN/driver not empty, daily_rate > 0)
   - **VIN referential integrity:** Inner joins against `dim_vehicle` — assignments for unknown VINs are dropped (acts as a foreign key check since Delta doesn't enforce FKs)
   - **Unix timestamp normalization:** The `unix_to_date()` helper checks if the value is 10-digit (seconds) or 13-digit (milliseconds) and normalizes accordingly
   - **Year 9000 problem:** Filters out dates where `year < 1950` or `year > 2050`. Without this, upstream systems sending `9999-12-31` as "no end date" would break BI timeline visuals
   - **Conflict resolution:** If two assignments exist for the same VIN on the same start_date, the highest daily_rate wins (deterministic Window dedup)
   - **Suspended driver filter:** Queries PostgreSQL `gold.driver_safety_status` to exclude any driver with status = `SUSPENDED`

3. **Initialize SCD2 (first file only):**
   - Orders all records chronologically per VIN
   - Uses `LEAD(start_date)` window function to derive missing end_dates: if a VIN has records on Jan 1, Mar 1, and Jun 1, then Jan 1's end_date = Mar 1 and Mar 1's end_date = Jun 1
   - The latest record per VIN with no derivable end_date → status = `IN-TRANSIT`
   - All others → status = `ARCHIVED`

4. **Apply SCD2 (incremental files):**
   - **Pre-Merge Sweep (Driver Uniqueness):** Ensures that if a driver is reassigned to a new vehicle, their old active vehicle is closed out. Finds any active assignments for the driver and updates them to `ARCHIVED` with an `end_date` matching the new assignment's `start_date`.
   - Guarantees exactly ONE row per VIN entering the merge (latest start_date, highest rate tiebreak)
   - Uses the **NULL merge key pattern**: creates two staging rows per changed VIN:
     - Row A: `merge_key = vin` → matches existing IN-TRANSIT row → UPDATE to ARCHIVED (sets end_date)
     - Row B: `merge_key = NULL` → matches nothing → INSERT as new IN-TRANSIT row
   - This is a standard Delta Lake SCD2 pattern that performs both UPDATE and INSERT in a single atomic MERGE operation

5. **Gold Postgres Write:**
   - Writes full SCD2 table to a staging table via Spark JDBC
   - Executes `INSERT ... ON CONFLICT (vin, start_date) DO UPDATE` for idempotent upsert
   - Truncates staging table after upsert

**Key design decisions:**
- **Why NULL merge key?** Delta's MERGE can only UPDATE or INSERT per matched row, not both. The NULL key trick forces the new IN-TRANSIT row to be treated as an INSERT while the existing row gets UPDATEd.
- **Why staging table pattern?** Spark's JDBC writer doesn't support ON CONFLICT. Writing to a staging table first, then running raw SQL, gives us proper upsert semantics.
- **Why filter suspended drivers?** A suspended driver should never appear as "active" in any downstream dashboard. We enforce this at ingestion time.

---

### Job 3: `job3_fuel_enrichment.py` — Fuel Receipt Cleansing
**Layer:** Bronze → Silver | **Schedule:** Daily (DAG 2, 05:00 UTC)

**What it does:** Cleanses and deduplicates ~87 MB of raw fuel transaction data.

**Flow:**
1. Reads raw CSV with explicit schema. Filters: transaction_id not empty, fuel_liters > 0, odometer ≥ 0.
2. Writes cleaned data to `processed/` partitioned by `ingestion_date`.
3. Parses string timestamps to proper `TimestampType`. Drops rows where parsing fails.
4. **Deterministic dedup:** Window on `transaction_id`, ordered by `desc(odometer_reading)`. If a receipt appears twice, the one with the higher odometer (more recent state) wins.
5. Writes to Silver Delta as `fact_fuel_transactions`.
6. Deletes raw file only after successful Silver write.

**Why `partitionOverwriteMode=dynamic`?** This config ensures that when writing with `mode("overwrite")` and `partitionBy("ingestion_date")`, only TODAY's partition is replaced — historical partitions remain untouched.

---

### Job 4: `job4_gold_fuel_audit.py` — Fuel Efficiency Audit
**Layer:** Silver → Gold | **Schedule:** Daily (DAG 2, after Job 3)

The most join-heavy job. Computes km/liter for each vehicle and flags potential fuel fraud.

**Flow:**
1. **Load all Silver tables:** fact_fuel_transactions, dim_vehicle, dim_date, dim_maintenance
2. **Batch filtering:** Filters fuel records to `ingestion_date = today`. Gets distinct VINs in this batch.
3. **Historical LAG window:** Loads ALL historical fuel records for those VINs (not just today's). This is critical because `LAG(odometer_reading)` needs the previous fill-up's odometer to compute distance. If we only loaded today's records, the first record per VIN would have no previous odometer.
4. **Distance calculation:** `distance_driven = odometer_reading - prev_odometer`. Filters: distance must be > 0 and ≤ 2000 km (physics sanity check — no vehicle drives 2000+ km between fill-ups).
5. **Dimension joins:**
   - `dim_vehicle`: Gets `baseline_kmpl` and `model`. Drops records with null baseline.
   - `dim_date`: Joins on `transaction_date` to get `is_weekend`. **Excludes weekends** (BRD requirement — vehicles on weekend routes have different fuel profiles).
   - `dim_maintenance`: **Anti-join** — excludes fuel transactions on days the vehicle was in maintenance (fuel consumed during service is not representative of operational efficiency).
6. **Aggregation:** Groups by (vin, model, transaction_date), sums distance and fuel.
7. **KMPL calculation:** `km_per_liter = total_distance / total_fuel`
8. **Flagging:** `threshold_kmpl = baseline_kmpl × 0.88`. If actual KMPL < threshold → `FLAGGED` (potential fuel fraud or mechanical issue).
9. **Dedup to 1 row per VIN:** Keeps only the latest audit_date per VIN (Gold = current status).
10. **Dual write:** S3 Delta (partitioned by ingestion_date with `replaceWhere`) + PostgreSQL via staging upsert.

**Why `cache()` before writes?** The final DataFrame is used twice (S3 write + JDBC write). Without caching, Spark recomputes the entire DAG from Silver for each write action, doubling compute cost and risking race conditions with `spark.stop()`.

---

### Job 5: `job5_gold_fleet_snapshot.py` — Active Fleet Snapshot
**Layer:** Silver → Gold | **Schedule:** Daily (DAG 1, after Job 2)

**What it does:** Generates a daily point-in-time view of all IN-TRANSIT vehicles grouped by model.

**Flow:**
1. Reads `dim_asset_history_scd2`, filters `status == "IN-TRANSIT"`
2. Joins with `dim_vehicle` to get model name
3. Groups by model, counts distinct VINs → `no_of_active_vehicles`
4. Writes to S3 Delta (partitioned by snapshot_date) + PostgreSQL via staging upsert

**Why `countDistinct("vin")`?** SCD2 may have edge cases with multiple active rows per VIN. Counting distinct VINs ensures accurate fleet counts.

---

### Job 6: `job6_gold_safety_summary.py` — Safety Compliance Summary
**Layer:** Silver → Gold | **Schedule:** Daily (DAG 2, independent branch)

**What it does:** Aggregates streaming violation data into daily executive KPIs.

**Flow:**
1. Reads `fact_safety_violations` (written by the streaming processor)
2. Filters on `ingestion_date` (NOT event_timestamp — aligns with the batch run)
3. Computes: total_violations, speed_violations, zone_violations
4. **Violation type counting:** Uses `.contains()` instead of strict equality. A `SPEED_AND_GEOFENCE` event is counted in BOTH speed and zone breakdowns. Without this, combined violations would vanish from both columns.
5. **Top 10 drivers:** Ranks drivers by strike count, serializes as JSON array
6. Writes single-row summary to S3 Delta + PostgreSQL (top_10_drivers stored as JSONB)

---

### Job 7: `job7_yearly_maintenance_load.py` — Maintenance Schedule
**Layer:** Bronze → Silver | **Schedule:** Yearly (Jan 1, 08:00 UTC)

**What it does:** Loads annual maintenance schedules from the vendor.

**Flow:** Clean → parse dates → dedup by (vin, service_date) → write to Silver Delta partitioned by `ingestion_year`.

**Why yearly?** Maintenance schedules are delivered once per year by the fleet vendor. Partitioning by year ensures each year's schedule is an independent, replaceable unit.

---

### Job 8: `job8_monthly_cooldown.py` — Monthly Cooldown & HR Report
**Layer:** Gold → Gold + S3 Report | **Schedule:** Monthly (1st of month, 05:00 UTC)

The business logic enforcement job. Implements the monthly driver penalty reset cycle.

**Flow (strictly ordered):**
1. **Report Generation (BEFORE rollover):** Reads previous month's Gold data. FULL OUTER JOINs with active SCD2 fleet to capture: drivers with violations (in Gold), clean drivers (only in SCD2), and departed drivers (only in Gold). Formats a structured `.txt` report with Suspended/Active sections. Uploads to S3.

2. **Rollover (Monthly Cooldown):**
   - **ACTIVE drivers (< 10 strikes):** `strike_count → 0`, `current_adjusted_rate → base_rate` (full reset)
   - **SUSPENDED drivers (≥ 10 strikes):** Carried forward as-is. No cooldown. Status and strikes persist.
   - Creates NEW rows for the new month — **never overwrites** previous month's data

3. **Postgres Sync:** Upserts rollover rows via staging table pattern.

**Why ROLLOVER instead of UPDATE?** An in-place UPDATE would overwrite May's data, destroying the historical audit trail. With rollover, each month is an immutable partition. HR can query any past month to see the exact state at that time.

**Why MERGE instead of OVERWRITE?** If the streaming processor already wrote some rows for the new month before Job 8 ran, MERGE only inserts rows that don't already exist (idempotency).

---

## Real-Time Streaming Pipeline

### Architecture: 3-Component Design

```
producer.py (EC2)  →  Kafka Topic  →  ingestor.py (EMR)  →  Bronze S3 Delta  →  processor.py (EMR)
   IoT Simulator       telemetry_stream    Lightweight sink      Immutable audit log    Heavy processing
```

**Why split ingestor and processor?** Fault isolation. If the processor crashes (Postgres timeout, OOM on joins), the ingestor keeps consuming from Kafka. No messages are lost. The processor can restart and replay from Bronze.

### producer.py — IoT Telemetry Simulator
- Reads `telemetry_messages.json` and publishes to Kafka topic `telemetry_stream`
- Two modes: `batch` (all at once) or `stream` (continuous loop at configurable rate)
- Injects fresh UTC timestamps on every event (simulates real-time)
- Uses gzip compression and VIN-based message keys (ensures ordering per vehicle)

### ingestor.py — Kafka → Bronze S3
- **Zero business logic.** Only job: persist raw Kafka JSON to S3 as Delta format
- Uses `startingOffsets="latest"` (avoids replaying entire topic backlog on first run)
- `failOnDataLoss=false` (survives Kafka segment deletion from retention policy)
- Timestamp fallback: if `event_timestamp` is malformed, falls back to Kafka broker timestamp
- Writes Delta (not JSON) to avoid the "nested streaming sink" trap where `_spark_metadata` causes downstream stream discovery hangs

### processor.py — Bronze → Silver → Gold → Postgres
Consumes Bronze Delta via `readStream`, applies all business logic per micro-batch:

1. **Validation:** Null checks, speed 0–200 km/h, valid GPS coordinates, drops "Null Island" (0,0) which is a common GPS default for broken sensors
2. **Active Asset Validation:** Inner joins against SCD2 `IN-TRANSIT` records. Telemetry from unknown or decommissioned vehicles is dropped.
3. **Violation Detection:**
   - Speed > 110 km/h → `speed_flag = true`
   - GPS inside any restricted zone bounding box (spatial join) → `zone_flag = true`
4. **2-Minute Window Deduplication:** A driver speeding through a geofence generates a burst of events over 60–90 seconds. A 2-minute tumbling window treats this as ONE violation instead of 3–4 separate strikes. **Merged flags:** If SPEED occurs at t=0:00 and GEOFENCE at t=1:30 within the same window, the final violation_type = `SPEED_AND_GEOFENCE` (no data loss).
5. **Silver Write:** Appends `fact_safety_violations` partitioned by `ingestion_date`. Runs `OPTIMIZE` compaction every 20 batches.
6. **Gold Aggregation:** Sums strikes per (driver_id, month). Joins with SCD2 base_rate.
7. **Gold Delta Upsert:** Uses `batch_id > last_batch_id` as an idempotency guard — if a batch is replayed, strikes are NOT double-counted. Computes `current_adjusted_rate = base_rate × (1 - 0.05 × strikes)` capped at 10 strikes. ≥10 strikes → `SUSPENDED`.
8. **Postgres Sync:** Only pushes rows affected by the current batch (not the entire Gold table).

---

## Airflow Orchestration

### DAG 1: `omniroute_dims_scd2_snapshot_daily` — 00:00 UTC

```
pipeline_start
  ├→ sense_vehicle_registry ──→ job1 ──────────────┐
  │   (soft_fail=True)          dim_core_load       ├→ registry_ready (convergence gate)
  │   └→ [SKIPPED] → check_dim_vehicle_exists ──────┘
  │                    ├→ proceed_without_job1 (dim exists from prev run)
  │                    └→ skip_dag (first ever run, no base data)
  │
  ├→ sense_vehicle_assignment ──┐
  │   (soft_fail=True)          ├→ job2 ──→ job5 ──→ pipeline_end
  │   └→ [SKIPPED] → check_scd2_exists
  │                    ├→ proceed_without_job2 (SCD2 exists)
  │                    └→ skip_dag
```

**Convergence Gate Pattern:** `registry_ready` collects both paths (job1 success OR fallback). Job2 waits for BOTH `registry_ready` AND `sense_assignment` before executing. This prevents **Cross-Branch Trigger Rule Contamination** where a registry success alone could accidentally fire job2 before the assignment file arrives.

### DAG 2: `omniroute_fuel_audit_safety_daily` — 05:00 UTC

```
pipeline_start
  ├→ wait_for_dag1 (ExternalTaskSensor, 90-min timeout)
  │   ├→ [SUCCESS] → sense_fuel → job3 → job4 → pipeline_end
  │   └→ [SKIPPED] → check_dims_for_fuel
  │                    ├→ dims_ok (dim_vehicle exists from prev run) → sense_fuel → ...
  │                    └→ skip_fuel_branch → pipeline_end
  │
  └→ check_safety_violations_table (independent branch)
      ├→ job6_safety_summary → pipeline_end
      └→ skip_safety_branch → pipeline_end
```

**Cross-DAG Dependency:** `ExternalTaskSensor` waits for DAG 1's `pipeline_end` with `execution_delta=5 hours` (DAG 1 runs at 00:00, DAG 2 at 05:00). Uses `mode="reschedule"` to release worker slots while waiting.

**Job 6 Independence:** Safety summary runs on a completely separate branch from fuel. Even if fuel data never arrives, job6 still executes (it reads from streaming Silver, not fuel data).

### DAG 3: `omniroute_monthly_pipeline` — 1st of month, 05:00 UTC
```
pipeline_start → job8_monthly_cooldown → pipeline_end
```

### DAG 4: `omniroute_yearly_pipeline` — Jan 1, 08:00 UTC
```
pipeline_start → wait_for_maintenance_csv (7-day timeout, soft_fail) → job7 → pipeline_end
```
Sensor uses `poke_interval=6 hours` and `mode="reschedule"` — checks 4 times/day without wasting a worker slot for a week.

---

## PostgreSQL Gold Schema

All tables live in the `gold` schema. Every table has a corresponding `_stg` staging table for the upsert pattern.

| Table | PK | Updated By | Purpose |
|:--|:--|:--|:--|
| `asset_history_scd2` | (vin, start_date) | Job 2 | Full driver-vehicle assignment timeline |
| `driver_safety_status` | (driver_id, month) | Streaming + Job 8 | Monthly strike/suspension tracking |
| `fuel_efficiency_audit` | (vin) | Job 4 | Current fuel efficiency per vehicle |
| `active_fleet_snapshot` | (model, snapshot_date) | Job 5 | Daily fleet count by model |
| `safety_compliance_summary` | (report_date) | Job 6 | Daily violation KPIs |

**Indexes** are created on frequently filtered columns (status, month, audit_date, snapshot_date) to optimize Power BI query performance.

**The Staging Table Pattern (used everywhere):**
1. `TRUNCATE` staging table
2. Spark JDBC `append` to staging (fast bulk insert)
3. SQL `INSERT ... ON CONFLICT DO UPDATE` from staging → target
4. This is idempotent: rerunning produces identical results with zero duplicates

---

## Business Logic Deep Dive

### 1. Driver Safety & Suspension System
- **Strike accumulation:** Each violation = 1 strike (after 2-minute dedup)
- **Rate deduction:** `current_rate = base_rate × (1 - 0.05 × strikes)`, capped at 10 strikes (50% max deduction)
- **Suspension threshold:** ≥ 10 strikes in a month → `SUSPENDED`
- **Monthly cooldown (Job 8):** ACTIVE drivers reset to 0 strikes; SUSPENDED drivers carry forward
- **Circuit breaker:** Suspended drivers are excluded from new vehicle assignments (Job 2) and their telemetry is ignored (streaming processor)

### 2. Fuel Fraud Detection
- **Baseline comparison:** Each vehicle has a manufacturer-rated `baseline_kmpl`
- **Threshold:** `threshold = baseline × 0.88` (12% tolerance for real-world conditions)
- **Flag logic:** If actual km/liter < threshold → `FLAGGED`
- **Exclusions:** Weekends and maintenance days are excluded from the audit (abnormal fuel usage patterns)

### 3. SCD Type 2 Lifecycle
- `IN-TRANSIT`: Vehicle currently assigned to this driver (active record)
- `ARCHIVED`: Assignment ended (closed with end_date when new assignment arrives)
- The system maintains a complete, immutable history of every driver-vehicle relationship

---

## Power BI Dashboards

Three dashboards connect to PostgreSQL:
1. **Fleet Operations Dashboard** — Active fleet snapshot, vehicle distribution by model
2. **Safety Compliance Dashboard** — Daily violation trends, top offenders, suspension status
3. **Fuel Efficiency Dashboard** — KMPL trends, flagged vehicles, audit status

---

## Project Structure

```
OmniRoute Project/
├── README.md                          # This file
├── BRD.docx                          # Business Requirements Document
├── init.sql                          # PostgreSQL DDL (all Gold tables + indexes + grants)
├── mermaid_final.png                  # Architecture diagram
│
├── glue_jobs/                         # AWS Glue PySpark batch jobs
│   ├── job1_dim_core_load.py          # Bronze → Silver dimensions
│   ├── job2_asset_history_scd2.py     # SCD2 engine (most complex)
│   ├── job3_fuel_enrichment.py        # Fuel receipt cleansing
│   ├── job4_gold_fuel_audit.py        # KMPL calculation + fraud flagging
│   ├── job5_gold_fleet_snapshot.py    # Active fleet by model
│   ├── job6_gold_safety_summary.py    # Daily violation KPIs
│   ├── job7_yearly_maintenance_load.py # Annual maintenance schedule
│   └── job8_monthly_cooldown.py       # Monthly rollover + HR report
│
├── streaming/                         # Real-time telemetry pipeline
│   ├── producer.py                    # Kafka IoT simulator
│   ├── ingestor.py                    # Kafka → Bronze S3 (zero logic)
│   └── processor.py                   # Bronze → Silver → Gold → Postgres
│
├── Airflow/dags/                      # Orchestration
│   ├── omniroute_daily_dag.py         # 2 DAGs: dims+SCD2 (00:00) + fuel+safety (05:00)
│   ├── omniroute_monthly_dag.py       # Job 8 cooldown (1st of month)
│   └── omniroute_yearly_dag.py        # Job 7 maintenance (Jan 1)
│
├── data/                              # Sample/raw data files
│   ├── vehicle_registry.csv
│   ├── vehicle_assignment.csv
│   ├── vehicle_assignment_1.csv       # Incremental assignment file
│   ├── fuel_transactions.csv          # ~87 MB
│   ├── maintenance_logs.csv
│   ├── restricted_zones.json
│   └── telemetry_messages.json        # ~2.2 MB (Kafka source data)
│
├── BI Reports/                        # Power BI dashboard exports
│   ├── Dashboard1.pdf
│   ├── Dashboard2.pdf
│   └── Dashboard3.pdf
│
└── DAG_screenshots/                   # Airflow UI screenshots
    └── *.png
```

> **OmniRoute** — *Safety First, Efficiency Always.* 🚛
