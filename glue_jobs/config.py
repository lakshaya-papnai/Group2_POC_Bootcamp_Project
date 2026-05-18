"""
config.py – Centralised path configuration for OmniRoute Glue Jobs.

Usage in any job:
    from config import cfg

    spark.read.csv(cfg.RAW_VEHICLE_REGISTRY_PATH)
    df.write.save(cfg.DIM_VEHICLE_PATH)
    ...

The values are resolved from environment variables (set via .env or
AWS Glue Job Parameters injected at runtime). A sensible default is
provided only where it is safe to do so; bucket names and prefixes
MUST come from the environment.
"""

import os

# ---------------------------------------------------------------------------
# Helper: read an env var or raise a clear error
# ---------------------------------------------------------------------------
def _require(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        raise EnvironmentError(
            f"[config] Required environment variable '{key}' is not set. "
            "Set it in .env or as a Glue Job Parameter."
        )
    return val

def _get(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


# ---------------------------------------------------------------------------
# Config class – build all paths once at import time
# ---------------------------------------------------------------------------
class _Config:
    # ── Buckets & prefixes ──────────────────────────────────────────────────
    BRONZE_BUCKET  = _require("BRONZE_BUCKET")
    BRONZE_PREFIX  = _require("BRONZE_PREFIX")

    SILVER_BUCKET  = _require("SILVER_BUCKET")
    SILVER_PREFIX  = _require("SILVER_PREFIX")

    GOLD_BUCKET    = _require("GOLD_BUCKET")
    GOLD_PREFIX    = _require("GOLD_PREFIX")

    # ── Convenience base URLs ───────────────────────────────────────────────
    BRONZE_BASE    = f"s3://{BRONZE_BUCKET}/{BRONZE_PREFIX}"
    SILVER_BASE    = f"s3://{SILVER_BUCKET}/{SILVER_PREFIX}"
    GOLD_BASE      = f"s3://{GOLD_BUCKET}/{GOLD_PREFIX}"

    # ── Raw source keys / prefixes ──────────────────────────────────────────
    _RAW_VEHICLE_REGISTRY_KEY     = _get("RAW_VEHICLE_REGISTRY_KEY",
                                         "vehicle_registry/vehicle_registry.csv")
    _RAW_VEHICLE_ASSIGNMENT_PFX   = _get("RAW_VEHICLE_ASSIGNMENT_PREFIX",
                                         "vehicle_assignment/")
    _RAW_FUEL_TRANSACTIONS_KEY    = _get("RAW_FUEL_TRANSACTIONS_KEY",
                                         "fuel_transactions/fuel_transactions.csv")
    _RAW_MAINTENANCE_KEY          = _get("RAW_MAINTENANCE_KEY",
                                         "maintenance_logs/maintenance_schedules.csv")

    # Full RAW S3 paths (used by Spark readers)
    RAW_VEHICLE_REGISTRY_PATH  = f"{BRONZE_BASE}/raw/{_RAW_VEHICLE_REGISTRY_KEY}"
    RAW_FUEL_TRANSACTIONS_PATH = f"{BRONZE_BASE}/raw/{_RAW_FUEL_TRANSACTIONS_KEY}"
    RAW_MAINTENANCE_PATH       = f"{BRONZE_BASE}/raw/{_RAW_MAINTENANCE_KEY}"

    # S3 key strings (used by boto3 delete_object)
    RAW_VEHICLE_REGISTRY_KEY   = f"{BRONZE_PREFIX}/raw/{_RAW_VEHICLE_REGISTRY_KEY}"
    RAW_VEHICLE_ASSIGNMENT_PFX = f"{BRONZE_PREFIX}/raw/{_RAW_VEHICLE_ASSIGNMENT_PFX}"
    RAW_FUEL_TRANSACTIONS_KEY  = f"{BRONZE_PREFIX}/raw/{_RAW_FUEL_TRANSACTIONS_KEY}"
    RAW_MAINTENANCE_KEY        = f"{BRONZE_PREFIX}/raw/{_RAW_MAINTENANCE_KEY}"

    # ── Bronze PROCESSED paths ──────────────────────────────────────────────
    _PROC_VEH_REG   = _get("PROCESSED_VEHICLE_REGISTRY_SUBPATH",   "processed/vehicle_registry")
    _PROC_VEH_ASGN  = _get("PROCESSED_VEHICLE_ASSIGNMENT_SUBPATH", "processed/vehicle_assignment")
    _PROC_FUEL      = _get("PROCESSED_FUEL_RECEIPTS_SUBPATH",      "processed/fuel_receipts")
    _PROC_MAINT     = _get("PROCESSED_MAINTENANCE_SUBPATH",        "processed/maintenance_logs")

    PROCESSED_VEHICLE_REGISTRY_PATH   = f"{BRONZE_BASE}/{_PROC_VEH_REG}"
    PROCESSED_VEHICLE_ASSIGNMENT_PATH = f"{BRONZE_BASE}/{_PROC_VEH_ASGN}"
    PROCESSED_FUEL_RECEIPTS_PATH      = f"{BRONZE_BASE}/{_PROC_FUEL}"
    PROCESSED_MAINTENANCE_PATH        = f"{BRONZE_BASE}/{_PROC_MAINT}"

    # ── Silver paths ────────────────────────────────────────────────────────
    _S_DIM_VEH    = _get("SILVER_DIM_VEHICLE",            "dim_vehicle")
    _S_DIM_ZONES  = _get("SILVER_DIM_ZONES",              "dim_restricted_zones")
    _S_DIM_DATE   = _get("SILVER_DIM_DATE",               "dim_date")
    _S_SCD2       = _get("SILVER_DIM_ASSET_HISTORY_SCD2", "dim_asset_history_scd2")
    _S_FACT_FUEL  = _get("SILVER_FACT_FUEL",              "fact_fuel_transactions")
    _S_FACT_SAFE  = _get("SILVER_FACT_SAFETY",            "fact_safety_violations")
    _S_DIM_MAINT  = _get("SILVER_DIM_MAINTENANCE",        "dim_maintenance_schedule")

    DIM_VEHICLE_PATH         = f"{SILVER_BASE}/{_S_DIM_VEH}"
    DIM_ZONES_PATH           = f"{SILVER_BASE}/{_S_DIM_ZONES}"
    DIM_DATE_PATH            = f"{SILVER_BASE}/{_S_DIM_DATE}"
    SCD2_PATH                = f"{SILVER_BASE}/{_S_SCD2}"
    FACT_FUEL_PATH           = f"{SILVER_BASE}/{_S_FACT_FUEL}"
    FACT_SAFETY_PATH         = f"{SILVER_BASE}/{_S_FACT_SAFE}"
    DIM_MAINTENANCE_PATH     = f"{SILVER_BASE}/{_S_DIM_MAINT}"

    # ── Gold paths ──────────────────────────────────────────────────────────
    _G_FUEL_AUDIT    = _get("GOLD_FUEL_AUDIT",           "fuel_efficiency_audit")
    _G_FLEET_SNAP    = _get("GOLD_FLEET_SNAPSHOT",       "active_fleet_snapshot")
    _G_SAFETY_SUM    = _get("GOLD_SAFETY_SUMMARY",       "safety_compliance_summary")
    _G_DRV_SAFETY    = _get("GOLD_DRIVER_SAFETY_STATUS", "driver_safety_status")
    _G_REPORT_PFX    = _get("GOLD_REPORT_PREFIX",        "fleet_reports")

    GOLD_FUEL_AUDIT_PATH          = f"{GOLD_BASE}/{_G_FUEL_AUDIT}"
    GOLD_FLEET_SNAPSHOT_PATH      = f"{GOLD_BASE}/{_G_FLEET_SNAP}"
    GOLD_SAFETY_SUMMARY_PATH      = f"{GOLD_BASE}/{_G_SAFETY_SUM}"
    GOLD_DRIVER_SAFETY_STATUS_PATH = f"{GOLD_BASE}/{_G_DRV_SAFETY}"
    GOLD_REPORT_PREFIX            = f"{GOLD_PREFIX}/{_G_REPORT_PFX}"   # key prefix, no s3://


# Single shared instance
cfg = _Config()
