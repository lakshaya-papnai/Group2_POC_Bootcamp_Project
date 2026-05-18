"""
config.py – Centralised path configuration for OmniRoute Streaming Jobs.

Usage in any streaming script:
    from config import cfg

    bronze_path = cfg.TELEMETRY_BRONZE_PATH
    checkpoint  = cfg.INGESTOR_CHECKPOINT
    ...

Values are resolved from environment variables set via .env (local/dev)
or injected at runtime by the execution environment (EMR, AWS Glue Streaming,
Docker, etc.). A clear EnvironmentError is raised for any missing required var.
"""

import os

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _require(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        raise EnvironmentError(
            f"[config] Required environment variable '{key}' is not set. "
            "Set it in .env or inject it at runtime."
        )
    return val

def _get(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


# ---------------------------------------------------------------------------
# Config class
# ---------------------------------------------------------------------------
class _Config:
    # ── Buckets & prefixes ──────────────────────────────────────────────────
    BRONZE_BUCKET = _require("BRONZE_BUCKET")
    BRONZE_PREFIX = _require("BRONZE_PREFIX")

    SILVER_BUCKET = _require("SILVER_BUCKET")
    SILVER_PREFIX = _require("SILVER_PREFIX")

    GOLD_BUCKET   = _require("GOLD_BUCKET")
    GOLD_PREFIX   = _require("GOLD_PREFIX")

    # ── Convenience base URLs ───────────────────────────────────────────────
    BRONZE_BASE = f"s3://{BRONZE_BUCKET}/{BRONZE_PREFIX}"
    SILVER_BASE = f"s3://{SILVER_BUCKET}/{SILVER_PREFIX}"
    GOLD_BASE   = f"s3://{GOLD_BUCKET}/{GOLD_PREFIX}"

    # ── Telemetry Bronze path (ingestor writes here; processor reads here) ──
    _TEL_SUBPATH        = _get("TELEMETRY_BRONZE_SUBPATH", "raw/telemetry_stream")
    TELEMETRY_BRONZE_PATH = f"{BRONZE_BASE}/{_TEL_SUBPATH}"

    # ── Checkpoint paths ────────────────────────────────────────────────────
    _CKPT_BUCKET        = _get("CHECKPOINT_BUCKET",             BRONZE_BUCKET)
    _CKPT_PREFIX        = _get("CHECKPOINT_PREFIX",             "8834_Lakshaya_bronze/checkpoints")
    _CKPT_BASE          = f"s3://{_CKPT_BUCKET}/{_CKPT_PREFIX}"

    _INGESTOR_CKPT_SUB  = _get("INGESTOR_CHECKPOINT_SUBPATH",  "ingestor/")
    _PROCESSOR_CKPT_SUB = _get("PROCESSOR_CHECKPOINT_SUBPATH", "processor_v2/")

    INGESTOR_CHECKPOINT  = f"{_CKPT_BASE}/{_INGESTOR_CKPT_SUB}"
    PROCESSOR_CHECKPOINT = f"{_CKPT_BASE}/{_PROCESSOR_CKPT_SUB}"

    # ── Silver paths ────────────────────────────────────────────────────────
    _S_FACT_SAFE = _get("SILVER_FACT_SAFETY",            "fact_safety_violations")
    _S_SCD2      = _get("SILVER_DIM_ASSET_HISTORY_SCD2", "dim_asset_history_scd2")
    _S_ZONES     = _get("SILVER_DIM_ZONES",              "dim_restricted_zones")

    FACT_SAFETY_PATH = f"{SILVER_BASE}/{_S_FACT_SAFE}"
    SCD2_PATH        = f"{SILVER_BASE}/{_S_SCD2}"
    DIM_ZONES_PATH   = f"{SILVER_BASE}/{_S_ZONES}"

    # ── Gold paths ──────────────────────────────────────────────────────────
    _G_DRV_SAFETY = _get("GOLD_DRIVER_SAFETY_STATUS", "driver_safety_status")
    GOLD_DRIVER_SAFETY_STATUS_PATH = f"{GOLD_BASE}/{_G_DRV_SAFETY}"

    # ── Kafka ───────────────────────────────────────────────────────────────
    KAFKA_TOPIC = _get("KAFKA_TOPIC", "telemetry_stream")

    # ── Producer ────────────────────────────────────────────────────────────
    PRODUCER_DATA_FILE = _get("PRODUCER_DATA_FILE", "data/telemetry_messages.json")


# Single shared instance
cfg = _Config()
