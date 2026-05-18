"""
OmniRoute — DAILY Pipeline DAGs
===============================
Contains two fault-tolerant Airflow DAGs:
1. omniroute_dims_scd2_snapshot_daily (00:00 UTC): Processes dimensions, SCD2, and active fleet snapshots.
2. omniroute_fuel_audit_safety_daily (05:00 UTC): Processes fuel enrichment, fuel audit, and safety summaries.
"""

from datetime import datetime, timedelta
import boto3

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sensors.external_task import ExternalTaskSensor

try:
    from airflow.operators.empty import EmptyOperator
except ImportError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator

SILVER_BUCKET   = Variable.get("SILVER_BUCKET", default_var="ttn-de-bootcamp-silver-us-east-1")
SILVER_BASE     = Variable.get("SILVER_PREFIX",  default_var="poc-bootcamp-grp2-silver")
DIM_VEHICLE_PREFIX  = f"{SILVER_BASE}/dim_vehicle/_delta_log/"
SCD2_PREFIX         = f"{SILVER_BASE}/dim_asset_history_scd2/_delta_log/"
SAFETY_VIO_PREFIX   = f"{SILVER_BASE}/fact_safety_violations/_delta_log/"

def _s3_prefix_exists(bucket: str, prefix: str) -> bool:
    s3 = boto3.client("s3")
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return resp.get("KeyCount", 0) > 0

def check_dim_vehicle_fallback(**context):
    if _s3_prefix_exists(SILVER_BUCKET, DIM_VEHICLE_PREFIX):
        return "proceed_without_job1"
    else:
        return "skip_dag"

def check_scd2_fallback(**context):
    if _s3_prefix_exists(SILVER_BUCKET, SCD2_PREFIX):
        return "proceed_without_job2"
    else:
        return "skip_dag"

def check_dims_for_fuel(**context):
    if _s3_prefix_exists(SILVER_BUCKET, DIM_VEHICLE_PREFIX):
        return "dims_ok_proceed_fuel"
    else:
        return "skip_fuel_branch"

def check_safety_violations_table(**context):
    if _s3_prefix_exists(SILVER_BUCKET, SAFETY_VIO_PREFIX):
        return "job6_safety_summary"
    else:
        return "skip_safety_branch"

PG_HOST     = Variable.get("PG_HOST")
PG_DB       = Variable.get("PG_DB",       default_var="fleet_db")
PG_USER     = Variable.get("PG_USER",     default_var="fleet_user")
PG_PASS     = Variable.get("PG_PASS")
AWS_CONN_ID = Variable.get("AWS_CONN_ID", default_var="aws_default")
GLUE_SCRIPTS_BUCKET = Variable.get("GLUE_SCRIPTS_BUCKET", default_var="ttn-de-bootcamp-scripts-us-east-1")
GLUE_SCRIPTS_PREFIX = Variable.get("GLUE_SCRIPTS_PREFIX", default_var="glue_jobs")

BRONZE_BUCKET = Variable.get("BRONZE_BUCKET", default_var="ttn-de-bootcamp-bronze-us-east-1")
BRONZE_PREFIX = Variable.get("BRONZE_PREFIX",  default_var="poc-bootcamp-grp2-bronze")
BASE_PREFIX   = f"{BRONZE_PREFIX}/raw"

REGISTRY_KEY   = f"{BASE_PREFIX}/vehicle_registry/vehicle_registry.csv"
ASSIGNMENT_KEY = f"{BASE_PREFIX}/vehicle_assignment/vehicle_assignment*.csv"
FUEL_KEY       = f"{BASE_PREFIX}/fuel_transactions/fuel_transactions.csv"

GLUE_JOB_1 = "job1_dim_core_load"
GLUE_JOB_2 = "job2_asset_history_scd2"
GLUE_JOB_3 = "job3_fuel_enrichment"
GLUE_JOB_4 = "job4_gold_fuel_audit"
GLUE_JOB_5 = "job5_active_fleet"
GLUE_JOB_6 = "job6_safety_summary"

GLUE_COMMON_ARGS = {
    "--datalake-formats":          "delta",
    "--additional-python-modules": "psycopg2-binary,python-dotenv",
    # config.py and utils.py are library files, NOT separate jobs.
    # Glue adds them to the Python path automatically at job startup.
    "--extra-py-files":            f"s3://{GLUE_SCRIPTS_BUCKET}/{GLUE_SCRIPTS_PREFIX}/config.py,s3://{GLUE_SCRIPTS_BUCKET}/{GLUE_SCRIPTS_PREFIX}/utils.py",
    "--enable-glue-datacatalog":   "true",
    "--enable-spark-ui":           "true",
    "--enable-job-insights":       "true",
}

def db_args(extra: dict = None) -> dict:
    base = {
        **GLUE_COMMON_ARGS,
        "--db_host":     PG_HOST,
        "--db_name":     PG_DB,
        "--db_user":     PG_USER,
        "--db_password": PG_PASS,
    }
    if extra:
        base.update(extra)
    return base

DEFAULT_ARGS = {
    "owner":            "omniroute",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=10),
    "email_on_failure": False,
}

with DAG(
    dag_id="omniroute_dims_scd2_snapshot_daily",
    description="00:00 UTC — Sensors → dims/SCD2 → snapshot",
    schedule_interval="0 0 * * *",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["omniroute", "daily", "dims", "scd2", "snapshot"],
) as dag_dims:

    pipeline_start = EmptyOperator(task_id="pipeline_start")
    pipeline_end   = EmptyOperator(
        task_id="pipeline_end",
        trigger_rule="none_failed",
    )
    skip_dag = EmptyOperator(task_id="skip_dag")
    proceed_without_job1 = EmptyOperator(task_id="proceed_without_job1")
    proceed_without_job2 = EmptyOperator(task_id="proceed_without_job2")
    
    registry_ready = EmptyOperator(
        task_id="registry_ready",
        trigger_rule="none_failed_min_one_success",
    )

    sense_registry = S3KeySensor(
        task_id="sense_vehicle_registry",
        bucket_name=BRONZE_BUCKET,
        bucket_key=REGISTRY_KEY,
        aws_conn_id=AWS_CONN_ID,
        timeout=1800,
        poke_interval=60,
        mode="poke",
        soft_fail=True,
    )

    sense_assignment = S3KeySensor(
        task_id="sense_vehicle_assignment",
        bucket_name=BRONZE_BUCKET,
        bucket_key=ASSIGNMENT_KEY,
        wildcard_match=True,
        aws_conn_id=AWS_CONN_ID,
        timeout=1800,
        poke_interval=60,
        mode="poke",
        soft_fail=True,
    )

    branch_dim_vehicle = BranchPythonOperator(
        task_id="check_dim_vehicle_exists",
        python_callable=check_dim_vehicle_fallback,
        trigger_rule="all_skipped",
    )

    branch_scd2 = BranchPythonOperator(
        task_id="check_scd2_exists",
        python_callable=check_scd2_fallback,
        trigger_rule="all_skipped",
    )

    job1 = GlueJobOperator(
        task_id="job1_dim_core_load",
        job_name=GLUE_JOB_1,
        aws_conn_id=AWS_CONN_ID,
        script_args={**GLUE_COMMON_ARGS, "--ingestion_date": "{{ data_interval_end.strftime('%Y-%m-%d') }}"},
        wait_for_completion=True,
    )

    job2 = GlueJobOperator(
        task_id="job2_asset_history_scd2",
        job_name=GLUE_JOB_2,
        aws_conn_id=AWS_CONN_ID,
        script_args=db_args({"--ingestion_date": "{{ data_interval_end.strftime('%Y-%m-%d') }}"}),
        wait_for_completion=True,
    )

    job5 = GlueJobOperator(
        task_id="job5_active_fleet_snapshot",
        job_name=GLUE_JOB_5,
        aws_conn_id=AWS_CONN_ID,
        script_args=db_args({"--snapshot_date": "{{ data_interval_end.strftime('%Y-%m-%d') }}"}),
        wait_for_completion=True,
        trigger_rule="none_failed_min_one_success",
    )

    pipeline_start >> [sense_registry, sense_assignment]

    sense_registry >> job1
    sense_registry >> branch_dim_vehicle
    branch_dim_vehicle >> [proceed_without_job1, skip_dag]
    [job1, proceed_without_job1] >> registry_ready

    sense_assignment >> branch_scd2
    branch_scd2 >> [proceed_without_job2, skip_dag]

    [registry_ready, sense_assignment] >> job2

    [job2, proceed_without_job2] >> job5 >> pipeline_end


with DAG(
    dag_id="omniroute_fuel_audit_safety_daily",
    description="05:00 UTC — Fuel Audit + Safety Summary",
    schedule_interval="0 5 * * *",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["omniroute", "daily", "fuel", "safety"],
) as dag_fuel:

    pipeline_start = EmptyOperator(task_id="pipeline_start")
    pipeline_end   = EmptyOperator(
        task_id="pipeline_end",
        trigger_rule="none_failed",
    )
    skip_fuel_branch   = EmptyOperator(task_id="skip_fuel_branch")
    skip_safety_branch = EmptyOperator(task_id="skip_safety_branch")
    dims_ok_proceed_fuel = EmptyOperator(task_id="dims_ok_proceed_fuel")

    wait_for_dag1 = ExternalTaskSensor(
        task_id="wait_for_dag1_pipeline_end",
        external_dag_id="omniroute_dims_scd2_snapshot_daily",
        external_task_id="pipeline_end",
        allowed_states=["success"],
        failed_states=["failed"],
        execution_delta=timedelta(hours=5),
        timeout=5400,
        poke_interval=120,
        mode="reschedule",
        soft_fail=True,
    )

    branch_dims_check = BranchPythonOperator(
        task_id="check_dims_for_fuel",
        python_callable=check_dims_for_fuel,
        trigger_rule="all_skipped",
    )

    sense_fuel = S3KeySensor(
        task_id="sense_fuel_transactions",
        bucket_name=BRONZE_BUCKET,
        bucket_key=FUEL_KEY,
        aws_conn_id=AWS_CONN_ID,
        timeout=1800,
        poke_interval=60,
        mode="poke",
        soft_fail=True,
        trigger_rule="none_failed_min_one_success",
    )

    branch_safety_check = BranchPythonOperator(
        task_id="check_safety_violations_table",
        python_callable=check_safety_violations_table,
    )

    job3 = GlueJobOperator(
        task_id="job3_fuel_enrichment",
        job_name=GLUE_JOB_3,
        aws_conn_id=AWS_CONN_ID,
        script_args={**GLUE_COMMON_ARGS, "--ingestion_date": "{{ data_interval_end.strftime('%Y-%m-%d') }}"},
        wait_for_completion=True,
    )

    job4 = GlueJobOperator(
        task_id="job4_gold_fuel_audit",
        job_name=GLUE_JOB_4,
        aws_conn_id=AWS_CONN_ID,
        script_args=db_args({"--ingestion_date": "{{ data_interval_end.strftime('%Y-%m-%d') }}"}),
        wait_for_completion=True,
    )

    job6 = GlueJobOperator(
        task_id="job6_safety_summary",
        job_name=GLUE_JOB_6,
        aws_conn_id=AWS_CONN_ID,
        script_args=db_args({"--report_date": "{{ data_interval_end.strftime('%Y-%m-%d') }}"}),
        wait_for_completion=True,
    )

    pipeline_start >> [wait_for_dag1, branch_safety_check]

    wait_for_dag1 >> sense_fuel
    wait_for_dag1 >> branch_dims_check
    branch_dims_check >> [dims_ok_proceed_fuel, skip_fuel_branch]
    dims_ok_proceed_fuel >> sense_fuel
    sense_fuel >> job3 >> job4 >> pipeline_end
    skip_fuel_branch >> pipeline_end

    branch_safety_check >> [job6, skip_safety_branch]
    job6 >> pipeline_end
    skip_safety_branch >> pipeline_end
