"""
OmniRoute — YEARLY Pipeline DAG
===============================
Runs on January 1st at 08:00 UTC each year.
Waits for the annual maintenance schedule CSV (with a 7-day soft-fail timeout) and executes Job 7 to load the data.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

try:
    from airflow.operators.empty import EmptyOperator
except ImportError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator

AWS_CONN_ID    = Variable.get("AWS_CONN_ID",    default_var="aws_default")
BRONZE_BUCKET  = Variable.get("BRONZE_BUCKET",  default_var="ttn-de-bootcamp-bronze-us-east-1")
BRONZE_PREFIX  = Variable.get("BRONZE_PREFIX",  default_var="poc-bootcamp-grp2-bronze")
GLUE_SCRIPTS_BUCKET = Variable.get("GLUE_SCRIPTS_BUCKET", default_var="ttn-de-bootcamp-scripts-us-east-1")
GLUE_SCRIPTS_PREFIX = Variable.get("GLUE_SCRIPTS_PREFIX", default_var="glue_jobs")

MAINTENANCE_S3_KEY = f"{BRONZE_PREFIX}/raw/maintenance_logs/maintenance_schedules.csv"

GLUE_JOB_7 = "job7_yearly_maintenance_load"

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

DEFAULT_ARGS = {
    "owner":            "omniroute",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(hours=1),
    "email_on_failure": False,
}

with DAG(
    dag_id="omniroute_yearly_pipeline",
    description="OmniRoute yearly: S3 sensor → job7 maintenance load",
    schedule_interval="0 8 1 1 *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["omniroute", "yearly", "glue"],
) as dag:

    pipeline_start = EmptyOperator(task_id="pipeline_start")
    
    pipeline_end   = EmptyOperator(
        task_id="pipeline_end",
        trigger_rule="none_failed",
    )

    wait_for_maintenance_file = S3KeySensor(
        task_id="wait_for_annual_maintenance_csv",
        bucket_name=BRONZE_BUCKET,
        bucket_key=MAINTENANCE_S3_KEY,
        aws_conn_id=AWS_CONN_ID,
        poke_interval=60 * 60 * 6,
        timeout=60 * 60 * 24 * 7,
        mode="reschedule",
        soft_fail=True,
    )

    job7 = GlueJobOperator(
        task_id="job7_yearly_maintenance_load",
        job_name=GLUE_JOB_7,
        aws_conn_id=AWS_CONN_ID,
        script_args={
            **GLUE_COMMON_ARGS,
            "--ingestion_date": "{{ ds }}",
        },
        wait_for_completion=True,
    )

    pipeline_start >> wait_for_maintenance_file >> job7 >> pipeline_end
