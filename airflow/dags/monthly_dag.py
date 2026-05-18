"""
OmniRoute — MONTHLY Pipeline DAG
===================================
Runs on the 1st of every month at 05:00 UTC.
Executes Job 8 to generate the Monthly Rate Deduction Report, create rollover rows for the new month in Gold Delta, and sync the rows to Postgres.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

try:
    from airflow.operators.empty import EmptyOperator
except ImportError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator

PG_HOST     = Variable.get("PG_HOST")
PG_DB       = Variable.get("PG_DB",       default_var="fleet_db")
PG_USER     = Variable.get("PG_USER",     default_var="fleet_user")
PG_PASS     = Variable.get("PG_PASS")
AWS_CONN_ID = Variable.get("AWS_CONN_ID", default_var="aws_default")

GLUE_JOB_8 = "job8_monthly_cooldown"

GLUE_COMMON_ARGS = {
    "--datalake-formats":          "delta",
    "--additional-python-modules": "psycopg2-binary,python-dotenv",
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
    "retry_delay":      timedelta(minutes=15),
    "email_on_failure": False,
}

with DAG(
    dag_id="omniroute_monthly_pipeline",
    description="OmniRoute monthly (05:00 UTC, 1st): rollover cooldown + rate deduction report",
    schedule_interval="0 5 1 * *",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["omniroute", "monthly"],
) as dag:

    pipeline_start = EmptyOperator(task_id="pipeline_start")
    pipeline_end   = EmptyOperator(
        task_id="pipeline_end",
        trigger_rule="none_failed",
    )

    job8_cooldown = GlueJobOperator(
        task_id="job8_monthly_cooldown",
        job_name=GLUE_JOB_8,
        aws_conn_id=AWS_CONN_ID,
        script_args=db_args({
            "--execution_date": "{{ data_interval_end.strftime('%Y-%m-%d') }}",
        }),
        wait_for_completion=True,
    )

    pipeline_start >> job8_cooldown >> pipeline_end
