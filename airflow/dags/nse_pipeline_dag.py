"""
NSE Market Data Pipeline DAG

Schedule: Mon-Fri 13:30 UTC (30 minutes after NSE closes at 15:30 IST).
Order: ingest → copy_into_snowflake → dbt run → dbt test → dbt snapshot.

Backfill: catchup=True, max_active_runs=1 prevents parallel runs writing to the same
S3 partition. Each task reads RUN_DATE from Airflow's logical_date ({{ ds }}).
"""

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.email import send_email

logger = logging.getLogger(__name__)

REPO_ROOT = "/opt/airflow"
DBT_DIR = f"{REPO_ROOT}/dbt_project"

# shared env passed to every bash task
_COMMON_ENV = {
    "S3_BUCKET": os.environ.get("S3_BUCKET", ""),
    "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
    "AWS_REGION": os.environ.get("AWS_REGION", "ap-south-1"),
    "SNOWFLAKE_ACCOUNT": os.environ.get("SNOWFLAKE_ACCOUNT", ""),
    "SNOWFLAKE_USER": os.environ.get("SNOWFLAKE_USER", ""),
    "SNOWFLAKE_PASSWORD": os.environ.get("SNOWFLAKE_PASSWORD", ""),
    "SNOWFLAKE_ROLE": os.environ.get("SNOWFLAKE_ROLE", "TRANSFORMER"),
    "SNOWFLAKE_DATABASE": os.environ.get("SNOWFLAKE_DATABASE", "NSE_MARKET"),
    "SNOWFLAKE_WAREHOUSE": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
}


def _on_failure(context):
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    exec_date = context["execution_date"]
    exception = context.get("exception")

    logger.error("task_failed dag=%s task=%s date=%s exc=%s", dag_id, task_id, exec_date, exception)

    alert_email = os.environ.get("ALERT_EMAIL")
    if alert_email:
        send_email(
            to=alert_email,
            subject=f"[AIRFLOW FAILURE] {dag_id} / {task_id}",
            html_content=(
                f"<b>Task:</b> {task_id}<br>"
                f"<b>Date:</b> {exec_date}<br>"
                f"<b>Exception:</b> {exception}"
            ),
        )


default_args = {
    "owner": "sachin",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _on_failure,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="nse_market_pipeline",
    description="Daily NSE price ingest → Snowflake → dbt",
    schedule_interval="30 13 * * 1-5",
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["nse", "market-data", "snowflake"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_nse_prices",
        bash_command=f"cd {REPO_ROOT} && python -m ingestion.main --run-date {{{{ ds }}}}",
        env={**_COMMON_ENV, "RUN_DATE": "{{ ds }}"},
    )

    copy_into_sf = BashOperator(
        task_id="copy_into_snowflake",
        bash_command=(
            f"cd {REPO_ROOT} && python -c \""
            "import sys; sys.path.insert(0, '.'); "
            "from ingestion.config import load_config; "
            "from ingestion.writer import copy_into_snowflake; "
            "cfg = load_config(run_date='{{{{ ds }}}}'); "
            "copy_into_snowflake(cfg['snowflake'], cfg['s3'], '{{{{ ds }}}}')\""
        ),
        env={**_COMMON_ENV, "RUN_DATE": "{{ ds }}"},
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --target prod",
        env=_COMMON_ENV,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --target prod",
        env=_COMMON_ENV,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_DIR} && dbt snapshot --target prod",
        env=_COMMON_ENV,
    )

    ingest >> copy_into_sf >> dbt_run >> dbt_test >> dbt_snapshot
