from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os

GCP_PROJECT = os.getenv("GCP_PROJECT")
BQ_DATASET_RAW  = os.getenv("BQ_DATASET_RAW")
BQ_DATASET_MART = os.getenv("BQ_DATASET_MART")

DBT_BIN      = "/home/airflow/.local/bin/dbt"
DBT_DIR      = "/opt/airflow/dbt"
DBT_PROFILES = "/opt/airflow/dbt"

default_args = {
    "owner":            "crypto-platform",
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="dbt_refresh_mart",
    description="Refreshes dbt mart models every 5 minutes",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["dbt", "bigquery", "crypto"],
) as dag:

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"{DBT_BIN} debug --profiles-dir {DBT_PROFILES}",
        cwd=DBT_DIR,
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT_BIN} run --select staging --profiles-dir {DBT_PROFILES}",
        cwd=DBT_DIR,
    )

    dbt_run_mart = BashOperator(
        task_id="dbt_run_mart",
        bash_command=f"{DBT_BIN} run --select mart --profiles-dir {DBT_PROFILES}",
        cwd=DBT_DIR,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_BIN} test --profiles-dir {DBT_PROFILES}",
        cwd=DBT_DIR,
    )

    dbt_debug >> dbt_run_staging >> dbt_run_mart >> dbt_test