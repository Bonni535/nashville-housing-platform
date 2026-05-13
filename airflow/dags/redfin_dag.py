
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from dag_utils import notify_slack_failure, notify_slack_success, write_audit_log

from ingestion.sources.redfin import ingest_redfin

PIPELINE_HOME = os.environ.get(
    "PIPELINE_HOME", "/opt/airflow/nashville-housing-platform"
)
DBT_DIR = f"{PIPELINE_HOME}/housing_pipeline"

default_args = {
    "owner": "luca",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_slack_failure,
}

with DAG(
    dag_id="redfin_dag",
    description="Weekly Redfin ingestion + full dbt run",
    schedule_interval="0 3 * * 3",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "weekly"],
) as dag:

    def run_redfin(**context):
        rows = ingest_redfin()
        context["ti"].xcom_push(key="redfin_rows", value=rows)

    def run_audit_log(**context):
        ti = context["ti"]
        redfin_rows = ti.xcom_pull(key="redfin_rows", task_ids="ingest_redfin") or 0
        notes = f"redfin={redfin_rows} rows (0 means ETag unchanged, no new data)"
        write_audit_log(
            dag_id=context["dag"].dag_id,
            run_id=context["run_id"],
            status="success",
            notes=notes,
            freshness_redfin="ok",
        )
        notify_slack_success(
            dag_id=context["dag"].dag_id,
            run_id=context["run_id"],
            notes=notes,
        )

    ingest_redfin_task = PythonOperator(
        task_id="ingest_redfin",
        python_callable=run_redfin,
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=f"cd {DBT_DIR} && uv run dbt run",
    )

    write_audit = PythonOperator(
        task_id="write_audit_log",
        python_callable=run_audit_log,
    )

    ingest_redfin_task >> run_dbt >> write_audit