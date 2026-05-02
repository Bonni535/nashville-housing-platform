# airflow/dags/daily_ingestion_dag.py

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from dag_utils import notify_slack_failure, notify_slack_success, write_audit_log

from ingestion.sources.census import ingest_census
from ingestion.sources.crime import ingest_crime
from ingestion.sources.property import ingest_property

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
    dag_id="daily_ingestion_dag",
    description="Daily ingestion: Census, Crime, Parcels + full dbt run",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "daily"],
) as dag:

    def run_census(**context):
        result = ingest_census()
        rows = sum(result.values()) if isinstance(result, dict) else result
        context["ti"].xcom_push(key="census_rows", value=rows)

    def run_crime(**context):
        rows = ingest_crime()
        context["ti"].xcom_push(key="crime_rows", value=rows)

    def run_parcels(**context):
        rows = ingest_property()
        context["ti"].xcom_push(key="parcels_rows", value=rows)

    def run_audit_log(**context):
        ti = context["ti"]
        census_rows = ti.xcom_pull(key="census_rows", task_ids="ingest_census") or 0
        crime_rows = ti.xcom_pull(key="crime_rows", task_ids="ingest_crime") or 0
        parcels_rows = ti.xcom_pull(key="parcels_rows", task_ids="ingest_parcels") or 0
        notes = (
            f"census={census_rows} rows, "
            f"crime={crime_rows} rows, "
            f"parcels={parcels_rows} rows"
        )
        write_audit_log(
            dag_id=context["dag"].dag_id,
            run_id=context["run_id"],
            status="success",
            notes=notes,
            freshness_census="ok",
            freshness_crime="ok",
            freshness_property="ok",
        )
        notify_slack_success(
            dag_id=context["dag"].dag_id,
            run_id=context["run_id"],
            notes=notes,
        )

    ingest_census_task = PythonOperator(
        task_id="ingest_census",
        python_callable=run_census,
    )

    ingest_crime_task = PythonOperator(
        task_id="ingest_crime",
        python_callable=run_crime,
    )

    ingest_parcels_task = PythonOperator(
        task_id="ingest_parcels",
        python_callable=run_parcels,
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=f"cd {DBT_DIR} && uv run dbt run",
    )

    write_audit = PythonOperator(
        task_id="write_audit_log",
        python_callable=run_audit_log,
    )

    [ingest_census_task, ingest_crime_task, ingest_parcels_task] >> run_dbt >> write_audit