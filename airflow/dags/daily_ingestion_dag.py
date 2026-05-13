
import json
import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_utils import notify_slack_failure, notify_slack_success, write_audit_log

from ingestion.sources.census import ingest_census
from ingestion.sources.crime import ingest_crime
from ingestion.sources.fred import ingest_fred
from ingestion.sources.permits import ingest_permits
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
    description="Daily ingestion: Census, Crime, Parcels, Permits, FRED + full dbt run",
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

    def run_permits(**context):
        rows = ingest_permits()
        context["ti"].xcom_push(key="permits_rows", value=rows)

    def run_fred(**context):
        rows = ingest_fred()
        context["ti"].xcom_push(key="fred_rows", value=rows)

    def run_dbt_task(**context):
        """
        Run dbt run + dbt test, then parse run_results.json for test counts.

        dbt writes target/run_results.json after every command — it is
        overwritten on each run. Parsing AFTER dbt test captures test results,
        not model run results.

        Test counts are pushed to XCom so write_audit_log can record them
        in PIPELINE_AUDIT.dbt_tests_run / dbt_tests_passed.
        """
        # Step 1 — rebuild all models
        subprocess.run(
            ["uv", "run", "dbt", "run"],
            cwd=DBT_DIR,
            check=True,
        )

        # Step 2 — run all data tests (DAG fails here if any test fails)
        subprocess.run(
            ["uv", "run", "dbt", "test"],
            cwd=DBT_DIR,
            check=True,
        )

        # Step 3 — parse run_results.json written by dbt test
        # Status values: "pass", "fail", "warn", "error"
        results_path = f"{DBT_DIR}/target/run_results.json"
        try:
            with open(results_path) as f:
                run_results = json.load(f)
            results = run_results.get("results", [])
            tests_run    = len(results)
            tests_passed = sum(1 for r in results if r.get("status") == "pass")
        except Exception:
            tests_run    = 0
            tests_passed = 0

        context["ti"].xcom_push(key="dbt_tests_run",    value=tests_run)
        context["ti"].xcom_push(key="dbt_tests_passed", value=tests_passed)

    def run_audit_log(**context):
        ti = context["ti"]
        census_rows      = ti.xcom_pull(key="census_rows",      task_ids="ingest_census")  or 0
        crime_rows       = ti.xcom_pull(key="crime_rows",       task_ids="ingest_crime")   or 0
        parcels_rows     = ti.xcom_pull(key="parcels_rows",     task_ids="ingest_parcels") or 0
        permits_rows     = ti.xcom_pull(key="permits_rows",     task_ids="ingest_permits") or 0
        fred_rows        = ti.xcom_pull(key="fred_rows",        task_ids="ingest_fred")    or 0
        dbt_tests_run    = ti.xcom_pull(key="dbt_tests_run",    task_ids="run_dbt")        or 0
        dbt_tests_passed = ti.xcom_pull(key="dbt_tests_passed", task_ids="run_dbt")        or 0

        notes = (
            f"census={census_rows} rows, "
            f"crime={crime_rows} rows, "
            f"parcels={parcels_rows} rows, "
            f"permits={permits_rows} rows, "
            f"fred={fred_rows} rows (0 means already up to date)"
        )

        write_audit_log(
            dag_id=context["dag"].dag_id,
            run_id=context["run_id"],
            status="success",
            dbt_tests_run=dbt_tests_run,
            dbt_tests_passed=dbt_tests_passed,
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

    ingest_permits_task = PythonOperator(
        task_id="ingest_permits",
        python_callable=run_permits,
    )

    ingest_fred_task = PythonOperator(
        task_id="ingest_fred",
        python_callable=run_fred,
    )

    run_dbt = PythonOperator(
        task_id="run_dbt",
        python_callable=run_dbt_task,
    )

    write_audit = PythonOperator(
        task_id="write_audit_log",
        python_callable=run_audit_log,
    )

    [
        ingest_census_task,
        ingest_crime_task,
        ingest_parcels_task,
        ingest_permits_task,
        ingest_fred_task,
    ] >> run_dbt >> write_audit