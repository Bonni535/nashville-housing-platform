# airflow/dags/dag_utils.py

import os
import httpx
import snowflake.connector


def get_slack_webhook() -> str | None:
    return os.environ.get("SLACK_WEBHOOK_URL") or None


def notify_slack_failure(context: dict) -> None:
    url = get_slack_webhook()
    if not url:
        return
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    httpx.post(url, json={
        "text": (
            f":red_circle: *{dag_id}* failed at task `{task_id}`\n"
            f"Run: `{run_id}`"
        )
    }, timeout=10)


def notify_slack_success(dag_id: str, run_id: str, notes: str) -> None:
    url = get_slack_webhook()
    if not url:
        return
    httpx.post(url, json={
        "text": (
            f":white_check_mark: *{dag_id}* succeeded\n"
            f"Run: `{run_id}`\n{notes}"
        )
    }, timeout=10)


def write_audit_log(
    dag_id: str,
    run_id: str,
    status: str,
    notes: str = None,
    dbt_tests_run: int = 0,
    dbt_tests_passed: int = 0,
    freshness_redfin: str = None,
    freshness_zillow: str = None,
    freshness_property: str = None,
    freshness_crime: str = None,
    freshness_census: str = None,
) -> None:
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "HOUSING_PIPELINE_ROLE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "HOUSING_PIPELINE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "HOUSING_PIPELINE"),
        schema="RAW",
    )
    try:
        conn.cursor().execute("""
            INSERT INTO RAW.PIPELINE_AUDIT (
                dag_id, run_id, status, notes,
                dbt_tests_run, dbt_tests_passed,
                freshness_redfin, freshness_zillow,
                freshness_property, freshness_crime, freshness_census
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s, %s, %s
            )
        """, (
            dag_id, run_id, status, notes,
            dbt_tests_run, dbt_tests_passed,
            freshness_redfin, freshness_zillow,
            freshness_property, freshness_crime, freshness_census,
        ))
    finally:
        conn.close()