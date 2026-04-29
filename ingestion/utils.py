# ingestion/utils.py
#
# Shared utilities for all ingestion source modules.
# Import these rather than reimplementing watermark logic per source.

import snowflake.connector
from datetime import datetime, timezone
from loguru import logger

from ingestion.config import settings


def get_snowflake_conn(schema: str = "RAW") -> snowflake.connector.SnowflakeConnection:
    """
    Return an open Snowflake connection using settings from config.

    Args:
        schema: Target schema. Defaults to RAW for all ingestion writes.
                Pass a different schema if querying MARTS or STAGING.

    Usage:
        with get_snowflake_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ...")
    """
    kwargs = settings.snowflake_connect_kwargs()
    kwargs["schema"] = schema
    return snowflake.connector.connect(**kwargs)


def get_watermark(source_name: str) -> str | None:
    """
    Read the last successfully loaded period for a given source.

    Returns the ISO date string stored in RAW.PIPELINE_STATE,
    or None if no data has been loaded yet (first run).

    Args:
        source_name: One of 'zillow', 'redfin', 'parcels', 'crime', 'census'

    Usage:
        watermark = get_watermark("zillow")
        if watermark:
            df = df.filter(pl.col("period_month") > watermark)
    """
    query = """
        SELECT watermark_date
        FROM RAW.PIPELINE_STATE
        WHERE source_name = %s
    """
    with get_snowflake_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(query, (source_name,))
        row = cursor.fetchone()

    if row is None:
        logger.warning(
            f"No PIPELINE_STATE row found for source '{source_name}'. "
            f"Run the INSERT statement to pre-populate."
        )
        return None

    watermark = row[0]
    if watermark:
        logger.info(f"[{source_name}] Watermark found: {watermark} — incremental load")
    else:
        logger.info(f"[{source_name}] No watermark — full history load")

    return watermark


def update_watermark(source_name: str, new_period: str) -> None:
    """
    Update the watermark for a source after a successful write.

    Should only be called AFTER data has been successfully written
    to Snowflake. Never update the watermark before the write —
    a failed write with an advanced watermark would cause data gaps.

    Args:
        source_name: One of 'zillow', 'redfin', 'parcels', 'crime', 'census'
        new_period:  ISO date string of the most recent period loaded
                     e.g. '2024-03-31'

    Usage:
        update_watermark("zillow", "2024-03-31")
    """
    query = """
        UPDATE RAW.PIPELINE_STATE
        SET watermark_date = %s,
            updated_at     = %s
        WHERE source_name = %s
    """
    updated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    with get_snowflake_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(query, (new_period, updated_at, source_name))
        conn.commit()

    logger.info(f"[{source_name}] Watermark updated to {new_period}")


def write_to_snowflake(
    rows: list[tuple],
    table: str,
    columns: list[str],
    schema: str = "RAW",
) -> int:
    """
    Write a list of row tuples to a Snowflake table.

    Uses executemany for batch insert efficiency. Returns the number
    of rows written.

    Args:
        rows:    List of tuples matching the column order in `columns`
        table:   Table name without schema prefix e.g. 'ZILLOW_METRICS'
        columns: Ordered list of column names matching the tuple structure
        schema:  Target schema, defaults to RAW

    Usage:
        rows = df.rows()
        write_to_snowflake(rows, "ZILLOW_METRICS", df.columns)
    """
    if not rows:
        logger.warning(f"write_to_snowflake called with 0 rows for {table} — skipping")
        return 0

    placeholders = ", ".join(["%s"] * len(columns))
    col_list = ", ".join(columns)
    query = f"INSERT INTO {schema}.{table} ({col_list}) VALUES ({placeholders})"

    with get_snowflake_conn(schema=schema) as conn:
        cursor = conn.cursor()
        cursor.executemany(query, rows)
        conn.commit()

    logger.info(f"Wrote {len(rows)} rows to {schema}.{table}")
    return len(rows)