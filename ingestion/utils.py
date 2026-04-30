# ingestion/utils.py
#
# Shared utilities for all ingestion source modules.
# Import these rather than reimplementing watermark or connection logic per source.
#
# Connection: always use get_snowflake_conn() — never import snowflake.connector directly
# in source modules.
#
# Watermark pattern:
#   Simple sources (Zillow):  get_watermark() / update_watermark()
#   ETag sources (Redfin):    get_pipeline_state() / update_pipeline_state()

import snowflake.connector
from datetime import datetime, timezone
from loguru import logger

from ingestion.config import settings


# ── Connection ─────────────────────────────────────────────────────────────────

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


# ── Pipeline state — canonical functions ───────────────────────────────────────

def get_pipeline_state(source_name: str) -> dict:
    """
    Read the full pipeline state for a given source from PIPELINE_STATE.

    Returns a dict with keys:
        watermark_date: str | None  — ISO date string e.g. '2024-03-31'
        last_etag:      str | None  — HTTP ETag from last successful fetch

    Returns {'watermark_date': None, 'last_etag': None} if no row found,
    with a warning logged.

    Args:
        source_name: One of 'zillow', 'redfin', 'parcels', 'crime', 'census'

    Usage:
        state = get_pipeline_state("redfin")
        watermark = state["watermark_date"]
        etag = state["last_etag"]
    """
    query = """
        SELECT watermark_date, last_etag
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
        return {"watermark_date": None, "last_etag": None}

    watermark_date, last_etag = row[0], row[1]

    if watermark_date:
        logger.info(
            f"[{source_name}] Watermark: {watermark_date} — incremental load"
        )
    else:
        logger.info(f"[{source_name}] No watermark — full history load")

    return {"watermark_date": watermark_date, "last_etag": last_etag}


def update_pipeline_state(
    source_name: str,
    watermark_date: str | None,
    last_etag: str | None,
) -> None:
    """
    Update both watermark_date and last_etag for a source.

    Should only be called AFTER data has been successfully written to Snowflake.
    Used by sources that track ETags (currently Redfin).

    Args:
        source_name:    One of 'zillow', 'redfin', 'parcels', 'crime', 'census'
        watermark_date: ISO date string of most recent period loaded e.g. '2024-03-31'
        last_etag:      HTTP ETag from the most recent successful file fetch

    Usage:
        update_pipeline_state("redfin", "2024-03-31", '"abc123"')
    """
    query = """
        UPDATE RAW.PIPELINE_STATE
        SET watermark_date = %s,
            last_etag      = %s,
            updated_at     = %s
        WHERE source_name = %s
    """
    updated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    with get_snowflake_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(query, (watermark_date, last_etag, updated_at, source_name))
        conn.commit()

    logger.info(
        f"[{source_name}] Pipeline state updated — "
        f"watermark: {watermark_date}, etag: {last_etag}"
    )


# ── Watermark helpers — backward compatible wrappers ──────────────────────────
# These wrap get/update_pipeline_state for sources that don't use ETags.
# Zillow uses these. Do not change their signatures.

def get_watermark(source_name: str) -> str | None:
    """
    Read the watermark date for a given source.

    Thin wrapper around get_pipeline_state() for sources that don't
    use ETag tracking (Zillow, Census, Parcels, Crime).

    Returns ISO date string or None if no data loaded yet.
    """
    return get_pipeline_state(source_name)["watermark_date"]


def update_watermark(source_name: str, new_period: str) -> None:
    """
    Update only the watermark_date for a source, leaving last_etag untouched.

    Thin wrapper for sources that don't use ETag tracking.
    Uses a targeted UPDATE — does not require a prior read of last_etag.

    Args:
        source_name: One of 'zillow', 'redfin', 'parcels', 'crime', 'census'
        new_period:  ISO date string e.g. '2024-03-31'
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


# ── Write helper ───────────────────────────────────────────────────────────────

def write_to_snowflake(
    rows: list[tuple],
    table: str,
    columns: list[str],
    schema: str = "RAW",
) -> int:
    """
    Write a list of row tuples to a Snowflake table.

    Uses executemany for batch insert efficiency.
    Batches in chunks of 100,000 rows to stay under Snowflake's
    200,000 expression limit per statement.
    Returns the number of rows written.

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
        logger.warning(
            f"write_to_snowflake called with 0 rows for {table} — skipping"
        )
        return 0

    placeholders = ", ".join(["%s"] * len(columns))
    col_list = ", ".join(columns)
    query = f"INSERT INTO {schema}.{table} ({col_list}) VALUES ({placeholders})"

    # Snowflake limit: 200,000 expressions per statement
    # Batch into 100,000-row chunks to stay safely under the limit
    BATCH_SIZE = 100_000
    total_written = 0

    with get_snowflake_conn(schema=schema) as conn:
        cursor = conn.cursor()
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            cursor.executemany(query, batch)
            conn.commit()
            total_written += len(batch)
            logger.info(
                f"Wrote batch {i // BATCH_SIZE + 1}: "
                f"{len(batch):,} rows to {schema}.{table} "
                f"({total_written:,}/{len(rows):,} total)"
            )

    logger.info(f"Wrote {total_written:,} rows to {schema}.{table}")
    return total_written