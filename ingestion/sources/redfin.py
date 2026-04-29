# ingestion/sources/redfin.py
#
# Ingests Redfin Data Center zip-level market tracker.
# Writes to RAW.REDFIN_METRICS — incremental on PERIOD_END via PIPELINE_STATE.
#
# Cadence: Weekly — data updates every Wednesday. Run via redfin_ingest_dag.py
# on schedule 0 3 * * 3 (Wednesday 3am CT). NOT in loader.py.
#
# Watermark source: RAW.PIPELINE_STATE where source_name = 'redfin'
# ETag tracking: HEAD check before streaming to avoid redundant 1.5GB downloads
#
# Memory strategy: stream to temp file -> gzip.open() -> Polars parse
# Loading .content directly on a 1.5GB file will exhaust Airflow worker memory.

import gzip
import os
import tempfile
from datetime import datetime, timezone

import httpx
import polars as pl
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.utils import (
    get_pipeline_state,
    get_snowflake_conn,
    update_pipeline_state,
    write_to_snowflake,
)

# ── Constants ──────────────────────────────────────────────────────────────────

URL = (
    "https://redfin-public-data.s3.us-west-2.amazonaws.com"
    "/redfin_market_tracker/zip_code_market_tracker.tsv000.gz"
)

# All seven value columns arrive as Utf8 — Redfin uses "NA" as null sentinel.
# These are cast to Float64 after replacing "NA" with None.
VALUE_COLS = [
    "MEDIAN_DOM",
    "INVENTORY",
    "AVG_SALE_TO_LIST",
    "MONTHS_OF_SUPPLY",
    "MEDIAN_SALE_PRICE",
    "HOMES_SOLD",
    "NEW_LISTINGS",
]

# ── RAW table DDL ──────────────────────────────────────────────────────────────

RAW_TABLE_DDL = """
    CREATE TABLE IF NOT EXISTS RAW.REDFIN_METRICS (
        zip_code          VARCHAR,
        state_code        VARCHAR,
        period_end        DATE,
        median_dom        FLOAT,
        inventory         FLOAT,
        avg_sale_to_list  FLOAT,
        months_of_supply  FLOAT,
        median_sale_price FLOAT,
        homes_sold        FLOAT,
        new_listings      FLOAT,
        ingested_at       TIMESTAMP_NTZ
    )
"""

# ── ETag check ─────────────────────────────────────────────────────────────────

def check_for_update(last_etag: str | None) -> tuple[bool, str | None]:
    """
    HEAD request to check if the Redfin file has changed since last run.

    Returns (has_changed, current_etag).

    If the HEAD request fails for any reason, returns (True, None) —
    defaulting to proceeding with the download is safer than silently
    skipping and causing a data gap.

    Args:
        last_etag: ETag stored in PIPELINE_STATE from last successful run,
                   or None if this is the first run.

    Usage:
        has_changed, current_etag = check_for_update(state["last_etag"])
        if not has_changed:
            return 0
    """
    try:
        response = httpx.head(URL, follow_redirects=True, timeout=30)
        response.raise_for_status()
        current_etag = response.headers.get("etag")

        if last_etag is None or current_etag is None:
            logger.info("[redfin] ETag unavailable — proceeding with download")
            return True, current_etag

        if current_etag == last_etag:
            logger.info(
                f"[redfin] ETag unchanged ({current_etag}) — file not updated, skipping"
            )
            return False, current_etag

        logger.info(
            f"[redfin] ETag changed ({last_etag} -> {current_etag}) — downloading"
        )
        return True, current_etag

    except Exception as e:
        logger.warning(
            f"[redfin] HEAD check failed: {e} — defaulting to download"
        )
        return True, None


# ── Download ───────────────────────────────────────────────────────────────────

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=30))
def download_to_tempfile() -> str:
    """
    Stream the Redfin gzipped TSV to a temporary file on disk.

    Returns the path to the temp file. Caller is responsible for
    deleting it — always use in a try/finally block.

    Streams in 64KB chunks — peak memory during download is one chunk,
    not the full 1.5GB file. Retries up to 3 times with exponential
    backoff on network errors.

    Usage:
        tmp_path = download_to_tempfile()
        try:
            df = parse_redfin(tmp_path)
        finally:
            os.unlink(tmp_path)
    """
    tmp = tempfile.NamedTemporaryFile(
        suffix=".tsv000.gz",
        delete=False,
    )
    tmp_path = tmp.name

    try:
        logger.info("[redfin] Streaming file to temp disk...")
        bytes_written = 0

        with httpx.stream("GET", URL, follow_redirects=True, timeout=300) as response:
            response.raise_for_status()
            for chunk in response.iter_bytes(chunk_size=65536):  # 64KB chunks
                tmp.write(chunk)
                bytes_written += len(chunk)

        tmp.close()
        logger.info(
            f"[redfin] Download complete — "
            f"{bytes_written / 1_000_000:.1f}MB written to {tmp_path}"
        )
        return tmp_path

    except Exception:
        tmp.close()
        os.unlink(tmp_path)
        raise


# ── Parse ──────────────────────────────────────────────────────────────────────

def parse_redfin(tmp_path: str) -> pl.DataFrame:
    """
    Parse the Redfin TSV from a local gzipped temp file.

    Steps:
        1. Decompress and parse with Polars (separator=tab)
        2. Filter to STATE_CODE == 'TN' immediately — reduces 6.6M to ~178K rows
        3. Parse zip_code from REGION field ('Zip Code: 37040' -> '37040')
        4. Parse PERIOD_END to Date
        5. Replace 'NA' sentinel with None and cast value cols to Float64
        6. Select and rename final columns
        7. Drop rows where all value columns are null

    Note: infer_schema_length=10000 tells Polars to sample the first 10,000
    rows for schema inference. All value columns will be inferred as Utf8
    due to 'NA' sentinels — this is expected and handled explicitly below.

    Note: MSA zip filter is intentionally left to dbt stg_redfin via seed —
    same pattern as Zillow. RAW contains all TN zips.
    """
    logger.info("[redfin] Parsing from temp file...")

    with gzip.open(tmp_path, "rb") as f:
        df = pl.read_csv(
            f,
            separator="\t",
            infer_schema_length=10000,
            ignore_errors=True,   # malformed rows skipped, not crash
        )

    logger.info(f"[redfin] Parsed {df.shape[0]:,} rows nationally")

    # Filter to Tennessee — STATE_CODE not State (Zillow uses State)
    df = df.filter(pl.col("STATE_CODE") == "TN")
    logger.info(f"[redfin] {df.shape[0]:,} TN rows after state filter")

    if df.shape[0] == 0:
        logger.warning(
            "[redfin] No TN rows found — check STATE_CODE column name"
        )
        return pl.DataFrame()

    # Parse zip_code from REGION field: "Zip Code: 37040" -> "37040"
    # If REGION is malformed or missing the ": " separator, result is null
    df = df.with_columns(
        pl.col("REGION")
        .str.split(": ")
        .list.get(1)
        .alias("zip_code")
    )

    # Drop rows where zip_code parse failed
    before = df.shape[0]
    df = df.drop_nulls(subset=["zip_code"])
    dropped = before - df.shape[0]
    if dropped > 0:
        logger.warning(
            f"[redfin] Dropped {dropped} rows with unparseable REGION values"
        )

    # Parse PERIOD_END to Date
    df = df.with_columns(
        pl.col("PERIOD_END").str.to_date("%Y-%m-%d")
    )

    # Replace "NA" sentinel and cast all value columns to Float64
    # All value cols arrive as Utf8 due to "NA" mixed into numeric fields
    df = df.with_columns([
        pl.col(c).replace("NA", None).cast(pl.Float64)
        for c in VALUE_COLS
        if c in df.columns
    ])

    # Add ingested_at timestamp
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Select and rename final columns — drop REGION, METRO, COUNTY, CITY
    df = df.select([
        pl.col("zip_code"),
        pl.col("STATE_CODE").alias("state_code"),
        pl.col("PERIOD_END").alias("period_end"),
        pl.col("MEDIAN_DOM").alias("median_dom"),
        pl.col("INVENTORY").alias("inventory"),
        pl.col("AVG_SALE_TO_LIST").alias("avg_sale_to_list"),
        pl.col("MONTHS_OF_SUPPLY").alias("months_of_supply"),
        pl.col("MEDIAN_SALE_PRICE").alias("median_sale_price"),
        pl.col("HOMES_SOLD").alias("homes_sold"),
        pl.col("NEW_LISTINGS").alias("new_listings"),
        pl.lit(ingested_at).alias("ingested_at"),
    ])

    # Drop rows where ALL value columns are null — no signal to store
    df = df.filter(
        pl.any_horizontal([pl.col(c).is_not_null() for c in [
            "median_dom", "inventory", "avg_sale_to_list",
            "months_of_supply", "median_sale_price",
            "homes_sold", "new_listings",
        ]])
    )

    logger.info(f"[redfin] {df.shape[0]:,} rows after parse and null filter")
    return df


# ── Watermark filter ───────────────────────────────────────────────────────────

def apply_watermark(
    df: pl.DataFrame,
    watermark: str | None,
) -> pl.DataFrame:
    """
    Filter DataFrame to only rows newer than the watermark.

    If watermark is None (first run), returns the full DataFrame.
    If watermark is set, returns only rows where period_end > watermark.
    """
    if watermark is None:
        logger.info("[redfin] No watermark — loading full TN history")
        return df

    cutoff = pl.lit(watermark).str.to_date("%Y-%m-%d")
    df_new = df.filter(pl.col("period_end") > cutoff)

    logger.info(
        f"[redfin] {df_new.shape[0]:,} new rows after watermark filter "
        f"(cutoff: {watermark})"
    )
    return df_new


# ── Snowflake helpers ──────────────────────────────────────────────────────────

def ensure_raw_table() -> None:
    """Create RAW.REDFIN_METRICS if it doesn't exist."""
    with get_snowflake_conn() as conn:
        conn.cursor().execute(RAW_TABLE_DDL)
    logger.info("[redfin] RAW.REDFIN_METRICS confirmed")


def _delete_periods(periods: list) -> None:
    """
    Delete existing rows for the given periods before insert.

    Ensures idempotency — re-running for the same week won't
    produce duplicate rows in RAW.REDFIN_METRICS.
    """
    placeholders = ", ".join(["%s"] * len(periods))
    query = (
        f"DELETE FROM RAW.REDFIN_METRICS "
        f"WHERE period_end IN ({placeholders})"
    )
    period_strs = [p.strftime("%Y-%m-%d") for p in periods]

    with get_snowflake_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(query, period_strs)
        conn.commit()

    logger.info(
        f"[redfin] Deleted existing rows for {len(periods)} periods (idempotency)"
    )


# ── Main entry point ───────────────────────────────────────────────────────────

def ingest_redfin() -> int:
    """
    Full incremental ingest for Redfin zip-level market tracker.

    Flow:
        1. Ensure RAW.REDFIN_METRICS exists
        2. Read pipeline state (watermark + ETag) from PIPELINE_STATE
        3. HEAD check — if ETag unchanged, file hasn't updated, exit early
        4. Stream file to temp disk (64KB chunks, ~400MB compressed)
        5. Parse from temp file — decompress, filter TN, handle NA sentinel
        6. Apply watermark filter — keep only new periods
        7. Safety delete existing rows for new periods (idempotency)
        8. Write new rows to Snowflake
        9. Update pipeline state (watermark + ETag) after successful write
       10. Clean up temp file in finally block

    Returns:
        Total rows written. Returns 0 if file unchanged or no new periods.
    """
    ensure_raw_table()

    state = get_pipeline_state("redfin")
    watermark = state["watermark_date"]
    last_etag = state["last_etag"]

    # HEAD check — skip 1.5GB download if file hasn't changed
    has_changed, current_etag = check_for_update(last_etag)
    if not has_changed:
        logger.info("[redfin] File unchanged since last run — nothing to do")
        return 0

    tmp_path = None
    try:
        tmp_path = download_to_tempfile()
        df = parse_redfin(tmp_path)

        if df.is_empty():
            logger.warning("[redfin] Empty DataFrame after parse — skipping write")
            return 0

        df_new = apply_watermark(df, watermark)

        if df_new.is_empty():
            logger.info("[redfin] No new periods after watermark filter — up to date")
            # Still update ETag so we don't re-download next run
            update_pipeline_state("redfin", watermark, current_etag)
            return 0

        new_periods = df_new["period_end"].unique().sort()
        new_max = new_periods.max()

        logger.info(
            f"[redfin] Writing {df_new.shape[0]:,} rows "
            f"across {len(new_periods)} new periods "
            f"(latest: {new_max})"
        )

        _delete_periods(new_periods.to_list())

        rows_written = write_to_snowflake(
            rows=df_new.rows(),
            table="REDFIN_METRICS",
            columns=df_new.columns,
        )

        # Update watermark and ETag AFTER successful write
        update_pipeline_state(
            "redfin",
            new_max.strftime("%Y-%m-%d"),
            current_etag,
        )

        return rows_written

    finally:
        # Always clean up temp file — even on error
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)
            logger.info(f"[redfin] Temp file deleted: {tmp_path}")


# ── CLI entrypoint ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    total = ingest_redfin()
    logger.info(f"[redfin] Ingest complete — {total} rows written")