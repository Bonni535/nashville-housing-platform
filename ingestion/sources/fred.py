# ingestion/sources/fred.py
#
# Ingests 30-year fixed mortgage rate from FRED (Federal Reserve Economic Data).
# Writes to RAW.FRED_MORTGAGE_RATES.
#
# Series: MORTGAGE30US — US weekly average, published every Thursday
# Source: Federal Reserve Bank of St. Louis
# History: pulling from 2000-01-01 to align with Zillow data floor
#
# Cadence: Weekly — incremental on observation_date. Running daily is fine;
# the watermark means it returns 0 rows on days with no new publication.
#
# No pagination needed — FRED returns all matching observations in one call.
# ~1,300 rows from 2000 to present, grows by 1 row per week.
#
# Connection handling: all Snowflake connections through get_snowflake_conn()
# in utils.py — no direct snowflake.connector calls in this module.

from datetime import datetime, timezone

import httpx
import polars as pl
from loguru import logger
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from ingestion.config import settings
from ingestion.utils import (
    get_snowflake_conn,
    get_watermark,
    update_watermark,
    write_to_snowflake,
)

# ── Constants ──────────────────────────────────────────────────────────────────

# FRED series ID for 30-year fixed rate mortgage average
SERIES_ID = "MORTGAGE30US"

# Pull history from this date — aligns with Zillow data floor (2000-01-31)
HISTORY_START = "2000-01-01"

# FRED API base URL
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# FRED uses "." to indicate missing/not-available values — drop before casting
FRED_MISSING_SENTINEL = "."

# ── RAW table DDL ──────────────────────────────────────────────────────────────

FRED_DDL = """
    CREATE TABLE IF NOT EXISTS RAW.FRED_MORTGAGE_RATES (
        observation_date  DATE,
        rate              FLOAT,
        series_id         VARCHAR(50),
        ingested_at       TIMESTAMP_NTZ
    )
"""

# ── Fetch ──────────────────────────────────────────────────────────────────────

def _is_retryable(exc: BaseException) -> bool:
    """Retry on network errors and transient HTTP errors (429, 5xx)."""
    if isinstance(exc, httpx.TransportError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in (429, 500, 502, 503, 504)
    return False


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception(_is_retryable),
)
def fetch_observations(observation_start: str) -> pl.DataFrame:
    """
    Fetch MORTGAGE30US observations from FRED API from observation_start onward.

    FRED returns all matching observations in a single response — no pagination
    needed. Values of "." indicate weeks with no published rate (rare) and are
    dropped before writing to Snowflake.

    Args:
        observation_start: ISO date string e.g. "2024-01-01"

    Returns:
        DataFrame with columns: observation_date, rate, series_id, ingested_at.
        Empty DataFrame if no observations returned.
    """
    params = {
        "series_id": SERIES_ID,
        "api_key": settings.fred_api_key,
        "file_type": "json",
        "observation_start": observation_start,
        "sort_order": "asc",
    }

    logger.info(f"[fred] Fetching {SERIES_ID} from {observation_start}...")
    response = httpx.get(FRED_BASE_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    observations = data.get("observations", [])

    if not observations:
        logger.info("[fred] No new observations returned")
        return pl.DataFrame()

    logger.info(f"[fred] Received {len(observations):,} observations")

    df = pl.DataFrame({
        "observation_date": [obs["date"] for obs in observations],
        "rate_raw":         [obs["value"] for obs in observations],
    })

    # Drop missing value sentinel before casting — "." is not a valid float
    before = df.shape[0]
    df = df.filter(pl.col("rate_raw") != FRED_MISSING_SENTINEL)
    dropped = before - df.shape[0]
    if dropped:
        logger.info(f"[fred] Dropped {dropped} rows with missing sentinel '.'")

    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    df = df.select([
        pl.col("observation_date").str.to_date("%Y-%m-%d"),
        pl.col("rate_raw").cast(pl.Float64).alias("rate"),
        pl.lit(SERIES_ID).alias("series_id"),
        pl.lit(ingested_at).alias("ingested_at"),
    ])

    logger.info(f"[fred] {df.shape[0]} rows ready to write")
    return df


# ── Snowflake helpers ──────────────────────────────────────────────────────────

def ensure_raw_table() -> None:
    """Create RAW.FRED_MORTGAGE_RATES if it doesn't exist."""
    with get_snowflake_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(FRED_DDL)
    logger.info("[fred] RAW.FRED_MORTGAGE_RATES confirmed")


def _delete_from_date(from_date: str) -> None:
    """
    Delete rows on or after from_date before insert.

    Ensures idempotency — re-running for overlapping date ranges won't
    produce duplicate rows.

    Args:
        from_date: ISO date string e.g. "2024-01-01"
    """
    query = "DELETE FROM RAW.FRED_MORTGAGE_RATES WHERE observation_date >= %s"
    with get_snowflake_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(query, (from_date,))
        conn.commit()
    logger.info(f"[fred] Deleted rows where observation_date >= {from_date}")


# ── Main entry point ───────────────────────────────────────────────────────────

def ingest_fred() -> int:
    """
    Full incremental ingest for FRED MORTGAGE30US series.

    Flow:
        1. Ensure RAW.FRED_MORTGAGE_RATES exists
        2. Read watermark (latest observation_date already loaded)
        3. No watermark → full history load from HISTORY_START (2000-01-01)
           Watermark set → incremental fetch from watermark date
        4. Delete existing rows from fetch start date (idempotency)
        5. Write new observations to Snowflake
        6. Update watermark to latest observation_date loaded

    Returns:
        Integer row count written. 0 if already up to date.
    """
    ensure_raw_table()

    watermark = get_watermark("fred")

    if watermark is None:
        fetch_from = HISTORY_START
        logger.info(f"[fred] No watermark — full history load from {HISTORY_START}")
    else:
        fetch_from = watermark
        logger.info(f"[fred] Watermark: {watermark} — incremental load from {fetch_from}")

    df = fetch_observations(fetch_from)

    if df.is_empty():
        logger.info("[fred] No new data — already up to date")
        return 0

    _delete_from_date(fetch_from)

    rows_written = write_to_snowflake(
        rows=df.rows(),
        table="FRED_MORTGAGE_RATES",
        columns=["observation_date", "rate", "series_id", "ingested_at"],
    )

    # df["observation_date"].max() returns a Python date object
    # str() formats it as "YYYY-MM-DD" — matches PIPELINE_STATE VARCHAR format
    latest_date = str(df["observation_date"].max())
    update_watermark("fred", latest_date)

    logger.info(
        f"[fred] Ingest complete — {rows_written} rows written, "
        f"watermark → {latest_date}"
    )
    return rows_written


# ── CLI entrypoint ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    result = ingest_fred()
    logger.info(f"[fred] Done — {result} rows")