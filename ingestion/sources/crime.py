# ingestion/sources/crime.py
#
# Ingests Nashville Police Department crime incidents from the Nashville
# Open Data ArcGIS FeatureServer.
#
# NOTE: Nashville migrated from Socrata to ArcGIS FeatureServer in 2025/2026.
# Original Socrata endpoint (2u6v-ujjs) redirects to hub.arcgis.com/legacy.
# This module uses the current ArcGIS FeatureServer endpoint.
#
# Cadence: Daily incremental with 30-day lookback window to catch
# late-reported incidents. Crime data is often entered days or weeks
# after occurrence date.
#
# Watermark source: RAW.PIPELINE_STATE where source_name = 'crime'
# Watermark format: ISO datetime string e.g. '2024-03-15T00:00:00'
#
# Key ArcGIS details:
#   - Incident_Occurred is esriFieldTypeDate — returned as Unix milliseconds
#   - maxRecordCount=2000 hard server cap — cannot be overridden
#   - returnGeometry=false set but server enforces 2,000 cap regardless
#   - TIMESTAMP 'YYYY-MM-DD HH:MM:SS' syntax for date WHERE clauses
#   - feature["attributes"] unwrapping required
#   - 304 Not Modified can occur under heavy pagination — retried with backoff
#
# Connection handling: all Snowflake connections go through get_snowflake_conn()
# in utils.py — no direct snowflake.connector calls in this module.

from datetime import datetime, timedelta, timezone

import httpx
import polars as pl
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ingestion.utils import (
    get_snowflake_conn,
    get_watermark,
    update_watermark,
    write_to_snowflake,
)

# ── Constants ──────────────────────────────────────────────────────────────────

ARCGIS_URL = (
    "https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services"
    "/Metro_Nashville_Police_Department_Incidents_view/FeatureServer/0/query"
)

# Fields to request — explicit, no wildcards
OUT_FIELDS = "Incident_Occurred,Offense_Description,ZIP_Code"

# Server hard cap — maxRecordCount=2000 cannot be overridden
PAGE_SIZE = 2000

# Lookback window — re-fetch last 30 days on every incremental run
# to catch incidents entered after their occurrence date.
LOOKBACK_DAYS = 30

# ── RAW table DDL ──────────────────────────────────────────────────────────────

RAW_TABLE_DDL = """
    CREATE TABLE IF NOT EXISTS RAW.CRIME_INCIDENTS (
        incident_occurred  TIMESTAMP_NTZ,
        incident_type      VARCHAR,
        zip_code           VARCHAR,
        ingested_at        TIMESTAMP_NTZ
    )
"""

# ── Custom exceptions ──────────────────────────────────────────────────────────

class RateLimitError(Exception):
    """Raised when ArcGIS returns a 429 Too Many Requests response."""
    pass


class NotModifiedError(Exception):
    """Raised on 304 responses — retryable transient caching issue."""
    pass


# ── Watermark helpers ──────────────────────────────────────────────────────────

def get_fetch_from_datetime(watermark: str | None) -> str:
    """
    Compute the datetime to use in the ArcGIS WHERE clause.

    If watermark is None (first run) → return '2010-01-01T00:00:00'
    to pull full available history.

    If watermark is set → subtract 30-day lookback to catch late-reported
    incidents. Combined with idempotency delete, overlapping records
    are replaced cleanly.

    Args:
        watermark: ISO datetime string from PIPELINE_STATE e.g.
                   '2024-03-15T00:00:00', or None.

    Returns:
        ISO datetime string for ArcGIS TIMESTAMP WHERE clause.
    """
    if watermark is None:
        logger.info("[crime] No watermark — fetching full history from 2010-01-01")
        return "2010-01-01T00:00:00"

    watermark_dt = datetime.strptime(watermark, "%Y-%m-%dT%H:%M:%S")
    fetch_from = watermark_dt - timedelta(days=LOOKBACK_DAYS)
    fetch_from_str = fetch_from.strftime("%Y-%m-%dT%H:%M:%S")

    logger.info(
        f"[crime] Watermark: {watermark} — "
        f"fetching from {fetch_from_str} (30-day lookback)"
    )
    return fetch_from_str


# ── ArcGIS fetch ───────────────────────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=4, max=60),
    retry=retry_if_exception_type((httpx.TransportError, RateLimitError, NotModifiedError)),
)
def fetch_page(fetch_from: str, offset: int) -> list[dict]:
    """
    Fetch a single page of crime incidents from the ArcGIS FeatureServer.

    Uses TIMESTAMP 'YYYY-MM-DD HH:MM:SS' syntax for date filter.
    Cache-Control headers prevent 304 Not Modified responses under
    heavy pagination load. 304s are retried with exponential backoff.

    Args:
        fetch_from: ISO datetime string lower bound e.g. '2024-01-01T00:00:00'
        offset:     resultOffset for pagination (0, 2000, 4000, ...)

    Returns:
        List of flat attribute dicts. Empty list if no results.

    Raises:
        NotModifiedError: On 304 response — triggers tenacity retry.
        RateLimitError:   On 429 response — triggers tenacity retry.
        ValueError:       On ArcGIS error response body.
    """
    fetch_from_ts = fetch_from.replace("T", " ")

    params = {
        "where": f"Incident_Occurred >= TIMESTAMP '{fetch_from_ts}'",
        "outFields": OUT_FIELDS,
        "returnGeometry": "false",
        "f": "json",
        "resultOffset": offset,
        "resultRecordCount": PAGE_SIZE,
        "orderByFields": "Incident_Occurred ASC",
        "useStandardizedQueries": "true",
    }

    # Cache-Control headers prevent ArcGIS returning 304 under heavy pagination
    headers = {"Cache-Control": "no-cache", "Pragma": "no-cache"}

    response = httpx.get(ARCGIS_URL, params=params, headers=headers, timeout=60)

    if response.status_code == 304:
        logger.warning(f"[crime] 304 Not Modified at offset {offset} — retrying with backoff")
        raise NotModifiedError("304 Not Modified")

    if response.status_code == 429:
        logger.warning("[crime] Rate limited (429) — retrying with backoff")
        raise RateLimitError("Rate limit hit")

    response.raise_for_status()
    data = response.json()

    if "error" in data:
        raise ValueError(
            f"[crime] ArcGIS error: {data['error']} — check WHERE clause"
        )

    features = data.get("features", [])
    return [f["attributes"] for f in features]


def fetch_all_pages(fetch_from: str) -> pl.DataFrame:
    """
    Paginate through all ArcGIS results for the given datetime range.

    Fetches pages of PAGE_SIZE until a page returns fewer records,
    indicating the last page has been reached.

    Args:
        fetch_from: ISO datetime string lower bound e.g. '2024-01-01T00:00:00'

    Returns:
        Polars DataFrame with all raw records across all pages.
        Returns empty DataFrame if no records found.
    """
    all_records = []
    offset = 0
    page = 1

    while True:
        logger.info(f"[crime] Fetching page {page} (offset={offset:,})...")
        records = fetch_page(fetch_from, offset)

        all_records.extend(records)
        logger.info(
            f"[crime] Page {page}: {len(records):,} records "
            f"(total so far: {len(all_records):,})"
        )

        if len(records) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        page += 1

    if not all_records:
        logger.warning("[crime] No records returned")
        return pl.DataFrame()

    logger.info(
        f"[crime] Fetched {len(all_records):,} total records "
        f"across {page} pages"
    )
    return pl.DataFrame(all_records)


# ── Transform ──────────────────────────────────────────────────────────────────

def transform_crime(df: pl.DataFrame) -> pl.DataFrame:
    """
    Clean and cast the raw ArcGIS response DataFrame.

    Steps:
        1. Guard against empty DataFrame
        2. Drop rows where ZIP_Code is null or empty
        3. Cast ZIP_Code from float string — strict=False handles 'UNK' sentinel
        4. Drop rows where ZIP_Code became null after cast
        5. Drop rows where Incident_Occurred is null
        6. Convert Incident_Occurred from Unix milliseconds to Datetime
        7. Add ingested_at timestamp
        8. Select and rename final columns to snake_case

    Args:
        df: Raw DataFrame from fetch_all_pages()

    Returns:
        Cleaned DataFrame ready for Snowflake write.
    """
    if df.is_empty():
        logger.warning("[crime] transform_crime called with empty DataFrame")
        return df

    before = df.shape[0]

    # Drop rows with null or empty ZIP_Code
    df = df.filter(
        pl.col("ZIP_Code").is_not_null() & (pl.col("ZIP_Code") != "")
    )
    dropped_zip = before - df.shape[0]
    if dropped_zip > 0:
        logger.info(
            f"[crime] Dropped {dropped_zip:,} rows with null/empty ZIP_Code "
            f"({dropped_zip/before*100:.1f}% of page)"
        )

    # ZIP_Code arrives as float string e.g. '37013.0' — strip decimal
    # strict=False returns null for 'UNK' and other non-numeric sentinels
    df = df.with_columns(
        pl.col("ZIP_Code")
        .cast(pl.Float64, strict=False)
        .cast(pl.Int64, strict=False)
        .cast(pl.Utf8)
        .alias("ZIP_Code")
    )

    # Drop rows where ZIP_Code became null after cast (e.g. 'UNK' sentinel)
    before_cast = df.shape[0]
    df = df.drop_nulls(subset=["ZIP_Code"])
    dropped_cast = before_cast - df.shape[0]
    if dropped_cast > 0:
        logger.info(
            f"[crime] Dropped {dropped_cast:,} rows with non-numeric ZIP_Code "
            f"(e.g. 'UNK' sentinel)"
        )

    # Drop rows with null Incident_Occurred
    before_dt = df.shape[0]
    df = df.drop_nulls(subset=["Incident_Occurred"])
    dropped_dt = before_dt - df.shape[0]
    if dropped_dt > 0:
        logger.info(
            f"[crime] Dropped {dropped_dt:,} rows with null Incident_Occurred"
        )

    if df.is_empty():
        logger.warning("[crime] Empty after null drops — nothing to write")
        return df

    # Convert Incident_Occurred from Unix milliseconds to Datetime
    df = df.with_columns(
        pl.from_epoch(
            (pl.col("Incident_Occurred") / 1000).cast(pl.Int64),
            time_unit="s"
        ).alias("Incident_Occurred")
    )

    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    df = df.select([
        pl.col("Incident_Occurred").alias("incident_occurred"),
        pl.col("Offense_Description").alias("incident_type"),
        pl.col("ZIP_Code").alias("zip_code"),
        pl.lit(ingested_at).alias("ingested_at"),
    ])

    logger.info(
        f"[crime] {df.shape[0]:,} rows after transform "
        f"({df.shape[0]/before*100:.1f}% of input retained)"
    )
    return df


# ── Snowflake helpers ──────────────────────────────────────────────────────────

def ensure_raw_table() -> None:
    """Create RAW.CRIME_INCIDENTS if it doesn't exist."""
    with get_snowflake_conn() as conn:
        conn.cursor().execute(RAW_TABLE_DDL)
    logger.info("[crime] RAW.CRIME_INCIDENTS confirmed")


def _delete_from_datetime(fetch_from: str) -> None:
    """
    Delete existing rows for the fetched datetime range before insert.

    Removes all rows where incident_occurred >= fetch_from. Handles
    the 30-day lookback overlap — records already loaded for dates
    in the lookback window are replaced with fresh data.

    Args:
        fetch_from: ISO datetime string lower bound e.g. '2024-01-01T00:00:00'
    """
    query = "DELETE FROM RAW.CRIME_INCIDENTS WHERE incident_occurred >= %s"

    with get_snowflake_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(query, (fetch_from,))
        conn.commit()

    logger.info(
        f"[crime] Deleted existing rows where incident_occurred >= {fetch_from} "
        f"(idempotency + lookback overlap)"
    )


# ── Main entry point ───────────────────────────────────────────────────────────

def ingest_crime() -> int:
    """
    Full incremental ingest for Nashville Police Department crime incidents.

    Flow:
        1. Ensure RAW.CRIME_INCIDENTS exists
        2. Read watermark from PIPELINE_STATE
        3. Compute fetch_from with 30-day lookback
        4. Paginate through ArcGIS FeatureServer results
        5. Transform — convert Unix ms datetime, drop null/invalid zips
        6. Safety delete rows >= fetch_from (idempotency + lookback)
        7. Write to Snowflake
        8. Update watermark to MAX(incident_occurred) in loaded data

    Returns:
        Total rows written.
    """
    ensure_raw_table()

    watermark = get_watermark("crime")
    fetch_from = get_fetch_from_datetime(watermark)

    df_raw = fetch_all_pages(fetch_from)

    if df_raw.is_empty():
        logger.info("[crime] No records fetched — nothing to write")
        return 0

    df = transform_crime(df_raw)

    if df.is_empty():
        logger.info("[crime] Empty after transform — nothing to write")
        return 0

    new_watermark = (
        df["incident_occurred"]
        .max()
        .strftime("%Y-%m-%dT%H:%M:%S")
    )

    logger.info(
        f"[crime] Writing {df.shape[0]:,} rows "
        f"(incident_occurred range: "
        f"{df['incident_occurred'].min()} -> {new_watermark})"
    )

    _delete_from_datetime(fetch_from)

    rows_written = write_to_snowflake(
        rows=df.rows(),
        table="CRIME_INCIDENTS",
        columns=df.columns,
    )

    update_watermark("crime", new_watermark)

    return rows_written


# ── CLI entrypoint ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    total = ingest_crime()
    logger.info(f"[crime] Ingest complete — {total} rows written")