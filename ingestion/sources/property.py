# ingestion/sources/property.py
#
# Ingests Nashville property sale transactions from the Nashville Parcels
# ArcGIS MapServer endpoint.
#
# This is a genuine transaction ingest — the Parcels layer exposes SalePrice,
# OwnDate, and a ValidSale arm's-length flag. Filtering to ValidSale='Y'
# excludes foreclosures and family transfers, producing a clean market-rate
# sale dataset.
#
# Cadence: Daily incremental — watermark on OwnDate with a 30-day lookback
# window to catch late-arriving records. Idempotency delete handles any
# overlap between runs cleanly.
#
# Watermark source: RAW.PIPELINE_STATE where source_name = 'parcels'
#
# Key ArcGIS gotchas (documented here for reference):
#   - Date syntax in where clause: DATE 'YYYY-MM-DD' (not a quoted string)
#   - OwnDate returned as Unix milliseconds — divide by 1000 before casting
#   - Results paginated at 10,000 records per request via resultOffset
#   - Field values nested inside feature["attributes"], not at top level
#
# Connection handling: all Snowflake connections go through get_snowflake_conn()
# in utils.py — no direct snowflake.connector calls in this module.

from datetime import datetime, timedelta, timezone

import httpx
import polars as pl
from loguru import logger
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from ingestion.utils import (
    get_snowflake_conn,
    get_watermark,
    update_watermark,
    write_to_snowflake,
)

# ── Constants ──────────────────────────────────────────────────────────────────

ARCGIS_URL = (
    "https://maps.nashville.gov/arcgis/rest/services"
    "/Cadastral/Parcels/MapServer/0/query"
)

# Fields to request from ArcGIS — explicit list, no wildcards
OUT_FIELDS = ",".join([
    "APN",
    "PropZip",
    "LUCode",
    "LUDesc",
    "SalePrice",
    "OwnDate",
    "ValidSale",
    "TotlAppr",
    "TotlAssd",
])

# ArcGIS max records per page
PAGE_SIZE = 10000

# Lookback window — re-fetch the last 30 days on every incremental run
# to catch late-arriving records that were backdated after our last ingest.
# Combined with idempotency delete, duplicates are handled automatically.
LOOKBACK_DAYS = 30

# ── RAW table DDL ──────────────────────────────────────────────────────────────

RAW_TABLE_DDL = """
    CREATE TABLE IF NOT EXISTS RAW.PROPERTY_SALES (
        apn          VARCHAR,
        prop_zip     VARCHAR,
        lu_code      VARCHAR,
        lu_desc      VARCHAR,
        sale_price   FLOAT,
        own_date     DATE,
        valid_sale   VARCHAR,
        totl_appr    FLOAT,
        totl_assd    FLOAT,
        ingested_at  TIMESTAMP_NTZ
    )
"""

# ── Watermark helpers ──────────────────────────────────────────────────────────

def get_fetch_from_date(watermark: str | None) -> str:
    """
    Compute the date to use in the ArcGIS WHERE clause.

    If watermark is None (first run) → return '2000-01-01' to pull full history.
    If watermark is set → subtract 30-day lookback window to catch late arrivals.

    The lookback window means we re-fetch the last 30 days on every run.
    Combined with the idempotency delete in ingest_property(), duplicates
    from the overlap are removed before insert.

    Args:
        watermark: ISO date string from PIPELINE_STATE e.g. '2024-03-15', or None.

    Returns:
        ISO date string to use as the lower bound in the ArcGIS WHERE clause.
    """
    if watermark is None:
        logger.info("[parcels] No watermark — fetching full history from 2000-01-01")
        return "2000-01-01"

    watermark_date = datetime.strptime(watermark, "%Y-%m-%d").date()
    fetch_from = watermark_date - timedelta(days=LOOKBACK_DAYS)
    fetch_from_str = fetch_from.strftime("%Y-%m-%d")

    logger.info(
        f"[parcels] Watermark: {watermark} — "
        f"fetching from {fetch_from_str} (30-day lookback)"
    )
    return fetch_from_str


# ── ArcGIS fetch ───────────────────────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(httpx.TransportError),
)
def fetch_page(where: str, offset: int) -> list[dict]:
    """
    Fetch a single page of results from the ArcGIS MapServer.

    Returns a list of attribute dicts — one per feature.
    ArcGIS nests field values inside feature["attributes"], which this
    function unwraps so callers receive flat dicts.

    Args:
        where:  ArcGIS WHERE clause string
        offset: resultOffset for pagination (0, 10000, 20000, ...)

    Returns:
        List of flat attribute dicts. Empty list if no results.
    """
    params = {
        "where": where,
        "outFields": OUT_FIELDS,
        "f": "json",
        "resultOffset": offset,
        "resultRecordCount": PAGE_SIZE,
    }

    response = httpx.get(ARCGIS_URL, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    # ArcGIS returns an "error" key instead of raising HTTP error codes
    # for some query failures (e.g. malformed where clause)
    if "error" in data:
        raise ValueError(
            f"[parcels] ArcGIS error response: {data['error']} "
            f"— check WHERE clause syntax"
        )

    features = data.get("features", [])

    # Unwrap attributes from nested feature structure
    # Input:  [{"attributes": {"APN": "...", "SalePrice": 250000}}, ...]
    # Output: [{"APN": "...", "SalePrice": 250000}, ...]
    return [f["attributes"] for f in features]


def fetch_all_pages(fetch_from_date: str) -> pl.DataFrame:
    """
    Paginate through all ArcGIS results for the given date range.

    ArcGIS esriFieldTypeDate fields require timestamp string syntax in
    WHERE clauses: timestamp 'YYYY-MM-DD HH:MM:SS'
    Raw milliseconds and DATE 'YYYY-MM-DD' both return errors or zero results.

    IsActive field removed — not present on this layer version.

    Args:
        fetch_from_date: ISO date string lower bound e.g. '2024-01-01'

    Returns:
        Polars DataFrame with all raw records across all pages.
        Returns empty DataFrame if no records found.
    """
    where = (
    f"IsActive='Y' AND ValidSale='Y' "
    f"AND OwnDate >= TIMESTAMP '{fetch_from_date} 00:00:00'"
)

    logger.info(f"[parcels] WHERE clause: {where}")

    all_records = []
    offset = 0
    page = 1

    while True:
        logger.info(f"[parcels] Fetching page {page} (offset={offset})...")
        records = fetch_page(where, offset)

        all_records.extend(records)
        logger.info(
            f"[parcels] Page {page}: {len(records)} records "
            f"(total so far: {len(all_records):,})"
        )

        # Stop when page is less than PAGE_SIZE — we've reached the last page.
        # If total records are an exact multiple of PAGE_SIZE, the next request
        # will return 0 records and we stop there — no records are missed.
        if len(records) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        page += 1

    if not all_records:
        logger.warning("[parcels] No records returned — check WHERE clause")
        return pl.DataFrame()

    logger.info(f"[parcels] Fetched {len(all_records):,} total records across {page} pages")
    return pl.DataFrame(all_records)


# ── Transform ──────────────────────────────────────────────────────────────────

def transform_parcels(df: pl.DataFrame) -> pl.DataFrame:
    """
    Clean and cast the raw ArcGIS response DataFrame.

    Steps:
        1. Drop rows where OwnDate is null — can't cast null Unix ms to Date
        2. Convert OwnDate from Unix milliseconds to Date
           Sequence: / 1000 → cast Int64 → cast Date
           Wrong order produces silent date overflow errors
        3. Cast SalePrice, TotlAppr, TotlAssd to Float64
        4. Cast PropZip, LUCode to string (may arrive as int from ArcGIS)
        5. Add ingested_at timestamp
        6. Select and rename final columns to snake_case

    Args:
        df: Raw DataFrame from fetch_all_pages()

    Returns:
        Cleaned DataFrame ready for Snowflake write.
    """
    if df.is_empty():
        logger.warning("[parcels] transform_parcels called with empty DataFrame — returning early")
        return df
    before = df.shape[0]

    # Drop rows where OwnDate is null before Unix ms conversion
    df = df.drop_nulls(subset=["OwnDate"])
    dropped = before - df.shape[0]
    if dropped > 0:
        logger.warning(f"[parcels] Dropped {dropped} rows with null OwnDate")

    # Convert OwnDate from Unix milliseconds to Date
    # Step 1: divide by 1000 to get Unix seconds (still float)
    # Step 2: cast to Int64 (required by Polars for from_epoch)
    # Step 3: cast to Date
    df = df.with_columns(
        pl.from_epoch(
            (pl.col("OwnDate") / 1000).cast(pl.Int64),
            time_unit="s"
        ).alias("OwnDate")
    )
    # Drop rows where OwnDate is out of Polars' valid date range
    before_range = df.shape[0]
    df = df.drop_nulls(subset=["OwnDate"])
    dropped_range = before_range - df.shape[0]
    if dropped_range > 0:
        logger.warning(f"[parcels] Dropped {dropped_range} rows with out-of-range OwnDate")

    # Cast numeric fields — ArcGIS may return these as int or null
    df = df.with_columns([
        pl.col("SalePrice").cast(pl.Float64),
        pl.col("TotlAppr").cast(pl.Float64),
        pl.col("TotlAssd").cast(pl.Float64),
        # PropZip and LUCode may arrive as int — cast to string
        pl.col("PropZip").cast(pl.Utf8),
        pl.col("LUCode").cast(pl.Utf8),
    ])

    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    df = df.select([
        pl.col("APN").alias("apn"),
        pl.col("PropZip").alias("prop_zip"),
        pl.col("LUCode").alias("lu_code"),
        pl.col("LUDesc").alias("lu_desc"),
        pl.col("SalePrice").alias("sale_price"),
        pl.col("OwnDate").alias("own_date"),
        pl.col("ValidSale").alias("valid_sale"),
        pl.col("TotlAppr").alias("totl_appr"),
        pl.col("TotlAssd").alias("totl_assd"),
        pl.lit(ingested_at).alias("ingested_at"),
    ])

    logger.info(f"[parcels] {df.shape[0]:,} rows after transform")
    return df


# ── Snowflake helpers ──────────────────────────────────────────────────────────

def ensure_raw_table() -> None:
    """Create RAW.PROPERTY_SALES if it doesn't exist."""
    with get_snowflake_conn() as conn:
        conn.cursor().execute(RAW_TABLE_DDL)
    logger.info("[parcels] RAW.PROPERTY_SALES confirmed")


def _delete_date_range(fetch_from_date: str) -> None:
    """
    Delete existing rows for the fetched date range before insert.

    Removes all rows where own_date >= fetch_from_date. This handles
    the 30-day lookback overlap — any records already loaded for dates
    in the lookback window are replaced with fresh data.

    Args:
        fetch_from_date: ISO date string lower bound e.g. '2024-01-01'
    """
    query = "DELETE FROM RAW.PROPERTY_SALES WHERE own_date >= %s"

    with get_snowflake_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(query, (fetch_from_date,))
        conn.commit()

    logger.info(
        f"[parcels] Deleted existing rows where own_date >= {fetch_from_date} "
        f"(idempotency + lookback overlap)"
    )


# ── Main entry point ───────────────────────────────────────────────────────────

def ingest_property() -> int:
    """
    Full incremental ingest for Nashville Parcels property transactions.

    Flow:
        1. Ensure RAW.PROPERTY_SALES exists
        2. Read watermark from PIPELINE_STATE
        3. Compute fetch_from_date with 30-day lookback
        4. Paginate through ArcGIS results for the date range
        5. Transform — cast OwnDate from Unix ms, cast numeric fields
        6. Safety delete rows >= fetch_from_date (idempotency + lookback)
        7. Write to Snowflake
        8. Update watermark to most recent own_date in the loaded data

    Returns:
        Total rows written.

    Note on watermark update: we set the watermark to MAX(own_date) in the
    loaded data — not to today's date. This ensures we never advance the
    watermark past actual data, which could create gaps if the ArcGIS
    layer has a publication lag.
    """
    ensure_raw_table()

    watermark = get_watermark("parcels")
    fetch_from_date = get_fetch_from_date(watermark)

    df_raw = fetch_all_pages(fetch_from_date)

    if df_raw.is_empty():
        logger.info("[parcels] No records fetched — nothing to write")
        return 0

    df = transform_parcels(df_raw)
    

    if df.is_empty():
        logger.info("[parcels] Empty after transform — nothing to write")
        return 0

    # New watermark = most recent own_date in the data
    # Using MAX(own_date) rather than today's date prevents watermark
    # advancing past actual data if ArcGIS has a publication lag
    new_watermark = df["own_date"].max().strftime("%Y-%m-%d")

    logger.info(
            f"[parcels] Writing {df.shape[0]:,} rows "
            f"(own_date range: {df['own_date'].min()} → {new_watermark})"
        )

    _delete_date_range(fetch_from_date)

    rows_written = write_to_snowflake(
        rows=df.rows(),
        table="PROPERTY_SALES",
        columns=df.columns,
    )

    # Update watermark AFTER successful write
    update_watermark("parcels", new_watermark)

    return rows_written


# ── CLI entrypoint ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    total = ingest_property()
    logger.info(f"[parcels] Ingest complete — {total} rows written")