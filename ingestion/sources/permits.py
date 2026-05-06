# ingestion/sources/permits.py
#
# Ingests Nashville building permits from Metro Codes ArcGIS FeatureServer.
# Writes to RAW.BUILDING_PERMITS.
#
# Source: Nashville Open Data — Building Permits Issued
# Endpoint: Building_Permits_Issued_2/FeatureServer/0
# Org ID: HdTo6HJqh92wn4D8 (same as crime incidents)
#
# Cadence: Daily — incremental on Date_Issued.
# Dataset note: Metro Codes maintains a rolling ~3-year window.
#   Records older than ~3 years are dropped from the API automatically.
#   7-day lookback on incremental runs catches late-entered permits.
#
# Max Record Count: 1,000 per page (half of crime endpoint's 2,000 cap).
#   Pagination via resultOffset — same pattern as crime.py.
#
# Date handling: Date_Issued is esriFieldTypeDate — returned as Unix milliseconds.
#   Convert via pl.from_epoch(..., time_unit="ms") — same as crime.py.
#
# Zip handling: Zip_Code arrives as string — no float cast needed (unlike crime).
#
# Connection handling: all Snowflake connections through get_snowflake_conn()
# in utils.py — no direct snowflake.connector calls in this module.

from datetime import datetime, timedelta, timezone

import httpx
import polars as pl
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception,
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

FEATURE_SERVER_URL = (
    "https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services"
    "/Building_Permits_Issued_2/FeatureServer/0/query"
)

# Fields to request from the API — only what we need
OUT_FIELDS = "Permit__,Permit_Type_Description,Date_Issued,ZIP,Const_Cost"

# Max records per page — server enforces 1,000 hard cap
PAGE_SIZE = 1_000

# Lookback window — catches permits entered days after issuance
LOOKBACK_DAYS = 7

# ── RAW table DDL ──────────────────────────────────────────────────────────────

PERMITS_DDL = """
    CREATE TABLE IF NOT EXISTS RAW.BUILDING_PERMITS (
        permit_number      VARCHAR,
        permit_type        VARCHAR,
        date_issued        TIMESTAMP_NTZ,
        zip_code           VARCHAR,
        construction_cost  FLOAT,
        ingested_at        TIMESTAMP_NTZ
    )
"""

# ── Retry predicate ────────────────────────────────────────────────────────────

def _is_retryable(exc: BaseException) -> bool:
    """Retry on network errors and transient HTTP errors (429, 5xx)."""
    if isinstance(exc, httpx.TransportError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in (429, 500, 502, 503, 504)
    return False


# ── Fetch ──────────────────────────────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception(_is_retryable),
)
def _fetch_page(where: str, offset: int) -> list[dict]:
    """
    Fetch one page of permit records from the ArcGIS FeatureServer.

    Args:
        where: SQL WHERE clause e.g. "Date_Issued >= TIMESTAMP '2024-01-01 00:00:00'"
        offset: resultOffset for pagination

    Returns:
        List of feature attribute dicts. Empty list signals end of results.
    """
    params = {
        "where":             where,
        "outFields":         OUT_FIELDS,
        "returnGeometry":    "false",
        "orderByFields":     "Date_Issued ASC",
        "resultOffset":      offset,
        "resultRecordCount": PAGE_SIZE,
        "f":                 "json",
    }

    response = httpx.get(FEATURE_SERVER_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    if "error" in data:
        raise RuntimeError(f"[permits] ArcGIS error: {data['error']}")

    features = data.get("features", [])
    return [f["attributes"] for f in features]


def fetch_all_pages(where: str) -> list[dict]:
    """
    Paginate through all matching permit records using resultOffset.

    Args:
        where: SQL WHERE clause for date filtering

    Returns:
        List of all attribute dicts across all pages.
    """
    all_records: list[dict] = []
    offset = 0
    page = 1

    while True:
        logger.info(f"[permits] Fetching page {page} (offset={offset:,})...")
        records = _fetch_page(where, offset)

        if not records:
            logger.info(f"[permits] No more records — {len(all_records):,} total")
            break

        all_records.extend(records)
        logger.info(
            f"[permits] Page {page}: {len(records):,} records "
            f"(total so far: {len(all_records):,})"
        )

        if len(records) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        page += 1

    return all_records


# ── Transform ──────────────────────────────────────────────────────────────────

def transform_permits(records: list[dict]) -> pl.DataFrame:
    """
    Transform raw ArcGIS attribute dicts into a clean Polars DataFrame.

    Key transforms:
    - Date_Issued: Unix milliseconds → TIMESTAMP_NTZ via pl.from_epoch
    - Zip_Code: string, may need null handling
    - Construction_Cost: float, nullable
    - Rows with null Date_Issued dropped — unusable without a date

    Args:
        records: List of attribute dicts from ArcGIS

    Returns:
        DataFrame ready to write to RAW.BUILDING_PERMITS
    """
    df = pl.DataFrame({
        "Permit__":                [r.get("Permit__")                for r in records],
        "Permit_Type_Description": [r.get("Permit_Type_Description") for r in records],
        "Date_Issued":             [r.get("Date_Issued")             for r in records],
        "ZIP":                     [r.get("ZIP")                     for r in records],
        "Const_Cost":              [r.get("Const_Cost")              for r in records],
    })

    before = df.shape[0]
    df = df.filter(pl.col("Date_Issued").is_not_null())
    dropped = before - df.shape[0]
    if dropped:
        logger.info(f"[permits] Dropped {dropped} rows with null Date_Issued")

    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    df = df.select([
        pl.col("Permit__").alias("permit_number"),
        pl.col("Permit_Type_Description").alias("permit_type"),

        # Date_Issued is Unix milliseconds — same pattern as crime.py
        pl.from_epoch(
            pl.col("Date_Issued").cast(pl.Int64),
            time_unit="ms",
        ).dt.replace_time_zone("UTC")
         .dt.convert_time_zone("America/Chicago")
         .dt.replace_time_zone(None)
         .alias("date_issued"),

        pl.col("ZIP").alias("zip_code"),
        pl.col("Const_Cost").cast(pl.Float64, strict=False).alias("construction_cost"),
        pl.lit(ingested_at).alias("ingested_at"),
    ])

    logger.info(f"[permits] {df.shape[0]} rows after transform")
    return df


# ── Snowflake helpers ──────────────────────────────────────────────────────────

def ensure_raw_table() -> None:
    """Create RAW.BUILDING_PERMITS if it doesn't exist."""
    with get_snowflake_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(PERMITS_DDL)
    logger.info("[permits] RAW.BUILDING_PERMITS confirmed")


def _delete_from_datetime(from_dt: str) -> None:
    """
    Delete rows where date_issued >= from_dt before insert.

    Ensures idempotency — re-running for the same date range won't
    produce duplicate rows.

    Args:
        from_dt: ISO datetime string e.g. "2024-01-01T00:00:00"
    """
    query = "DELETE FROM RAW.BUILDING_PERMITS WHERE date_issued >= %s"
    with get_snowflake_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(query, (from_dt,))
        conn.commit()
    logger.info(f"[permits] Deleted rows where date_issued >= {from_dt}")


# ── Watermark helper ───────────────────────────────────────────────────────────

def get_fetch_from_datetime(watermark: str | None) -> str:
    """
    Determine the fetch start datetime from the watermark.

    No watermark → fetch all available records (rolling ~3-year window).
    Watermark set → fetch from watermark minus LOOKBACK_DAYS.

    The 7-day lookback catches permits where Date_Issued was backdated
    or entered into the system after the fact.

    Args:
        watermark: ISO datetime string from PIPELINE_STATE, or None.

    Returns:
        ISO datetime string to use as Date_Issued filter.
    """
    if watermark is None:
        # Pull full available history — dataset is rolling ~3 years
        # Use a far-back date to capture everything currently in the API
        fetch_from = "2020-01-01T00:00:00"
        logger.info(f"[permits] No watermark — full load from {fetch_from}")
    else:
        watermark_dt = datetime.fromisoformat(watermark)
        fetch_from_dt = watermark_dt - timedelta(days=LOOKBACK_DAYS)
        fetch_from = fetch_from_dt.strftime("%Y-%m-%dT%H:%M:%S")
        logger.info(
            f"[permits] Watermark: {watermark} — "
            f"fetching from {fetch_from} ({LOOKBACK_DAYS}-day lookback)"
        )

    return fetch_from


# ── Main entry point ───────────────────────────────────────────────────────────

def ingest_permits() -> int:
    """
    Full incremental ingest for Nashville building permits.

    Flow:
        1. Ensure RAW.BUILDING_PERMITS exists
        2. Read watermark (latest date_issued already loaded)
        3. Determine fetch start with 7-day lookback
        4. Paginate through ArcGIS FeatureServer in 1,000-record pages
        5. Transform records (date conversion, null handling)
        6. Delete existing rows from fetch start (idempotency)
        7. Write to Snowflake
        8. Update watermark to latest date_issued loaded

    Returns:
        Integer row count written. 0 if no new records.
    """
    ensure_raw_table()

    watermark = get_watermark("permits")
    fetch_from = get_fetch_from_datetime(watermark)

    # ArcGIS TIMESTAMP syntax — same as crime.py
    fetch_from_ts = fetch_from.replace("T", " ")
    where = f"Date_Issued >= TIMESTAMP '{fetch_from_ts}'"

    logger.info(f"[permits] WHERE clause: {where}")

    records = fetch_all_pages(where)

    if not records:
        logger.info("[permits] No new records — already up to date")
        return 0

    df = transform_permits(records)

    if df.is_empty():
        logger.info("[permits] No rows after transform")
        return 0

    _delete_from_datetime(fetch_from)

    rows_written = write_to_snowflake(
        rows=df.rows(),
        table="BUILDING_PERMITS",
        columns=["permit_number", "permit_type", "date_issued",
                 "zip_code", "construction_cost", "ingested_at"],
    )

    latest_dt = df["date_issued"].max()
    update_watermark("permits", str(latest_dt))

    logger.info(
        f"[permits] Ingest complete — {rows_written} rows written, "
        f"watermark → {latest_dt}"
    )
    return rows_written


# ── CLI entrypoint ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    result = ingest_permits()
    logger.info(f"[permits] Done — {result} rows")