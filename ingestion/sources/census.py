# ingestion/sources/census.py
#
# Ingests US Census ACS5 demographic data at ZCTA and county level.
# Writes to RAW.CENSUS_ZIP and RAW.CENSUS_COUNTY.
#
# Cadence: Annual — one vintage per year. Pulls last 5 vintages on first run
# so the demographic growth score has historical comparison data from day one.
#
# Watermark: vintage year stored as string in PIPELINE_STATE e.g. "2023"
# No ETag check needed — annual cadence makes redundant fetches acceptable.
#
# Geographic scope:
#   CENSUS_ZIP    — all TN ZCTAs (MSA filter applied in dbt via seed)
#   CENSUS_COUNTY — five MSA counties only (Davidson, Williamson, Rutherford,
#                   Wilson, Sumner)
#
# Note on ZCTA geography: Census API does not support filtering ZCTAs by state
# via the 'in' parameter — ZCTAs don't nest within states in the Census
# hierarchy. We pull all ZCTAs nationally and filter to TN range (37000-38599)
# in Python. National ZCTA response is ~33,000 rows — manageable in memory.
#
# Connection handling: all Snowflake connections go through get_snowflake_conn()
# in utils.py — no direct snowflake.connector calls in this module.

from datetime import datetime, timezone

import httpx
import polars as pl
from loguru import logger
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from ingestion.config import settings
from ingestion.utils import (
    get_snowflake_conn,
    get_watermark,
    update_watermark,
    write_to_snowflake,
)

# ── Constants ──────────────────────────────────────────────────────────────────

# Latest available ACS5 vintage. Update this annually when Census publishes.
LATEST_VINTAGE = 2024

# Pull 5 vintages on first run so demographic growth score has historical data.
ALL_VINTAGES = list(range(2019, LATEST_VINTAGE + 1))  # [2019, 2020, 2021, 2022, 2023]

# Census ACS5 variables
VARIABLES = "NAME,B19013_001E,B17001_002E,B01003_001E"

# Census base URL
CENSUS_BASE_URL = "https://api.census.gov/data/{year}/acs/acs5"

# Tennessee state FIPS
STATE_FIPS = "47"

# Five Nashville MSA counties — state FIPS 47, county FIPS codes
# Full FIPS = STATE_FIPS + county key e.g. "47037"
MSA_COUNTIES = {
    "037": "Davidson",
    "187": "Williamson",
    "149": "Rutherford",
    "189": "Wilson",
    "165": "Sumner",
}

# Census sentinel for suppressed/missing data — replace with None before casting
CENSUS_SENTINEL = "-666666666"

# ACS5 variable columns that need sentinel handling and float casting
VALUE_COLS = ["B19013_001E", "B17001_002E", "B01003_001E"]

# Tennessee ZCTA range — used to filter national ZCTA response to TN only
# ZCTAs don't nest within states in Census geography so we filter by zip range
TN_ZIP_MIN = 37000
TN_ZIP_MAX = 38599

# ── RAW table DDLs ─────────────────────────────────────────────────────────────

CENSUS_ZIP_DDL = """
    CREATE TABLE IF NOT EXISTS RAW.CENSUS_ZIP (
        zcta                     VARCHAR,
        state_fips               VARCHAR,
        median_household_income  FLOAT,
        poverty_count            FLOAT,
        total_population         FLOAT,
        vintage_year             INTEGER,
        ingested_at              TIMESTAMP_NTZ
    )
"""

CENSUS_COUNTY_DDL = """
    CREATE TABLE IF NOT EXISTS RAW.CENSUS_COUNTY (
        county_fips              VARCHAR,
        county_name              VARCHAR,
        state_fips               VARCHAR,
        median_household_income  FLOAT,
        poverty_count            FLOAT,
        total_population         FLOAT,
        vintage_year             INTEGER,
        ingested_at              TIMESTAMP_NTZ
    )
"""

# ── Watermark helpers ──────────────────────────────────────────────────────────

def get_vintages_to_fetch(watermark: str | None) -> list[int]:
    """
    Determine which vintage years need to be fetched.

    If watermark is None (first run) → return all vintages (2019-2023).
    If watermark is set → return only vintages newer than the stored year.

    Args:
        watermark: String vintage year from PIPELINE_STATE e.g. "2022", or None.

    Returns:
        List of integer vintage years to fetch. Empty list = nothing to do.
    """
    if watermark is None:
        logger.info(f"[census] No watermark — fetching all vintages: {ALL_VINTAGES}")
        return ALL_VINTAGES

    last_year = int(watermark)
    to_fetch = [y for y in ALL_VINTAGES if y > last_year]

    if not to_fetch:
        logger.info(f"[census] All vintages up to date (latest: {last_year})")
    else:
        logger.info(f"[census] Fetching new vintages: {to_fetch}")

    return to_fetch


# ── API response helpers ───────────────────────────────────────────────────────

def _parse_census_response(data: list[list]) -> pl.DataFrame:
    """
    Parse Census API JSON response (list of lists) into a Polars DataFrame.

    The first row is the header. Remaining rows are data.
    All values arrive as strings — numeric casting handled by callers.
    """
    headers = data[0]
    rows = data[1:]
    return pl.DataFrame(
        {col: [row[i] for row in rows] for i, col in enumerate(headers)}
    )


def _apply_sentinel_and_cast(df: pl.DataFrame) -> pl.DataFrame:
    """
    Replace Census -666666666 sentinel with None and cast value cols to Float64.

    Census uses -666666666 (as a string in the API response) to indicate
    suppressed or unavailable data. Must be replaced before numeric casting
    or Snowflake will receive string values in float columns.
    """
    return df.with_columns([
        pl.col(c).replace(CENSUS_SENTINEL, None).cast(pl.Float64)
        for c in VALUE_COLS
        if c in df.columns
    ])


# ── Fetch functions ────────────────────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(httpx.TransportError),
    # Only retry on network errors — 400/404s should not be retried
)
def fetch_zip_vintage(year: int) -> pl.DataFrame:
    """
    Fetch ACS5 data for all Tennessee ZCTAs for a given vintage year.

    Pulls all ZCTAs nationally (no state filter supported by Census API for
    ZCTAs) and filters to TN range (37000-38599) in Python.

    Returns empty DataFrame if the vintage is not available (404).
    Retries up to 3 times on network errors only.

    Args:
        year: ACS5 vintage year e.g. 2023

    Returns:
        DataFrame with columns: zcta, state_fips, median_household_income,
        poverty_count, total_population, vintage_year, ingested_at
    """
    url = CENSUS_BASE_URL.format(year=year)

    # No 'in' parameter — ZCTAs don't nest within states in Census geography.
    # We pull national and filter to TN range below.
    params = {
        "get": VARIABLES,
        "for": "zip code tabulation area:*",
        "key": settings.census_api_key,
    }

    logger.info(f"[census/{year}] Fetching ZCTA data (national, then TN filter)...")
    response = httpx.get(url, params=params, timeout=60)

    if response.status_code == 404:
        logger.warning(f"[census/{year}] Vintage not available (404) — skipping")
        return pl.DataFrame()

    response.raise_for_status()
    data = response.json()

    df = _parse_census_response(data)
    logger.info(f"[census/{year}] Parsed {df.shape[0]:,} ZCTAs nationally")

    # Filter to Tennessee ZCTAs by zip code range
    # Cast to Int32 for numeric comparison — ZCTA column is string from API
    df = df.filter(
        pl.col("zip code tabulation area")
        .cast(pl.Int32, strict=False)
        .is_between(TN_ZIP_MIN, TN_ZIP_MAX)
    )
    logger.info(f"[census/{year}] {df.shape[0]} TN ZCTAs after range filter")

    if df.shape[0] == 0:
        logger.warning(f"[census/{year}] No TN ZCTAs found — check zip range filter")
        return pl.DataFrame()

    df = _apply_sentinel_and_cast(df)

    # "zip code tabulation area" is the literal column name returned by the API
    # No "state" column returned when pulling ZCTAs without in=state filter
    df = df.rename({
        "zip code tabulation area": "zcta",
    })

    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    df = df.select([
        pl.col("zcta"),
        pl.lit(STATE_FIPS).alias("state_fips"),  # hardcoded — not in API response
        pl.col("B19013_001E").alias("median_household_income"),
        pl.col("B17001_002E").alias("poverty_count"),
        pl.col("B01003_001E").alias("total_population"),
        pl.lit(year).alias("vintage_year"),
        pl.lit(ingested_at).alias("ingested_at"),
    ])

    logger.info(f"[census/{year}] ZCTA: {df.shape[0]} rows ready to write")
    return df


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(httpx.TransportError),
)
def fetch_county_vintage(year: int) -> pl.DataFrame:
    """
    Fetch ACS5 data for the five Nashville MSA counties for a given vintage year.

    Counties DO support the 'in=state:XX' filter — this is the correct pattern
    for county-level Census API calls.

    Returns empty DataFrame if the vintage is not available (404).

    Args:
        year: ACS5 vintage year e.g. 2023

    Returns:
        DataFrame with columns: county_fips (full 5-digit), county_name,
        state_fips, median_household_income, poverty_count, total_population,
        vintage_year, ingested_at
    """
    url = CENSUS_BASE_URL.format(year=year)
    county_fips_list = ",".join(MSA_COUNTIES.keys())

    params = {
        "get": VARIABLES,
        "for": f"county:{county_fips_list}",
        "in": f"state:{STATE_FIPS}",
        "key": settings.census_api_key,
    }

    logger.info(f"[census/{year}] Fetching county data for MSA counties...")
    response = httpx.get(url, params=params, timeout=30)

    if response.status_code == 404:
        logger.warning(f"[census/{year}] County vintage not available (404) — skipping")
        return pl.DataFrame()

    response.raise_for_status()
    data = response.json()

    df = _parse_census_response(data)
    df = _apply_sentinel_and_cast(df)

    # "county" is the literal column name for the county FIPS code field
    df = df.rename({
        "NAME": "county_name",
        "county": "county_fips_short",
        "state": "state_fips",
    })

    # Construct full 5-digit FIPS: state_fips (2 digits) + county_fips (3 digits)
    # e.g. "47" + "037" = "47037" (Davidson County)
    df = df.with_columns(
        (pl.col("state_fips") + pl.col("county_fips_short")).alias("county_fips")
    )

    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    df = df.select([
        pl.col("county_fips"),
        pl.col("county_name"),
        pl.col("state_fips"),
        pl.col("B19013_001E").alias("median_household_income"),
        pl.col("B17001_002E").alias("poverty_count"),
        pl.col("B01003_001E").alias("total_population"),
        pl.lit(year).alias("vintage_year"),
        pl.lit(ingested_at).alias("ingested_at"),
    ])

    logger.info(f"[census/{year}] County: {df.shape[0]} rows ready to write")
    return df


# ── Snowflake helpers ──────────────────────────────────────────────────────────

def ensure_raw_tables() -> None:
    """Create RAW.CENSUS_ZIP and RAW.CENSUS_COUNTY if they don't exist."""
    with get_snowflake_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(CENSUS_ZIP_DDL)
        cursor.execute(CENSUS_COUNTY_DDL)
    logger.info("[census] RAW.CENSUS_ZIP and RAW.CENSUS_COUNTY confirmed")


def _delete_vintage_years(table: str, years: list[int]) -> None:
    """
    Delete existing rows for given vintage years before insert.

    Ensures idempotency — re-running for the same vintage year won't
    produce duplicate rows. Years passed as Python ints matching
    the INTEGER column type in Snowflake.

    Args:
        table: Table name without schema e.g. 'CENSUS_ZIP'
        years: List of integer vintage years to delete
    """
    placeholders = ", ".join(["%s"] * len(years))
    query = f"DELETE FROM RAW.{table} WHERE vintage_year IN ({placeholders})"

    with get_snowflake_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(query, years)
        conn.commit()

    logger.info(f"[census] Deleted existing rows for vintages {years} from {table}")


# ── Main entry point ───────────────────────────────────────────────────────────

def ingest_census() -> dict[str, int]:
    """
    Full incremental ingest for Census ACS5 at ZCTA and county level.

    Flow:
        1. Ensure both RAW tables exist
        2. Read watermark (latest vintage year already loaded)
        3. Determine which vintages to fetch
        4. For each vintage: fetch ZCTA data and county data
        5. Safety delete existing rows for those vintages (idempotency)
        6. Write to CENSUS_ZIP and CENSUS_COUNTY
        7. Update watermark to latest vintage year loaded

    Returns:
        Dict with keys 'zip' and 'county' — rows written to each table.
        Returns {'zip': 0, 'county': 0} if all vintages are up to date.

    Note: Returns dict (not int) because two separate tables are written.
    This differs from zillow.py and redfin.py which write to one table.
    """
    ensure_raw_tables()

    watermark = get_watermark("census")
    vintages = get_vintages_to_fetch(watermark)

    if not vintages:
        return {"zip": 0, "county": 0}

    zip_frames = []
    county_frames = []

    for year in vintages:
        try:
            df_zip = fetch_zip_vintage(year)
            if not df_zip.is_empty():
                zip_frames.append(df_zip)
        except Exception as e:
            logger.error(f"[census/{year}] ZCTA fetch failed: {e}")
            raise

        try:
            df_county = fetch_county_vintage(year)
            if not df_county.is_empty():
                county_frames.append(df_county)
        except Exception as e:
            logger.error(f"[census/{year}] County fetch failed: {e}")
            raise

    # Write ZIP data
    zip_rows_written = 0
    if zip_frames:
        df_zip_final = pl.concat(zip_frames)
        years_loaded = df_zip_final["vintage_year"].unique().to_list()
        _delete_vintage_years("CENSUS_ZIP", years_loaded)
        zip_rows_written = write_to_snowflake(
            rows=df_zip_final.rows(),
            table="CENSUS_ZIP",
            columns=df_zip_final.columns,
        )

    # Write county data
    county_rows_written = 0
    if county_frames:
        df_county_final = pl.concat(county_frames)
        years_loaded = df_county_final["vintage_year"].unique().to_list()
        _delete_vintage_years("CENSUS_COUNTY", years_loaded)
        county_rows_written = write_to_snowflake(
            rows=df_county_final.rows(),
            table="CENSUS_COUNTY",
            columns=df_county_final.columns,
        )

    # Update watermark to latest vintage successfully loaded
    # str() because PIPELINE_STATE.watermark_date is VARCHAR
    max_year_loaded = max(vintages)
    update_watermark("census", str(max_year_loaded))

    logger.info(
        f"[census] Ingest complete — "
        f"{zip_rows_written} ZIP rows, {county_rows_written} county rows"
    )

    return {"zip": zip_rows_written, "county": county_rows_written}


# ── CLI entrypoint ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    result = ingest_census()
    logger.info(f"[census] Done — {result}")