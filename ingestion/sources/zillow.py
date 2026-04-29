# ingestion/sources/zillow.py
#
# Ingests Zillow Research Data CSVs (ZHVI, ZORI, ZHVF) at zip code level.
# Writes to RAW.ZILLOW_METRICS — incremental on period_month via PIPELINE_STATE.
#
# Cadence: Monthly (triggered on Zillow Research Data release)
# Watermark source: RAW.PIPELINE_STATE where source_name = 'zillow'
#
# Connection handling: all Snowflake connections go through get_snowflake_conn()
# in utils.py — no direct snowflake.connector calls in this module.

from datetime import datetime, timezone
import io

import httpx
import polars as pl
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.utils import (
    get_snowflake_conn,
    get_watermark,
    update_watermark,
    write_to_snowflake,
)

# ── Source URLs ────────────────────────────────────────────────────────────────
# Zillow publishes zip-level ZHVI, ZORI, ZHVF as public CSVs.
# URLs are stable — Zillow overwrites the same file on each monthly release.

ZILLOW_SOURCES = {
    "ZHVI": (
        "https://files.zillowstatic.com/research/public_csvs/zhvi/"
        "Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
    ),
    "ZORI": (
        "https://files.zillowstatic.com/research/public_csvs/zori/"
        "Zip_zori_uc_sfrcondomfr_sm_month.csv"
    ),
    "ZHVF": (
        "https://files.zillowstatic.com/research/public_csvs/zhvf_growth/"
        "Zip_zhvf_growth_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
    ),
}

# Columns that identify a zip — everything else is treated as a date column.
# RegionID, SizeRank, RegionType, StateName, City are in this set but dropped
# before the melt — we only carry RegionName, State, Metro, CountyName forward.
IDENTITY_COLS = {
    "RegionID", "SizeRank", "RegionName", "RegionType",
    "StateName", "State", "City", "Metro", "CountyName",
}

# ── RAW table DDL ──────────────────────────────────────────────────────────────

RAW_TABLE_DDL = """
    CREATE TABLE IF NOT EXISTS RAW.ZILLOW_METRICS (
        zip_code      VARCHAR,
        state         VARCHAR,
        metro         VARCHAR,
        county_name   VARCHAR,
        period_month  DATE,
        value         FLOAT,
        metric_type   VARCHAR,
        ingested_at   TIMESTAMP_NTZ
    )
"""

# ── HTTP ───────────────────────────────────────────────────────────────────────

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_zillow_csv(metric_type: str, url: str) -> pl.DataFrame:
    """
    Fetch a single Zillow CSV and return as a Polars DataFrame.

    Uses httpx.get() — Zillow CSVs are a few MB each, no streaming needed.
    Retries up to 3 times with exponential backoff on network errors.
    Only the HTTP fetch is retried — transform and write failures are not.
    """
    logger.info(f"[zillow/{metric_type}] Fetching from {url}")

    response = httpx.get(url, timeout=60, follow_redirects=True)
    response.raise_for_status()

    df = pl.read_csv(io.BytesIO(response.content))
    logger.info(
        f"[zillow/{metric_type}] Fetched {df.shape[0]} rows, "
        f"{df.shape[1]} columns"
    )

    return df


# ── Transform ──────────────────────────────────────────────────────────────────

def melt_zillow(df: pl.DataFrame, metric_type: str) -> pl.DataFrame:
    """
    Transform Zillow wide-format CSV to long format.

    Input:  One row per zip, one column per month (e.g. '2024-01-31')
    Output: One row per zip per month with columns:
            zip_code, state, metro, county_name, period_month,
            value, metric_type, ingested_at

    Steps:
        1. Filter to Tennessee — MSA zip filter applied later in dbt via seed
        2. Detect date columns by shape (YYYY-MM-DD), raise if none found
        3. Select only needed identity cols + date cols before melt
        4. Unpivot wide -> long
        5. Cast types, add metric_type and ingested_at
        6. Drop null values (zip/month combos with no data)

    Note: ingested_at is stamped once per melt call, so all rows for a given
    metric share the same timestamp. ZHVI, ZORI, ZHVF will differ slightly
    depending on fetch duration — this is intentional and acceptable.
    """
    df = df.filter(pl.col("State") == "TN")
    logger.info(f"[zillow/{metric_type}] {df.shape[0]} TN rows after state filter")

    if df.shape[0] == 0:
        logger.warning(
            f"[zillow/{metric_type}] No TN rows found — "
            f"check whether 'State' column name has changed"
        )
        return pl.DataFrame()

    # Detect date columns — any column not in IDENTITY_COLS matching YYYY-MM-DD
    date_cols = [
        col for col in df.columns
        if col not in IDENTITY_COLS
        and len(col) == 10
        and col[4] == "-"
        and col[7] == "-"
    ]

    if not date_cols:
        raise ValueError(
            f"[zillow/{metric_type}] No date columns detected — "
            f"Zillow may have changed their CSV schema. "
            f"Columns found: {df.columns}"
        )

    logger.info(
        f"[zillow/{metric_type}] Melting {len(date_cols)} date columns "
        f"({date_cols[0]} -> {date_cols[-1]})"
    )

    # Select only the columns we need before melting —
    # RegionID, SizeRank, RegionType, StateName, City dropped intentionally
    df_long = df.select(
        ["RegionName", "State", "Metro", "CountyName"] + date_cols
    ).unpivot(
        index=["RegionName", "State", "Metro", "CountyName"],
        on=date_cols,
        variable_name="period_month",
        value_name="value",
    )

    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    df_long = df_long.with_columns([
        pl.col("RegionName").cast(pl.Utf8).alias("zip_code"),
        pl.col("period_month").str.to_date("%Y-%m-%d"),
        pl.col("value").cast(pl.Float64),
        pl.lit(metric_type).alias("metric_type"),
        pl.lit(ingested_at).alias("ingested_at"),
    ]).select([
        "zip_code",
        "State",
        "Metro",
        "CountyName",
        "period_month",
        "value",
        "metric_type",
        "ingested_at",
    ]).rename({
        "State": "state",
        "Metro": "metro",
        "CountyName": "county_name",
    })

    df_long = df_long.drop_nulls(subset=["value"])

    logger.info(
        f"[zillow/{metric_type}] {df_long.shape[0]} rows after melt and null drop"
    )

    return df_long


def apply_watermark(
    df: pl.DataFrame,
    watermark: str | None,
    metric_type: str,
) -> pl.DataFrame:
    """
    Filter DataFrame to only rows newer than the watermark.

    If watermark is None (first run), returns the full DataFrame.
    If watermark is set, returns only rows where period_month > watermark.
    """
    if watermark is None:
        logger.info(f"[zillow/{metric_type}] No watermark — loading full history")
        return df

    cutoff = pl.lit(watermark).str.to_date("%Y-%m-%d")
    df_new = df.filter(pl.col("period_month") > cutoff)

    logger.info(
        f"[zillow/{metric_type}] {df_new.shape[0]} new rows after watermark filter "
        f"(cutoff: {watermark})"
    )

    return df_new


# ── Snowflake helpers ──────────────────────────────────────────────────────────

def ensure_raw_table() -> None:
    """Create RAW.ZILLOW_METRICS if it doesn't exist."""
    with get_snowflake_conn() as conn:
        conn.cursor().execute(RAW_TABLE_DDL)
    logger.info("[zillow] RAW.ZILLOW_METRICS confirmed")


def _delete_periods(periods: list) -> None:
    """
    Delete any existing rows for the given periods before insert.

    Ensures idempotency — re-running the pipeline for the same month
    won't produce duplicate rows in RAW.ZILLOW_METRICS.
    Called immediately before write_to_snowflake in ingest_zillow.
    """
    placeholders = ", ".join(["%s"] * len(periods))
    query = (
        f"DELETE FROM RAW.ZILLOW_METRICS "
        f"WHERE period_month IN ({placeholders})"
    )
    # Convert Polars Date objects to ISO strings for Snowflake
    period_strs = [p.strftime("%Y-%m-%d") for p in periods]

    with get_snowflake_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(query, period_strs)
        conn.commit()

    logger.info(
        f"[zillow] Deleted existing rows for {len(periods)} periods (idempotency)"
    )


# ── Main entry point ───────────────────────────────────────────────────────────

def ingest_zillow() -> int:
    """
    Full incremental ingest for all three Zillow metrics.

    Flow:
        1. Ensure RAW.ZILLOW_METRICS exists
        2. Read current watermark from PIPELINE_STATE
        3. For each metric (ZHVI, ZORI, ZHVF):
            a. Fetch CSV
            b. Melt wide -> long
            c. Filter to new periods only
        4. Concatenate all three metrics
        5. Safety delete existing rows for new periods (idempotency)
        6. Write new rows to Snowflake
        7. Update watermark to most recent period loaded

    Returns:
        Total rows written across all three metrics.
        Returns 0 if all metrics are already up to date.
    """
    ensure_raw_table()

    watermark = get_watermark("zillow")
    all_frames = []

    for metric_type, url in ZILLOW_SOURCES.items():
        try:
            df_raw = fetch_zillow_csv(metric_type, url)
            df_long = melt_zillow(df_raw, metric_type)

            if df_long.is_empty():
                logger.warning(
                    f"[zillow/{metric_type}] Empty after transform — skipping"
                )
                continue

            df_new = apply_watermark(df_long, watermark, metric_type)

            if df_new.is_empty():
                logger.info(
                    f"[zillow/{metric_type}] No new periods — already up to date"
                )
                continue

            all_frames.append(df_new)

        except Exception as e:
            logger.error(f"[zillow/{metric_type}] Failed: {e}")
            raise

    if not all_frames:
        logger.info("[zillow] All metrics up to date — nothing to write")
        return 0

    df_final = pl.concat(all_frames)

    new_periods = df_final["period_month"].unique().sort()
    new_max = new_periods.max()

    logger.info(
        f"[zillow] Writing {df_final.shape[0]} rows "
        f"across {len(new_periods)} new periods "
        f"(latest: {new_max})"
    )

    _delete_periods(new_periods.to_list())

    rows_written = write_to_snowflake(
        rows=df_final.rows(),
        table="ZILLOW_METRICS",
        columns=df_final.columns,
    )

    # Watermark updated AFTER successful write.
    # .strftime() on a Polars Date produces "YYYY-MM-DD" — not str() which
    # would produce "datetime.date(2024, 3, 31)" and break incremental reads.
    update_watermark("zillow", new_max.strftime("%Y-%m-%d"))

    return rows_written


# ── CLI entrypoint ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    total = ingest_zillow()
    logger.info(f"[zillow] Ingest complete — {total} rows written")