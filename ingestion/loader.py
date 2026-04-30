# ingestion/loader.py
#
# Daily ingestion orchestrator — runs Census, Parcels, and Crime in parallel.
#
# Zillow and Redfin are NOT in this file:
#   - Zillow: monthly cadence, triggered on Zillow Research Data release
#   - Redfin: weekly cadence, dedicated redfin_ingest_dag.py (Wednesdays)
#
# This module is called by the daily Airflow DAG (housing_pipeline_dag.py)
# and can also be run directly for manual ingestion:
#   uv run python ingestion/loader.py
#
# Per-source error handling: a failed source logs the error and does not
# block other sources from running. Final summary shows status per source.

import concurrent.futures
from loguru import logger

from ingestion.sources.census import ingest_census
from ingestion.sources.crime import ingest_crime
from ingestion.sources.property import ingest_property


# ── Source registry ────────────────────────────────────────────────────────────

# Each entry: (source_name, callable)
# Order doesn't matter — all three run in parallel via ThreadPoolExecutor
DAILY_SOURCES = [
    ("census", ingest_census),
    ("parcels", ingest_property),
    ("crime", ingest_crime),
]


# ── Runner ─────────────────────────────────────────────────────────────────────

def run_source(name: str, fn) -> tuple[str, int | dict | None, Exception | None]:
    """
    Run a single ingestion source function and return its result.

    Returns a tuple of (source_name, result, error) where:
        - result is the rows written (int or dict for census)
        - error is None on success, Exception on failure

    This function is designed to be called inside a ThreadPoolExecutor.
    Exceptions are caught and returned rather than raised so a single
    source failure does not cancel the other parallel tasks.

    Args:
        name: Source name for logging e.g. 'census'
        fn:   Ingestion callable e.g. ingest_census

    Returns:
        Tuple of (name, result, error)
    """
    logger.info(f"[loader] Starting {name} ingestion...")
    try:
        result = fn()
        logger.info(f"[loader] {name} complete — {result} rows written")
        return name, result, None
    except Exception as e:
        logger.error(f"[loader] {name} failed: {e}")
        return name, None, e


# ── Main entry point ───────────────────────────────────────────────────────────

def run_daily_ingestion() -> dict[str, str]:
    """
    Run all three daily ingestion sources in parallel.

    Uses ThreadPoolExecutor with 3 workers — one per source. I/O-bound
    tasks (network fetches + Snowflake writes) benefit from threading
    even with Python's GIL since most time is spent waiting on I/O.

    Flow:
        1. Submit all three sources to thread pool simultaneously
        2. Collect results as each completes
        3. Log per-source summary
        4. Return status dict for Airflow audit log

    Returns:
        Dict mapping source name to status string:
        {'census': 'success: 636 rows', 'parcels': 'success: 3 rows',
         'crime': 'success: 350000 rows'}
        or {'census': 'failed: <error message>'} on failure.
    """
    logger.info("[loader] Starting daily ingestion — Census, Parcels, Crime")
    logger.info("[loader] Note: Zillow (monthly) and Redfin (weekly) run on separate schedules")

    results = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # Submit all sources simultaneously
        futures = {
            executor.submit(run_source, name, fn): name
            for name, fn in DAILY_SOURCES
        }

        # Collect results as each completes
        for future in concurrent.futures.as_completed(futures):
            name, result, error = future.result()

            if error is not None:
                results[name] = f"failed: {error}"
            else:
                # Census returns dict, others return int
                if isinstance(result, dict):
                    total = sum(result.values())
                    results[name] = f"success: {total} rows ({result})"
                else:
                    results[name] = f"success: {result} rows"

    # Final summary
    logger.info("[loader] Daily ingestion complete:")
    for source, status in results.items():
        logger.info(f"  {source}: {status}")

    failed = [s for s, r in results.items() if r.startswith("failed")]
    if failed:
        logger.error(f"[loader] {len(failed)} source(s) failed: {failed}")
    else:
        logger.info("[loader] All sources completed successfully")

    return results


# ── CLI entrypoint ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    results = run_daily_ingestion()