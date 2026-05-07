#!/usr/bin/env python3
"""
scripts/fetch_nashville_geojson.py

One-time setup script. Fetches Nashville MSA ZCTA boundary GeoJSON from
Census TIGERweb, saves to dashboard/nashville_zips.geojson, and uploads
it to a Snowflake internal stage so the SiS app can load it without any
external network access at runtime.

Run once from repo root:
    uv run python scripts/fetch_nashville_geojson.py

Safe to re-run — stage upload uses OVERWRITE = TRUE.
"""

import csv
import json
import os
import urllib.parse
import urllib.request
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
REPO_ROOT   = Path(__file__).parent.parent
SEED_PATH   = REPO_ROOT / "housing_pipeline" / "seeds" / "nashville_valid_zips.csv"
OUTPUT_PATH = REPO_ROOT / "dashboard" / "nashville_zips.geojson"
STAGE_NAME  = "HOUSING_PIPELINE.PUBLIC.DASHBOARD_ASSETS"
STAGE_FILE  = "nashville_zips.geojson"


# ---------------------------------------------------------------------------
# Step 1 — Load zip codes from seed file (single source of truth)
# ---------------------------------------------------------------------------
def load_nashville_zips() -> list[str]:
    with open(SEED_PATH, newline="") as f:
        reader = csv.DictReader(f)
        zips = [row["zip_code"].strip() for row in reader]
    print(f"  Loaded {len(zips)} zip codes from seed file")
    return zips


# ---------------------------------------------------------------------------
# Step 2 — Fetch GeoJSON from TIGERweb
# ---------------------------------------------------------------------------
def fetch_geojson(zip_codes: list[str]) -> dict:
    # ZCTA5 is a text field — values must be quoted as strings in the WHERE clause
    zip_list = ",".join(f"'{z}'" for z in zip_codes)
    base_url = (
        "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/"
        "PUMA_TAD_TAZ_UGA_ZCTA/MapServer/4/query"
    )
    params = urllib.parse.urlencode({
        "where":     f"ZCTA5 IN ({zip_list})",
        "outFields": "ZCTA5",
        "outSR":     "4326",
        "f":         "geojson",
    })
    url = f"{base_url}?{params}"
    print(f"  Fetching GeoJSON from TIGERweb ({len(zip_codes)} zips)...")
    with urllib.request.urlopen(url, timeout=60) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    feature_count = len(data.get("features", []))
    print(f"  Received {feature_count} features")
    if feature_count < len(zip_codes) * 0.9:
        print(f"  WARNING: expected ~{len(zip_codes)} features, got {feature_count}")
    return data


# ---------------------------------------------------------------------------
# Step 3 — Save to disk
# ---------------------------------------------------------------------------
def save_geojson(data: dict) -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(data, f)
    size_kb = OUTPUT_PATH.stat().st_size / 1024
    print(f"  Saved to {OUTPUT_PATH} ({size_kb:.0f} KB)")


# ---------------------------------------------------------------------------
# Step 4 — Upload to Snowflake internal stage
# ---------------------------------------------------------------------------
def upload_to_stage() -> None:
    load_dotenv(REPO_ROOT / ".env")

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "HOUSING_PIPELINE_ROLE"),
        database="HOUSING_PIPELINE",
        schema="PUBLIC",
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "HOUSING_PIPELINE_WH"),
    )
    cur = conn.cursor()

    # Create stage if it doesn't exist
    cur.execute(f"""
        CREATE STAGE IF NOT EXISTS {STAGE_NAME}
            ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
            COMMENT = 'Static assets for Nashville Housing Platform dashboard'
    """)
    print(f"  Stage {STAGE_NAME} ready")

    # Upload — AUTO_COMPRESS=FALSE keeps the file as plain JSON
    rows = cur.execute(f"""
        PUT file://{OUTPUT_PATH.absolute()}
            @{STAGE_NAME}
            AUTO_COMPRESS = FALSE
            OVERWRITE = TRUE
    """).fetchall()

    status = rows[0][6] if rows else "unknown"
    print(f"  PUT status: {status}")

    cur.close()
    conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("\n=== fetch_nashville_geojson.py ===\n")

    print("Step 1 — Loading zip codes from seed...")
    zips = load_nashville_zips()

    print("\nStep 2 — Fetching GeoJSON from TIGERweb...")
    geojson = fetch_geojson(zips)

    print("\nStep 3 — Saving to disk...")
    save_geojson(geojson)

    print("\nStep 4 — Uploading to Snowflake stage...")
    upload_to_stage()

    print("\n✅ Done. nashville_zips.geojson is in the stage and ready for the dashboard.")
    print(f"   Local copy kept at: {OUTPUT_PATH}")
    print("   Commit dashboard/nashville_zips.geojson to the repo as a static asset.\n")