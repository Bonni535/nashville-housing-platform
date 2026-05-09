# Nashville Housing Intelligence Platform

An end-to-end data engineering capstone project built at Nashville Software School (NSS), 2026. The platform ingests real estate, demographic, crime, and economic data for the Nashville MSA, transforms it through a layered dbt pipeline, orchestrates daily loads with Airflow, and surfaces insights through an interactive Streamlit in Snowflake dashboard.

---

## What It Does

The platform answers one question: **where in Nashville represents the best housing opportunity right now?**

It computes a composite **Opportunity Score** (0–100) for each of 76 Nashville MSA zip codes using 7 signals: affordability, market speed, transaction activity, household income, poverty rate, crime safety, and building permit activity. The score is surfaced on an interactive choropleth map with adjustable signal weights, alongside trend charts for affordability, inventory, market momentum, and transactions.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | Python 3.12, httpx, Polars, Pydantic v2 |
| Package management | uv |
| Data warehouse | Snowflake (XS warehouse) |
| Transformation | dbt-snowflake 1.11.4 |
| Orchestration | Apache Airflow 2.9.1 (LocalExecutor, Docker Compose) |
| CI/CD | GitHub Actions (ruff + dbt compile + dbt test on every PR) |
| Dashboard | Streamlit in Snowflake (pydeck, Altair) |
| Testing | pytest (106 ingestion unit tests) |

---

## Architecture

```
External APIs
    │
    ├── Zillow Research (S3 CSV)          ─┐
    ├── Redfin Data Center (TSV.GZ)        │
    ├── Census ACS5 API                    ├── Python ingestion
    ├── FRED API (MORTGAGE30US)            │   (httpx, Polars)
    ├── Nashville Metro Codes (ArcGIS)     │
    └── MNPD Crime Incidents (ArcGIS)     ─┘
                │
                ▼
        Snowflake RAW schema
        (ZILLOW_METRICS, REDFIN_METRICS,
         CENSUS_ZIP/COUNTY, CRIME_INCIDENTS,
         FRED_MORTGAGE_RATES, BUILDING_PERMITS)
                │
                ▼
        dbt STAGING (views)
        MSA filter + type casting + rename
                │
                ▼
        dbt INTERMEDIATE (tables)
        int_zip_demographics, int_market_activity,
        int_crime_index, int_permit_activity
                │
                ▼
        dbt MARTS (tables)
        fct_monthly_zip, dim_geography,
        fct_opportunity_score
                │
                ▼
        Streamlit in Snowflake Dashboard
        Map · Affordability · Inventory ·
        Crime · Momentum · Transactions ·
        Pipeline Health

Apache Airflow (Docker Compose, LocalExecutor)
    ├── daily_ingestion_dag  (6am daily)   — ingest + dbt run + dbt test
    ├── redfin_dag           (3am Wednesday)
    └── zillow_dag           (4am 1st of month)

GitHub Actions CI
    ├── ruff check .
    ├── dbt compile --target ci
    ├── dbt test --target ci
    └── dbt source freshness --target ci
```

---

## Data Sources

| Source | What It Provides | Cadence |
|--------|-----------------|---------|
| Zillow Research | ZHVI (home value index), ZORI (rent index), ZHVF (forecast) by zip | Monthly |
| Redfin Data Center | Median DOM, inventory, sale-to-list ratio, homes sold, new listings by zip | Weekly |
| Census ACS5 | Median household income, poverty rate, population by zip and county | Annual (vintages 2019–2024) |
| FRED (St. Louis Fed) | 30-year fixed mortgage rate (MORTGAGE30US) | Weekly |
| Nashville Metro Codes | Building permits issued — zip, type, cost (Davidson County) | Daily incremental |
| MNPD via Nashville Open Data | Crime incidents by zip and type (Davidson County, 2019–present) | Daily incremental |

---

## Repository Structure

```
nashville-housing-platform/
├── ingestion/
│   ├── config.py              — Pydantic v2 Settings (Snowflake + API keys)
│   ├── utils.py               — Shared Snowflake helpers, watermark tracking
│   ├── loader.py              — ThreadPoolExecutor orchestrator for daily sources
│   └── sources/
│       ├── zillow.py          — ZHVI/ZORI/ZHVF ingestion
│       ├── redfin.py          — Weekly market tracker (ETag conditional fetch)
│       ├── census.py          — ACS5 ZIP + county ingestion
│       ├── fred.py            — FRED MORTGAGE30US ingestion
│       ├── permits.py         — Nashville building permits (ArcGIS)
│       ├── crime.py           — MNPD crime incidents (ArcGIS)
│       └── property.py        — Nashville Parcels property sales (ArcGIS)
│
├── housing_pipeline/          — dbt project root
│   ├── dbt_project.yml
│   ├── profiles.yml           — Uses env_var() throughout, safe to commit
│   ├── seeds/
│   │   ├── nashville_valid_zips.csv    — 76 MSA zip codes
│   │   └── nashville_zip_regions.csv  — zip → region/county/fips mapping
│   └── models/
│       ├── staging/           — 7 views (stg_*)
│       ├── intermediate/      — 4 tables (int_*)
│       └── marts/             — 3 tables (fct_*, dim_*)
│
├── airflow/
│   ├── docker-compose.yml
│   └── dags/
│       ├── dag_utils.py
│       ├── daily_ingestion_dag.py
│       ├── redfin_dag.py
│       └── zillow_dag.py
│
├── dashboard/
│   ├── app.py                 — Streamlit in Snowflake dashboard
│   └── nashville_zips.geojson — Static ZCTA boundaries (5.5MB)
│
├── tests/
│   └── ingestion/             — 106 unit tests for all ingestion modules
│       ├── conftest.py
│       ├── test_config.py
│       ├── test_utils.py
│       ├── test_fred.py
│       ├── test_permits.py
│       ├── test_census.py
│       ├── test_crime.py
│       ├── test_redfin.py
│       ├── test_zillow.py
│       └── test_property.py
│
├── scripts/
│   ├── build_seed_files.py    — Generates nashville_valid_zips.csv from Census crosswalk
│   └── fetch_nashville_geojson.py  — One-time GeoJSON fetch + Snowflake stage upload
│
├── .github/
│   └── workflows/
│       └── ci.yml             — Lint + dbt test on every PR to main
│
├── pyproject.toml
├── uv.lock
└── .env                       — Never committed (see .env.example)
```

---

## Setup

### Prerequisites

- Python 3.12
- [uv](https://docs.astral.sh/uv/) — `curl -LsSf https://astral.sh/uv/install.sh | sh`
- Snowflake account with `HOUSING_PIPELINE_ROLE` and `HOUSING_PIPELINE_WH`
- Census API key (free at [api.census.gov](https://api.census.gov/data/signup.html))
- FRED API key (free at [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html))

### Environment Variables

Create `.env` at repo root:

```bash
SNOWFLAKE_ACCOUNT=your-account.region
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=HOUSING_PIPELINE_ROLE
SNOWFLAKE_DATABASE=HOUSING_PIPELINE
SNOWFLAKE_WAREHOUSE=HOUSING_PIPELINE_WH
SNOWFLAKE_SCHEMA=RAW
CENSUS_API_KEY=your_census_key
FRED_API_KEY=your_fred_key
SLACK_WEBHOOK_URL=          # optional
PIPELINE_ENV=dev
```

**Shell variable gotcha:** If any of these are set as shell env vars they will override `.env`. Run `env | grep -E "SNOWFLAKE|CENSUS|FRED"` and `unset` any stale values.

### Install Dependencies

```bash
uv sync
```

### dbt Setup

```bash
cd housing_pipeline
uv run --env-file ../.env dbt debug    # verify connection
uv run --env-file ../.env dbt seed     # load seed files
uv run --env-file ../.env dbt run      # build all models
uv run --env-file ../.env dbt test     # run all 54 tests
```

All dbt commands must be run from inside `housing_pipeline/`. Running from repo root gives "dbt_project.yml not found".

---

## Running the Pipeline

### Ingestion (daily sources)

```bash
uv run --env-file .env python ingestion/loader.py
```

### Ingestion (individual sources)

```bash
uv run --env-file .env python -c "from ingestion.sources.zillow import run; run()"
uv run --env-file .env python -c "from ingestion.sources.redfin import run; run()"
```

### dbt (from housing_pipeline/)

```bash
# Full run
uv run --env-file ../.env dbt run

# Single model + downstream
uv run --env-file ../.env dbt run --select fct_opportunity_score+

# CI target (writes to HOUSING_PIPELINE.CI schema)
uv run --env-file ../.env dbt run --target ci
```

### Unit Tests

```bash
uv run pytest tests/ingestion/ -v
```

Runs 106 unit tests covering all ingestion modules — config validation, watermark logic, transform functions, and HTTP client behavior. No Snowflake connection or API keys required; all external calls are mocked.

### Airflow (from airflow/)

```bash
# First time only
docker-compose up airflow-init

# Start
docker-compose up airflow-webserver airflow-scheduler

# UI: http://localhost:8082  (login: admin / admin)
# Note: port 8082 because VS Code Helper occupies 8080
```

### One-Time Setup: GeoJSON for Dashboard Map

```bash
uv run python scripts/fetch_nashville_geojson.py
```

This fetches Nashville MSA ZCTA boundaries from Census TIGERweb (layer 4), saves to `dashboard/nashville_zips.geojson`, creates the `DASHBOARD_ASSETS` stage in Snowflake, and uploads the file. Only needs to run once.

---

## Dashboard

The dashboard runs in Streamlit in Snowflake (SiS):

1. Open Snowflake → Streamlit → Nashville Housing Platform
2. Ensure `pydeck` is added via the Packages panel
3. Hit Run

**Sections:**
- **Map** — Choropleth of opportunity scores with 7 adjustable signal weight sliders
- **Affordability** — ZHVI trend, sale price by region, mortgage rate, zip drill-down
- **Inventory** — Active listings, months of supply, new listings, zip drill-down
- **Crime** — Crime rate by zip and region, trend 2019–2026 (Davidson County only)
- **Momentum** — Days on market, sale-to-list ratio, zip drill-down
- **Transactions** — Homes sold trend, building permits by zip
- **Pipeline Health** — Airflow run history, dbt test results, source freshness

---

## CI/CD

GitHub Actions workflow (`.github/workflows/ci.yml`) runs on every PR to `main`:

1. `ruff check .` — linting
2. `dbt compile --target ci` — SQL syntax check
3. `dbt test --target ci` — all 54 data tests against `HOUSING_PIPELINE.CI` schema
4. `dbt source freshness --target ci` — source recency checks

Branch protection on `main` requires both `lint` and `dbt-ci` to pass before merge.

Required GitHub secrets: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`

---

## Opportunity Score Methodology

The opportunity score is a min-max normalized composite of 7 signals, equal-weighted by default:

| Signal | Source | Direction |
|--------|--------|-----------|
| Affordability | Redfin `median_sale_price` | Inverted (lower price = higher score) |
| Market Speed | Redfin `median_dom` | Inverted (lower DOM = higher score) |
| Activity | Redfin `homes_sold` | Direct |
| Income | Census ACS5 `median_household_income` | Direct |
| Low Poverty | Census ACS5 `poverty_rate` | Inverted |
| Safety | MNPD `incidents_per_1k` | Inverted |
| Permits | Metro Codes `permit_count` | Direct |

**Score range:** ~40–82 across 76 Nashville MSA zip codes  
**Data confidence:** High (52 zips) · Partial (19 zips) · Low (5 zips)

Suburban zips with no MNPD or Metro Codes data are imputed with the MSA average for those signals rather than excluded.

---

## Known Limitations

- **Walk Score** — Free tier requires domain email. Future work: production would integrate Walk Score API for zip-level accessibility scoring.
- **MNPD crime coverage** — Davidson County only. 23 suburban zips (Williamson, Rutherford, Wilson, Sumner) are imputed.
- **Metro Codes permits** — Davidson County only, same 23 zips imputed. Rolling ~3-year window only.
- **Crime history starts 2019** — Nashville's ArcGIS migration from Socrata did not carry pre-2019 data.
- **Williamson County crime** — No public queryable FeatureServer API available.
- **Nashville Parcels** — Public ArcGIS endpoint returns only ~3 arm's-length sale records. Full transaction history requires authenticated access to Davidson County Assessor database. Transaction signals sourced from Redfin instead.
- **`months_of_supply`** — Not populated by Redfin at Nashville zip level. Derived from `inventory / homes_sold`.
- **FRED duplicate observations** — Concurrent Airflow runs sharing the same watermark window can produce duplicate `observation_date` rows in `RAW.FRED_MORTGAGE_RATES` via the delete-then-insert idempotency pattern. Mitigated by `QUALIFY` deduplication in `stg_fred_mortgage_rates` and `max_active_runs=1` on the DAG.
- **Building permits deduplication** — Metro Codes ArcGIS can return duplicate `permit_number` values across incremental loads. 33 duplicates observed; deduplicated via `QUALIFY ROW_NUMBER()` in `stg_building_permits`.

---

## Acknowledgements

Built as an NSS Data Engineering capstone, 2026. Data sources: Zillow Research, Redfin Data Center, US Census Bureau ACS5, Federal Reserve Bank of St. Louis (FRED), Nashville Metro Codes, Metro Nashville Police Department via Nashville Open Data.