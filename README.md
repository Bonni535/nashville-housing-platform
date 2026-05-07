# Nashville Housing Platform

A full-stack data engineering pipeline for analyzing real estate opportunity across the Nashville MSA. Built as an NSS Data Engineering capstone project by Luca Bonini (2026).

---

## Overview

The Nashville Housing Platform ingests data from six public sources, transforms it through a layered dbt model graph, orchestrates daily runs via Apache Airflow, and surfaces insights through a Streamlit in Snowflake dashboard. At its core is a **7-signal composite opportunity score** covering all 76 zip codes in the Nashville metropolitan area.

**Repo:** github.com/Bonni535/nashville-housing-platform
**Project Board:** github.com/users/Bonni535/projects/5

---

## Architecture

```
External APIs                  Ingestion Layer              Snowflake RAW
─────────────                  ───────────────              ─────────────
Zillow S3          ──────────► zillow.py          ────────► ZILLOW_METRICS
Redfin TSV.GZ      ──────────► redfin.py          ────────► REDFIN_METRICS
Census ACS5 API    ──────────► census.py          ────────► CENSUS_ZIP
                                                            CENSUS_COUNTY
MNPD ArcGIS        ──────────► crime.py           ────────► CRIME_INCIDENTS
FRED API           ──────────► fred.py            ────────► FRED_MORTGAGE_RATES
Metro Codes ArcGIS ──────────► permits.py         ────────► BUILDING_PERMITS

                  dbt Transformations
                  ───────────────────
RAW  ──► STAGING (views) ──► INTERMEDIATE (tables) ──► MARTS (tables)

                  Orchestration
                  ─────────────
Apache Airflow (Docker, LocalExecutor)
  daily_ingestion_dag  — Census + Crime + Permits @ 6am daily
  redfin_dag           — Redfin + FRED @ 3am Wednesdays
  zillow_dag           — Zillow @ 4am 1st of month

                  Dashboard
                  ─────────
Streamlit in Snowflake — fct_opportunity_score + fct_monthly_zip
```

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.12 |
| Package manager | uv |
| Data warehouse | Snowflake (XS warehouse) |
| Transformation | dbt-snowflake 1.11.4 |
| Orchestration | Apache Airflow 2.9.1 (Docker) |
| Dashboard | Streamlit in Snowflake |
| Ingestion HTTP | httpx |
| DataFrame | Polars |
| Config | Pydantic v2 Settings |
| Linting | ruff |
| CI/CD | GitHub Actions |

---

## Repository Structure

```
nashville-housing-platform/
├── ingestion/
│   ├── config.py              — Pydantic Settings (env vars, Snowflake connection)
│   ├── utils.py               — Shared utilities: watermarks, Snowflake writes
│   ├── loader.py              — Daily orchestrator (Census + Crime + Permits)
│   └── sources/
│       ├── zillow.py          — Zillow ZHVI/ZORI/ZHVF ingestion
│       ├── redfin.py          — Redfin market tracker ingestion (ETag)
│       ├── census.py          — Census ACS5 at ZCTA + county level
│       ├── crime.py           — MNPD crime incidents (ArcGIS)
│       ├── fred.py            — FRED 30-year mortgage rate
│       ├── permits.py         — Nashville building permits (ArcGIS)
│       └── property.py        — Nashville parcels (retained, data-limited)
│
├── housing_pipeline/          — dbt project root (run all dbt commands from here)
│   ├── dbt_project.yml
│   ├── profiles.yml           — Safe to commit — uses env_var() throughout
│   ├── seeds/
│   │   ├── nashville_valid_zips.csv    — 76 MSA zip codes
│   │   └── nashville_zip_regions.csv  — zip → region/county mapping
│   └── models/
│       ├── staging/
│       │   ├── sources.yml
│       │   ├── stg_zillow.sql
│       │   ├── stg_redfin.sql
│       │   ├── stg_census_zip.sql
│       │   ├── stg_census_county.sql
│       │   ├── stg_crime_incidents.sql
│       │   ├── stg_fred_mortgage_rates.sql
│       │   └── stg_building_permits.sql
│       ├── intermediate/
│       │   ├── int_zip_demographics.sql
│       │   ├── int_market_activity.sql
│       │   ├── int_crime_index.sql
│       │   └── int_permit_activity.sql
│       └── marts/
│           ├── fct_monthly_zip.sql
│           ├── dim_geography.sql
│           └── fct_opportunity_score.sql
│
├── airflow/
│   ├── docker-compose.yml
│   └── dags/
│       ├── dag_utils.py           — Slack alerts + audit log utilities
│       ├── daily_ingestion_dag.py — Census + Crime + Permits + dbt
│       ├── redfin_dag.py          — Redfin + FRED + dbt
│       └── zillow_dag.py          — Zillow + dbt
│
├── .github/
│   └── workflows/
│       └── ci.yml             — Lint + dbt test on every PR to main
│
├── pyproject.toml
├── uv.lock
└── .env                       — Never committed (see Environment Variables)
```

---

## Data Sources

### Zillow Research Data
- **What:** ZHVI (home value index), ZORI (rent index), ZHVF (forecast) at zip level
- **Cadence:** Monthly (1st of month)
- **Range:** 2000–2027 (ZHVF is forecast)
- **Pattern:** Full CSV download from Zillow S3, TN filter in ingestion, MSA filter in dbt

### Redfin Data Center
- **What:** Weekly zip-level market activity — median DOM, inventory, sale-to-list ratio, homes sold, median sale price
- **Cadence:** Weekly (Wednesdays)
- **Range:** 2012–present
- **Pattern:** 1.5GB TSV.GZ streamed to disk, ETag HEAD check prevents redundant downloads

### Census ACS5
- **What:** Median household income, poverty count, total population at ZCTA and county level
- **Cadence:** Annual (Census publishes ~January each year)
- **Vintages loaded:** 2019–2024
- **Pattern:** ZCTAs don't nest within states in Census geography — pull all 33K+ nationally, filter to TN range (37000–38599) in Python

### Nashville MNPD Crime Incidents
- **What:** Crime incidents from Nashville Police Department ArcGIS FeatureServer
- **Cadence:** Daily (30-day lookback for late-arriving records)
- **Range:** 2019–present (Nashville migrated from Socrata to ArcGIS in 2025/2026; pre-2019 data not available)
- **Coverage:** Davidson County only — 23 suburban MSA zips have no data (imputed)
- **Note:** ~42% of raw records have null ZIP_Code — filtered before writing to Snowflake

### FRED Mortgage Rate
- **What:** MORTGAGE30US — US weekly 30-year fixed rate mortgage average
- **Cadence:** Weekly (published Thursdays)
- **Range:** 2000–present
- **Purpose:** Economic context for dashboard — not used in opportunity score (national series, no zip-level differentiation)

### Nashville Building Permits
- **What:** Building permits issued by Metro Codes Administration
- **Cadence:** Daily (7-day lookback)
- **Range:** Rolling ~3-year window maintained by Metro Codes
- **Coverage:** Davidson County only — suburban zips imputed with MSA average
- **Records:** ~29,000 permits
- **Key API fields:** `ZIP` (not `Zip_Code`), `Const_Cost` (not `Construction_Cost`), max 1,000 records/page

---

## Snowflake Schema

Database: `HOUSING_PIPELINE`
Role: `HOUSING_PIPELINE_ROLE`
Warehouse: `HOUSING_PIPELINE_WH` (XS)
Schemas: `RAW` → `STAGING` → `INTERMEDIATE` → `MARTS`

### RAW Tables

| Table | Rows | Notes |
|-------|------|-------|
| ZILLOW_METRICS | 173,247 | ZHVI + ZORI + ZHVF, 2000–2027 |
| REDFIN_METRICS | 178,757 | Weekly, 2012–2026 |
| CENSUS_ZIP | ~3,800 | 6 vintages × ~636 TN ZCTAs |
| CENSUS_COUNTY | 30 | 6 vintages × 5 MSA counties |
| CRIME_INCIDENTS | 466,000+ | 2019–present, daily incremental |
| FRED_MORTGAGE_RATES | 1,374 | Weekly, 2000–present |
| BUILDING_PERMITS | 29,213 | Rolling ~3 years |
| NASHVILLE_VALID_ZIPS | 76 | Seed — MSA zip filter |
| NASHVILLE_ZIP_REGIONS | 76 | Seed — zip → region/county |
| PIPELINE_STATE | 7 | Watermarks: zillow, redfin, census, parcels, crime, fred, permits |
| PIPELINE_AUDIT | 7+ | DAG run history |

### Key Mart Tables

**`MARTS.FCT_MONTHLY_ZIP`** — 10,650 rows (76 zips × 169 months)
Monthly zip-level market activity joining Redfin, Zillow, and FRED mortgage rate.

**`MARTS.FCT_OPPORTUNITY_SCORE`** — 76 rows (one per MSA zip)
Composite opportunity score from 7 normalized signals.

| Signal | Direction | Source |
|--------|-----------|--------|
| affordability_score | Lower price = higher score | Redfin median_sale_price |
| market_speed_score | Lower DOM = higher score | Redfin median_dom |
| activity_score | More sales = higher score | Redfin median_homes_sold |
| income_score | Higher income = higher score | Census median_household_income |
| poverty_score | Lower poverty = higher score | Census poverty_rate |
| safety_score | Lower crime = higher score | MNPD incidents_per_1k |
| permit_score | More permits = higher score | Metro Codes permit_count |

Final `opportunity_score` = unweighted average of all 7 signals (0–100 range).

`data_confidence` per zip: High / Partial / Low based on Redfin and crime data presence.

---

## Nashville MSA Coverage

76 zip codes across 5 counties:

| County | Zips | Notes |
|--------|------|-------|
| Davidson | 32 | Full coverage — crime + permits |
| Williamson | 13 | Crime + permits imputed |
| Rutherford | 15 | Crime + permits imputed |
| Sumner | 9 | Crime + permits imputed |
| Wilson | 7 | Crime + permits imputed |

Davidson County is split into 4 sub-regions: Urban Core, West Nashville, North Nashville, Southeast Nashville. Suburban counties are each treated as a single region.

---

## Setup

### Prerequisites

- Python 3.12
- uv (`pip install uv`)
- Snowflake account with `HOUSING_PIPELINE` database, schemas, role, and warehouse pre-created
- Docker Desktop (for Airflow)

### Environment Variables

Create `.env` at the repo root (never committed):

```bash
# Snowflake
SNOWFLAKE_ACCOUNT=your-account.region
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=HOUSING_PIPELINE_ROLE
SNOWFLAKE_WAREHOUSE=HOUSING_PIPELINE_WH
SNOWFLAKE_DATABASE=HOUSING_PIPELINE
SNOWFLAKE_SCHEMA=RAW

# APIs
CENSUS_API_KEY=your_key       # free at api.census.gov/data/signup.html
FRED_API_KEY=your_key         # free at fred.stlouisfed.org/docs/api/api_key.html

# Alerting (optional at dev time)
SLACK_WEBHOOK_URL=

# Environment
PIPELINE_ENV=dev
```

**Gotcha:** If any of these are set as shell environment variables they will override `.env`. Run `env | grep -E "SNOWFLAKE|CENSUS|FRED|SLACK"` and `unset` any stale values before running locally.

### Install Dependencies

```bash
uv sync
```

### dbt Setup

All dbt commands must be run from inside `housing_pipeline/`:

```bash
cd housing_pipeline
uv run --env-file ../.env dbt debug    # verify connection
uv run --env-file ../.env dbt seed     # load seed files
uv run --env-file ../.env dbt run      # build all models
uv run --env-file ../.env dbt test     # run all 37 tests
```

---

## Running Ingestion

### Full Initial Load

Run each source once from the repo root to populate RAW tables:

```bash
# Daily sources (Census, Crime, Parcels)
uv run --env-file .env python -m ingestion.loader

# Redfin (weekly)
uv run --env-file .env python -m ingestion.sources.redfin

# Zillow (monthly)
uv run --env-file .env python -m ingestion.sources.zillow

# FRED mortgage rate
uv run --env-file .env python -m ingestion.sources.fred

# Building permits
uv run --env-file .env python -m ingestion.sources.permits
```

### Incremental Loads

All sources are fully incremental via `RAW.PIPELINE_STATE` watermarks. Re-running any ingestion command only fetches new data since the last run.

**Redfin** additionally uses HTTP ETag conditional fetching — if the source file hasn't changed since the last run, the 1.5GB download is skipped entirely.

---

## Running Airflow

```bash
cd airflow

# First time only
docker-compose up airflow-init

# Start webserver + scheduler
docker-compose up airflow-webserver airflow-scheduler

# UI: http://localhost:8082 (login: admin / admin)

# Restart scheduler after Python code changes
docker-compose restart airflow-scheduler

# Teardown
docker-compose down
```

**Note:** Airflow runs on port 8082 (internal 8080 is occupied by VS Code Helper on the dev machine).

### DAG Schedule

| DAG | Schedule | Sources |
|-----|----------|---------|
| daily_ingestion_dag | 6am daily | Census + Crime + Permits + dbt |
| redfin_dag | 3am Wednesdays | Redfin + FRED + dbt |
| zillow_dag | 4am 1st of month | Zillow + dbt |

---

## CI/CD

GitHub Actions runs on every pull request to `main`:

1. **lint** — `ruff check .`
2. **dbt-ci** (depends on lint):
   - `dbt compile --target ci` — catches SQL syntax errors
   - `dbt test --target ci` — runs all 37 data tests against `HOUSING_PIPELINE.CI` schema
   - `dbt source freshness --target ci` — checks data recency

Branch protection on `main` requires both jobs to pass before merging.

**CI target:** Reads from shared `RAW` schema, writes to `HOUSING_PIPELINE.CI` — isolated from active development without duplicating source data.

**Required GitHub secrets:** `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`

---

## dbt Model Graph

```
seeds/
  nashville_valid_zips
  nashville_zip_regions
        │
        ▼
staging/ (views)
  stg_zillow ──────────────────────────────────────────────────────────┐
  stg_redfin ──────────────────────────────────────────────────────────┤
  stg_census_zip ───────────────────────────────────────────────────┐  │
  stg_census_county                                                 │  │
  stg_crime_incidents ──────────────────────────────────────────┐  │  │
  stg_fred_mortgage_rates ──────────────────────────────────┐   │  │  │
  stg_building_permits ─────────────────────────────────┐   │   │  │  │
        │                                               │   │   │  │  │
        ▼                                               │   │   │  │  │
intermediate/ (tables)                                 │   │   │  │  │
  int_zip_demographics ◄──────────────────────────────────────┘  │  │
  int_market_activity ◄────────────────────────────────────────────────┘
  int_crime_index ◄───────────────────────────────────────────┘     │
  int_permit_activity ◄───────────────────────────────────────┘     │
        │                                               │           │
        ▼                                               │           │
marts/ (tables)                                        │           │
  dim_geography ◄── int_zip_demographics               │           │
  fct_monthly_zip ◄── int_market_activity + stg_zillow ┘ + stg_fred┘
  fct_opportunity_score ◄── fct_monthly_zip + int_crime_index
                          + int_permit_activity + dim_geography
```

**Test suite:** 37 tests — schema tests (not_null, unique, accepted_values) + 7 singular tests (grain uniqueness, score range, null thresholds).

---

## Known Limitations

| Limitation | Root Cause | Status |
|------------|-----------|--------|
| Crime data starts 2019 | Nashville's Socrata→ArcGIS migration didn't carry pre-2019 history | Platform limitation — document, not a bug |
| 23 suburban zips have no crime data | MNPD jurisdiction covers Davidson County only | Imputed with MSA average |
| 23 suburban zips have no permit data | Metro Codes covers Davidson County only | Imputed with MSA average |
| Building permit history is rolling ~3 years | Metro Codes API design | No fix available |
| 5 zips have no Redfin coverage | Redfin doesn't publish data for those zips | No fix available |
| Walk Score not integrated | Free API tier requires domain email, blocks gmail | Future work |
| Parcel transaction data limited to 3 records | Nashville public ArcGIS exposes ownership snapshot only | Replaced by Redfin homes_sold signal |
| ZHVF (Zillow forecast) joins zero rows | Redfin spine ends 2026-03 and ZHVF starts 2026-04 | Resolves naturally as Redfin updates |

---

## Future Work

- **Walk Score API** — zip-level walkability/transit/bike accessibility scores
- **Williamson County crime data** — requires data sharing agreement with county sheriff
- **School quality signal** — TDOE or GreatSchools data for family-focused scoring
- **Production deployment** — authenticated Snowflake Assessor database access for full parcel history
- **dbt source-scoped selectors** — each Airflow DAG currently runs full `dbt run`; scoped selectors (e.g. `--select +stg_redfin+`) would be a production optimization

---

## Author

**Luca Bonini**
NSS Data Engineering Cohort, 2026
github.com/Bonni535