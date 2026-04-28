-- ============================================================
-- Nashville Housing Platform — Snowflake Environment Setup
-- Run once as ACCOUNTADMIN (or a role with CREATE DATABASE/WAREHOUSE/ROLE privileges)
-- ============================================================

-- Warehouse
CREATE WAREHOUSE IF NOT EXISTS HOUSING_PIPELINE_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Nashville Housing Platform — capstone XS warehouse';

-- Database
CREATE DATABASE IF NOT EXISTS HOUSING_PIPELINE
    COMMENT = 'Nashville Housing Platform — all schemas';

-- Schemas
CREATE SCHEMA IF NOT EXISTS HOUSING_PIPELINE.RAW
    COMMENT = 'Raw source data — all five ingestion sources land here unmodified';
CREATE SCHEMA IF NOT EXISTS HOUSING_PIPELINE.STAGING
    COMMENT = 'Staging views — type-cast, null-guarded, deduplicated';
CREATE SCHEMA IF NOT EXISTS HOUSING_PIPELINE.INTERMEDIATE
    COMMENT = 'Intermediate tables — enriched, joined across sources';
CREATE SCHEMA IF NOT EXISTS HOUSING_PIPELINE.MARTS
    COMMENT = 'Mart tables — final models powering the dashboard';

-- Role
CREATE ROLE IF NOT EXISTS HOUSING_PIPELINE_ROLE;

-- Grant role to your user (replace <your_user>)
GRANT ROLE HOUSING_PIPELINE_ROLE TO USER LUCANSS2026; ;

-- Privileges
GRANT USAGE ON WAREHOUSE HOUSING_PIPELINE_WH TO ROLE HOUSING_PIPELINE_ROLE;
GRANT ALL PRIVILEGES ON DATABASE HOUSING_PIPELINE TO ROLE HOUSING_PIPELINE_ROLE;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE HOUSING_PIPELINE TO ROLE HOUSING_PIPELINE_ROLE;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE HOUSING_PIPELINE TO ROLE HOUSING_PIPELINE_ROLE;
GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE HOUSING_PIPELINE TO ROLE HOUSING_PIPELINE_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE HOUSING_PIPELINE TO ROLE HOUSING_PIPELINE_ROLE;

-- Pipeline state table (used by property.py incremental watermark)
CREATE TABLE IF NOT EXISTS HOUSING_PIPELINE.RAW.PIPELINE_STATE (
    source_name     VARCHAR(100)  NOT NULL,
    last_processed  DATE          NOT NULL,
    updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_pipeline_state PRIMARY KEY (source_name)
);

-- Seed initial watermark for Nashville Parcels (adjust date as needed)
INSERT INTO HOUSING_PIPELINE.RAW.PIPELINE_STATE (source_name, last_processed)
    SELECT 'nashville_parcels', '2020-01-01'::DATE
    WHERE NOT EXISTS (
        SELECT 1 FROM HOUSING_PIPELINE.RAW.PIPELINE_STATE
        WHERE source_name = 'nashville_parcels'
    );

INSERT INTO HOUSING_PIPELINE.RAW.PIPELINE_STATE (source_name, last_processed)
    SELECT 'crime_incidents', '2020-01-01'::DATE
    WHERE NOT EXISTS (
        SELECT 1 FROM HOUSING_PIPELINE.RAW.PIPELINE_STATE
        WHERE source_name = 'crime_incidents'
    );

-- Pipeline audit log table (queried by Streamlit Pipeline Health panel — TICKET-028)
CREATE TABLE IF NOT EXISTS HOUSING_PIPELINE.RAW.PIPELINE_AUDIT (
    run_id                  VARCHAR(36)   NOT NULL DEFAULT UUID_STRING(),
    run_timestamp           TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    dag_id                  VARCHAR(100),
    status                  VARCHAR(20),   -- 'success' | 'partial_failure' | 'failure'
    dbt_tests_run           INTEGER,
    dbt_tests_passed        INTEGER,
    freshness_redfin        VARCHAR(20),   -- 'ok' | 'warn' | 'error'
    freshness_zillow        VARCHAR(20),
    freshness_property      VARCHAR(20),
    freshness_crime         VARCHAR(20),
    freshness_census        VARCHAR(20),
    notes                   VARCHAR(1000),
    CONSTRAINT pk_pipeline_audit PRIMARY KEY (run_id)
);

-- ============================================================
-- Verify setup
-- ============================================================
SHOW SCHEMAS IN DATABASE HOUSING_PIPELINE;
SHOW WAREHOUSES LIKE 'HOUSING_PIPELINE_WH';
