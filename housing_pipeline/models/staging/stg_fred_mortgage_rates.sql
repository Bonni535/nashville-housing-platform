-- models/staging/stg_fred_mortgage_rates.sql
--
-- Staging model for FRED MORTGAGE30US weekly observations.
-- Simple pass-through — no MSA filter needed (national series).
-- Drops ingested_at infrastructure column per staging layer convention.
--
-- Deduplication: concurrent Airflow runs with the same watermark window
-- can produce duplicate observation_date values in RAW.FRED_MORTGAGE_RATES
-- via the delete-then-insert idempotency pattern. QUALIFY ROW_NUMBER()
-- ensures exactly one row per observation_date in the staging view.
--
-- Grain: one row per observation_date (weekly, every Thursday)
-- Range: 2000-01-01 → present
-- Downstream: fct_monthly_zip (aggregated to monthly average)

with source as (

    select
        observation_date,
        rate,
        series_id

    from {{ source('raw', 'fred_mortgage_rates') }}

),

deduplicated as (

    select
        observation_date,
        rate,
        series_id

    from source

    qualify row_number() over (
        partition by observation_date
        order by observation_date
    ) = 1

)

select
    observation_date,
    rate,
    series_id

from deduplicated