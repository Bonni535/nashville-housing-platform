--- models/staging/stg_fred_mortgage_rates.sql
--
-- Staging model for FRED MORTGAGE30US weekly observations.
-- Simple pass-through — no MSA filter needed (national series).
-- Drops ingested_at infrastructure column per staging layer convention.
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

)

select
    observation_date,
    rate,
    series_id

from source