-- models/intermediate/int_permit_activity.sql
--
-- Aggregates building permits to annual zip level.
-- Produces permit_count and total_construction_cost per zip per year.
--
-- permit_count is used as the permit momentum signal in fct_opportunity_score.
-- total_construction_cost is carried through for dashboard visibility but not
-- scored directly — it is dominated by large commercial projects in 37203
-- (downtown) and would skew zip-level comparisons.
--
-- Coverage note: Metro Codes covers Davidson County only. Suburban zips have
-- no rows here. fct_opportunity_score handles this with neutral imputation
-- (avg permit_count) — same pattern used for crime data.
--
-- Grain: one row per zip_code + permit_year
-- Materialized as table — downstream fct_opportunity_score queries this
-- via QUALIFY ROW_NUMBER() to get the most recent year per zip.

with permits as (

    select
        zip_code,
        year(date_issued)        as permit_year,
        count(*)                 as permit_count,
        sum(construction_cost)   as total_construction_cost

    from {{ ref('stg_building_permits') }}

    where date_issued is not null

    group by
        zip_code,
        year(date_issued)

)

select
    zip_code,
    permit_year,
    permit_count,
    total_construction_cost

from permits