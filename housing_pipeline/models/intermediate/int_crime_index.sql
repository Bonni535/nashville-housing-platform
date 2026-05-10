-- models/intermediate/int_crime_index.sql
--
-- Annual crime rate per zip, normalized by residential population.
-- Administrative entries (police inquiries, lost/found property, transport,
-- natural deaths, etc.) are excluded — these inflated zip-level rates,
-- particularly in high-activity areas like downtown Nashville (37213).
--
-- crime_category is defined in stg_crime_incidents and covers:
--   Violent, Property, Drug, Other (all included here)
--   Administrative                  (excluded here)
--
-- Grain: one row per zip_code + incident_year
-- Downstream: fct_opportunity_score (safety_score signal)

with crimes as (

    select
        zip_code,
        year(incident_occurred) as incident_year,
        crime_category

    from {{ ref('stg_crime_incidents') }}

    -- Exclude non-criminal administrative entries.
    -- This is the primary fix for inflated downtown crime rates.
    -- POLICE INQUIRY alone accounted for ~167k of ~466k total incidents.
    where crime_category != 'Administrative'

),

annual_counts as (

    select
        zip_code,
        incident_year,
        count(*) as incident_count

    from crimes

    group by
        zip_code,
        incident_year

),

demographics as (

    select
        zip_code,
        total_population

    from {{ ref('int_zip_demographics') }}

),

joined as (

    select
        a.zip_code,
        a.incident_year,
        a.incident_count,
        d.total_population,
        round(
            a.incident_count / nullif(d.total_population, 0) * 1000,
            2
        )                           as incidents_per_1k

    from annual_counts a
    inner join demographics d on a.zip_code = d.zip_code

)

select
    zip_code,
    incident_year,
    incident_count,
    total_population,
    incidents_per_1k

from joined