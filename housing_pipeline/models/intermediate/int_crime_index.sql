-- models/intermediate/int_crime_index.sql

with crimes as (

    select
        zip_code,
        year(incident_occurred)     as incident_year,
        incident_type

    from {{ ref('stg_crime_incidents') }}

),

annual_counts as (

    select
        zip_code,
        incident_year,
        count(*)                    as incident_count

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