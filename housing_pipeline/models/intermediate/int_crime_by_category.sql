-- models/intermediate/int_crime_by_category.sql
--
-- Annual crime rate per zip per crime category, normalized by population.
-- Powers the crime type filter in the Streamlit dashboard Crime section.
-- Administrative entries excluded — same policy as int_crime_index.
--
-- crime_category values: 'Violent', 'Property', 'Drug', 'Other'
-- (Administrative is excluded, never appears in output)
--
-- Grain: one row per zip_code + incident_year + crime_category
-- Downstream: dashboard/app.py load_crime_by_category()

with crimes as (

    select
        zip_code,
        year(incident_occurred) as incident_year,
        crime_category

    from {{ ref('stg_crime_incidents') }}

    where crime_category != 'Administrative'

),

category_counts as (

    select
        zip_code,
        incident_year,
        crime_category,
        count(*) as incident_count

    from crimes

    group by
        zip_code,
        incident_year,
        crime_category

),

demographics as (

    select
        zip_code,
        total_population

    from {{ ref('int_zip_demographics') }}

),

joined as (

    select
        c.zip_code,
        c.incident_year,
        c.crime_category,
        c.incident_count,
        d.total_population,
        round(
            c.incident_count / nullif(d.total_population, 0) * 1000,
            2
        )                           as incidents_per_1k

    from category_counts c
    inner join demographics d on c.zip_code = d.zip_code

)

select
    zip_code,
    incident_year,
    crime_category,
    incident_count,
    total_population,
    incidents_per_1k

from joined