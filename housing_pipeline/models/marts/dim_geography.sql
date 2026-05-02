-- models/marts/dim_geography.sql

with demographics as (

    select
        zip_code,
        nashville_region,
        county_name,
        county_fips,
        median_household_income,
        poverty_count,
        total_population,
        poverty_rate,
        vintage_year

    from {{ ref('int_zip_demographics') }}

)

select
    zip_code,
    nashville_region,
    county_name,
    county_fips,
    median_household_income,
    poverty_count,
    total_population,
    poverty_rate,
    vintage_year

from demographics