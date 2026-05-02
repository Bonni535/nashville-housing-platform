-- models/intermediate/int_zip_demographics.sql

with census as (

    select
        zip_code,
        median_household_income,
        poverty_count,
        total_population,
        vintage_year

    from {{ ref('stg_census_zip') }}

    where vintage_year = 2023

),

regions as (

    select
        zip_code,
        nashville_region,
        county_name,
        county_fips

    from {{ ref('nashville_zip_regions') }}

),

joined as (

    select
        c.zip_code,
        r.nashville_region,
        r.county_name,
        r.county_fips,
        c.median_household_income,
        c.poverty_count,
        c.total_population,
        round(
            c.poverty_count / nullif(c.total_population, 0),
            4
        )                           as poverty_rate,
        c.vintage_year

    from census c
    inner join regions r on c.zip_code = r.zip_code

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

from joined