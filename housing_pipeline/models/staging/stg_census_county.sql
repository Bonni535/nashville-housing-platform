with source as (
    select * from {{ source('raw', 'census_county') }}
),

staged as (
    select
        s.county_fips,
        s.county_name,
        s.median_household_income,
        s.poverty_count,
        s.total_population,
        s.vintage_year
    from source s
)

select
    county_fips,
    county_name,
    median_household_income,
    poverty_count,
    total_population,
    vintage_year
from staged