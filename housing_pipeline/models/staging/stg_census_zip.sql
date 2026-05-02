with source as (
    select * from {{ source('raw', 'census_zip') }}
),

msa_filter as (
    select zip_code from {{ ref('nashville_valid_zips') }}
),

staged as (
    select
        s.zcta                       as zip_code,
        s.median_household_income,
        s.poverty_count,
        s.total_population,
        s.vintage_year
    from source s
    inner join msa_filter m
        on s.zcta = m.zip_code
)

select
    zip_code,
    median_household_income,
    poverty_count,
    total_population,
    vintage_year
from staged