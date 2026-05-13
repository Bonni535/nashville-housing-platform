-- The grain of stg_census_county is county_fips + vintage_year.
-- Fail if any combination appears more than once.

select
    county_fips,
    vintage_year,
    count(*) as row_count

from {{ ref('stg_census_county') }}

group by
    county_fips,
    vintage_year

having count(*) > 1