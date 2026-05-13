select
    zip_code,
    vintage_year,
    count(*) as row_count

from {{ ref('stg_census_zip') }}

group by
    zip_code,
    vintage_year

having count(*) > 1