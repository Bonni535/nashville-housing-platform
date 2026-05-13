-- Singular test: asserts that the grain of int_permit_activity is
-- exactly one row per zip_code + permit_year combination.
-- Returns rows only when the grain is violated — dbt fails if any rows returned.

select
    zip_code,
    permit_year,
    count(*) as row_count

from {{ ref('int_permit_activity') }}

group by
    zip_code,
    permit_year

having count(*) > 1