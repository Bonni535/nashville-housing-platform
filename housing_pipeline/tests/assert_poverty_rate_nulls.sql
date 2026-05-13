-- Poverty rate is null only for zero-population zips.
-- Fail if null count exceeds 5 (current known count is 2).

select count(*) as null_count
from {{ ref('int_zip_demographics') }}
where poverty_rate is null
having count(*) > 5