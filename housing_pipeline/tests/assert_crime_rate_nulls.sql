-- incidents_per_1k is null only for zero-population zip/year combos.
-- Fail if null count exceeds 10 (current known count is 3).

select count(*) as null_count
from {{ ref('int_crime_index') }}
where incidents_per_1k is null
having count(*) > 10