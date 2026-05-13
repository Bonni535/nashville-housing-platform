select
    zip_code,
    incident_year,
    count(*) as row_count

from {{ ref('int_crime_index') }}

group by
    zip_code,
    incident_year

having count(*) > 1