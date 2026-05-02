with source as (
    select * from {{ source('raw', 'crime_incidents') }}
),

msa_filter as (
    select zip_code from {{ ref('nashville_valid_zips') }}
),

staged as (
    select
        s.incident_occurred,
        s.incident_type,
        s.zip_code
    from source s
    inner join msa_filter m
        on s.zip_code = m.zip_code
)

select
    incident_occurred,
    incident_type,
    zip_code
from staged