with source as (
    select * from {{ source('raw', 'zillow_metrics') }}
),

msa_filter as (
    select zip_code from {{ ref('nashville_valid_zips') }}
),

staged as (
    select
        s.zip_code,
        cast(s.period_month as date) as period_month,
        s.value,
        s.metric_type
    from source s
    inner join msa_filter m
        on s.zip_code = m.zip_code
    where s.value is not null
)

select
    zip_code,
    period_month,
    value,
    metric_type
from staged