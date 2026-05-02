with source as (
    select * from {{ source('raw', 'redfin_metrics') }}
),

msa_filter as (
    select zip_code from {{ ref('nashville_valid_zips') }}
),

staged as (
    select
        s.zip_code,
        cast(s.period_end as date)   as period_end,
        s.median_dom,
        s.inventory,
        s.avg_sale_to_list,
        s.months_of_supply,
        s.median_sale_price,
        s.homes_sold,
        s.new_listings
    from source s
    inner join msa_filter m
        on s.zip_code = m.zip_code
)

select
    zip_code,
    period_end,
    median_dom,
    inventory,
    avg_sale_to_list,
    months_of_supply,
    median_sale_price,
    homes_sold,
    new_listings
from staged