-- models/intermediate/int_market_activity.sql

with redfin as (

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

    from {{ ref('stg_redfin') }}

),

monthly as (

    select
        zip_code,
        date_trunc('month', period_end)     as period_month,
        median(median_dom)                  as median_dom,
        median(inventory)                   as median_inventory,
        median(avg_sale_to_list)            as median_avg_sale_to_list,
        median(months_of_supply)            as median_months_of_supply,
        median(median_sale_price)           as median_sale_price,
        median(homes_sold)                  as median_homes_sold,
        median(new_listings)                as median_new_listings,
        count(*)                            as week_count

    from redfin

    group by
        zip_code,
        date_trunc('month', period_end)

)

select
    zip_code,
    period_month,
    median_dom,
    median_inventory,
    median_avg_sale_to_list,
    median_months_of_supply,
    median_sale_price,
    median_homes_sold,
    median_new_listings,
    week_count

from monthly