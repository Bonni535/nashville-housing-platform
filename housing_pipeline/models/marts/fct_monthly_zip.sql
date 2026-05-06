-- models/marts/fct_monthly_zip.sql
--
-- Wide fact table joining Redfin market activity, Zillow valuation metrics,
-- and FRED 30-year mortgage rate at zip + month grain.
--
-- Spine: int_market_activity (Redfin weekly aggregated to monthly zip level)
-- Zillow: pivoted from long to wide, date-truncated to month-start for join
-- FRED:   weekly rates averaged to monthly — national series, same rate per zip

with market as (

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

    from {{ ref('int_market_activity') }}

),

zillow_pivot as (

    select
        zip_code,
        date_trunc('month', period_month)                  as period_month,
        max(case when metric_type = 'ZHVI' then value end) as zhvi,
        max(case when metric_type = 'ZORI' then value end) as zori,
        max(case when metric_type = 'ZHVF' then value end) as zhvf

    from {{ ref('stg_zillow') }}

    group by
        zip_code,
        date_trunc('month', period_month)

),

fred_monthly as (

    -- Average weekly rates within each calendar month.
    -- Typically 4-5 observations per month depending on Thursday count.
    select
        date_trunc('month', observation_date)   as period_month,
        round(avg(rate), 2)                     as avg_mortgage_rate

    from {{ ref('stg_fred_mortgage_rates') }}

    group by 1

),

joined as (

    select
        m.zip_code,
        m.period_month,
        m.median_dom,
        m.median_inventory,
        m.median_avg_sale_to_list,
        m.median_months_of_supply,
        m.median_sale_price,
        m.median_homes_sold,
        m.median_new_listings,
        m.week_count,
        z.zhvi,
        z.zori,
        z.zhvf,
        f.avg_mortgage_rate

    from market m
    left join zillow_pivot z
        on  m.zip_code     = z.zip_code
        and m.period_month = z.period_month
    left join fred_monthly f
        on m.period_month  = f.period_month

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
    week_count,
    zhvi,
    zori,
    zhvf,
    avg_mortgage_rate

from joined