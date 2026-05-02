-- models/marts/fct_opportunity_score.sql

with latest_market as (

    select
        zip_code,
        period_month,
        median_sale_price,
        median_dom,
        median_homes_sold

    from {{ ref('fct_monthly_zip') }}

    qualify row_number() over (
        partition by zip_code
        order by period_month desc
    ) = 1

),

latest_crime as (

    select
        zip_code,
        incident_year,
        incidents_per_1k

    from {{ ref('int_crime_index') }}

    qualify row_number() over (
        partition by zip_code
        order by incident_year desc
    ) = 1

),

geography as (

    select
        zip_code,
        nashville_region,
        county_name,
        county_fips,
        median_household_income,
        poverty_rate

    from {{ ref('dim_geography') }}

),

combined as (

    select
        g.zip_code,
        g.nashville_region,
        g.county_name,
        g.county_fips,
        lm.period_month             as market_as_of,
        lc.incident_year            as crime_as_of,
        lm.median_sale_price,
        lm.median_dom,
        lm.median_homes_sold,
        g.median_household_income,
        g.poverty_rate,
        lc.incidents_per_1k

    from geography g
    left join latest_market lm  on g.zip_code = lm.zip_code
    left join latest_crime lc   on g.zip_code = lc.zip_code

),

normalized as (

    select
        zip_code,
        nashville_region,
        county_name,
        county_fips,
        market_as_of,
        crime_as_of,
        median_sale_price,
        median_dom,
        median_homes_sold,
        median_household_income,
        poverty_rate,
        incidents_per_1k,

        -- affordability: lower price = higher score
        round(
            (1 - (median_sale_price - min(median_sale_price) over ())
                / nullif(max(median_sale_price) over () - min(median_sale_price) over (), 0)
            ) * 100, 2
        )                           as affordability_score,

        -- market speed: lower dom = higher score
        round(
            (1 - (median_dom - min(median_dom) over ())
                / nullif(max(median_dom) over () - min(median_dom) over (), 0)
            ) * 100, 2
        )                           as market_speed_score,

        -- activity: higher homes sold = higher score
        round(
            (median_homes_sold - min(median_homes_sold) over ())
            / nullif(max(median_homes_sold) over () - min(median_homes_sold) over (), 0)
            * 100, 2
        )                           as activity_score,

        -- income: higher income = higher score
        round(
            (median_household_income - min(median_household_income) over ())
            / nullif(max(median_household_income) over () - min(median_household_income) over (), 0)
            * 100, 2
        )                           as income_score,

        -- poverty: lower rate = higher score
        round(
            (1 - (poverty_rate - min(poverty_rate) over ())
                / nullif(max(poverty_rate) over () - min(poverty_rate) over (), 0)
            ) * 100, 2
        )                           as poverty_score,

        -- safety: lower crime = higher score (coalesce nulls to 50 — neutral for suburban zips)
        round(
            (1 - (coalesce(incidents_per_1k, avg(incidents_per_1k) over ())
                    - min(coalesce(incidents_per_1k, 0)) over ())
                / nullif(max(incidents_per_1k) over () - min(incidents_per_1k) over (), 0)
            ) * 100, 2
        )                           as safety_score

    from combined

),

scored as (

    select
        zip_code,
        nashville_region,
        county_name,
        county_fips,
        market_as_of,
        crime_as_of,
        median_sale_price,
        median_dom,
        median_homes_sold,
        median_household_income,
        poverty_rate,
        incidents_per_1k,
        affordability_score,
        market_speed_score,
        activity_score,
        income_score,
        poverty_score,
        safety_score,
        round(
            (
                coalesce(affordability_score, 50)
                + coalesce(market_speed_score, 50)
                + coalesce(activity_score, 50)
                + coalesce(income_score, 50)
                + coalesce(poverty_score, 50)
                + coalesce(safety_score, 50)
            ) / 6, 2
        )                           as opportunity_score

    from normalized

)

select
    zip_code,
    nashville_region,
    county_name,
    county_fips,
    market_as_of,
    crime_as_of,
    median_sale_price,
    median_dom,
    median_homes_sold,
    median_household_income,
    poverty_rate,
    incidents_per_1k,
    affordability_score,
    market_speed_score,
    activity_score,
    income_score,
    poverty_score,
    safety_score,
    opportunity_score

from scored