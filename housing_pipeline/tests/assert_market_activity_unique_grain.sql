-- tests/assert_market_activity_unique_grain.sql

select
    zip_code,
    period_month,
    count(*) as row_count

from {{ ref('int_market_activity') }}

group by
    zip_code,
    period_month

having count(*) > 1