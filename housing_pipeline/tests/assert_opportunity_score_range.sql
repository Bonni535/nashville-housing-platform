-- tests/assert_opportunity_score_range.sql
-- Fails if any zip has a score outside 0-100

select
    zip_code,
    opportunity_score

from {{ ref('fct_opportunity_score') }}

where opportunity_score < 0
   or opportunity_score > 100