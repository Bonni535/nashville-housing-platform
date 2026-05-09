-- models/staging/stg_building_permits.sql
--
-- Staging model for Nashville Metro Codes building permits.
-- Applies MSA zip filter via inner join — same pattern as other staging models.
-- Drops ingested_at infrastructure column per staging layer convention.
--
-- Deduplication: Metro Codes ArcGIS API can return duplicate permit_number
-- values across incremental loads (amended records or overlapping pagination
-- windows). QUALIFY ROW_NUMBER() retains the most recently issued record per
-- permit_number. 33 duplicates observed as of Phase 8.
--
-- Source note: Metro Codes covers Davidson County only.
-- Suburban MSA zips (Williamson, Rutherford, Wilson, Sumner) will have no
-- matching rows — expected, handled by neutral imputation in int_permit_activity
-- and fct_opportunity_score.
--
-- Grain: one row per permit issued (permit_number is the natural key)
-- Range: rolling ~3-year window maintained by Metro Codes (currently 2023–2026)
-- Downstream: int_permit_activity

with source as (

    select
        permit_number,
        permit_type,
        date_issued,
        zip_code,
        construction_cost

    from {{ source('raw', 'building_permits') }}

),

msa_filter as (

    select zip_code
    from {{ ref('nashville_valid_zips') }}

),

deduplicated as (

    select
        s.permit_number,
        s.permit_type,
        s.date_issued,
        s.zip_code,
        s.construction_cost

    from source s
    inner join msa_filter m on s.zip_code = m.zip_code

    qualify row_number() over (
        partition by s.permit_number
        order by s.date_issued desc
    ) = 1

)

select
    permit_number,
    permit_type,
    date_issued,
    zip_code,
    construction_cost

from deduplicated