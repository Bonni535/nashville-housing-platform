-- models/staging/stg_building_permits.sql
--
-- Staging model for Nashville Metro Codes building permits.
-- Applies MSA zip filter via inner join — same pattern as other staging models.
-- Drops ingested_at infrastructure column per staging layer convention.
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

)

select
    s.permit_number,
    s.permit_type,
    s.date_issued,
    s.zip_code,
    s.construction_cost

from source s
inner join msa_filter m on s.zip_code = m.zip_code