-- models/staging/stg_crime_incidents.sql
--
-- Staging model for MNPD crime incidents filtered to Nashville MSA zips.
-- Applies MSA zip filter via inner join — same pattern as other staging models.
--
-- crime_category: classifies each incident into one of five buckets.
--   Violent      — assault, robbery, kidnapping, homicide, weapon offenses
--   Property     — theft, burglary, vandalism, fraud, vehicle theft
--   Drug         — drug possession, paraphernalia, controlled substances
--   Administrative — non-criminal entries: police inquiry, lost/found property,
--                   transport, natural/accidental deaths, civil cases, overdoses.
--                   These inflate zip-level crime rates and are excluded from
--                   int_crime_index and int_crime_by_category aggregations.
--   Other        — domestic offenses, harassment, contempt, trespass, etc.
--
-- Source note: MNPD covers Davidson County only.
-- Suburban MSA zips have no rows — expected, handled by neutral imputation
-- in fct_opportunity_score.
--
-- Grain: one row per incident
-- Downstream: int_crime_index, int_crime_by_category

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
        s.zip_code,

        -- ── Crime category classification ──────────────────────────────────
        -- Evaluated top-to-bottom — first match wins.
        -- Administrative entries are flagged here so downstream models can
        -- exclude them cleanly with a single WHERE clause.
        case
            -- Violent: assault, robbery, kidnapping, homicide, weapon offenses
            when s.incident_type ilike any (
                '%asslt%',
                '%assault%',
                '%robbery%',
                '%kidnap%',
                '%homicide%',
                '%murder%',
                '%rape%',
                '%sexual assault%',
                '%weapon offense%',
                '%aggrav asslt%'
            ) then 'Violent'

            -- Property: theft, burglary, vandalism, vehicle theft, fraud
            when s.incident_type ilike any (
                '%theft%',
                '%larceny%',
                '%larc%',
                '%burglary%',
                '%burgl%',
                '%vandal%',
                '%damage prop%',
                '%shoplifting%',
                '%fraud%'
            ) then 'Property'

            -- Drug: possession, paraphernalia, controlled substances
            when s.incident_type ilike any (
                '%drug%',
                '%marijuana%',
                '%controlled substance%',
                '%cocaine%',
                '%narcotic%'
            ) then 'Drug'

            -- Administrative: non-criminal — exclude from crime rate calculations
            when s.incident_type ilike any (
                '%police inquiry%',
                '%lost property%',
                '%found property%',
                '%recovery, stolen%',
                '%transport%',
                '%death natural%',
                '%death unnatural%',
                '%accidental injury%',
                '%civil case%',
                '%suicide%',
                '%overdose%',
                '%test only%'
            ) then 'Administrative'

            -- Other: domestic offenses, harassment, contempt, trespass, etc.
            else 'Other'

        end as crime_category

    from source s
    inner join msa_filter m
        on s.zip_code = m.zip_code

)

select
    incident_occurred,
    incident_type,
    zip_code,
    crime_category

from staged