-- models/facts/fct_salary_estimates.sql
-- ===================================================
-- Fact: salary estimates
-- Grain: one row per (title_normalized, location_key,
--         salary_currency, ingestion_date)
--
-- Carries both total compensation and base salary columns
-- so mart layer can choose which to display without
-- recomputing annualization.
--
-- Snapshot fact — multiple rows per (title, location) pair
-- across different ingestion dates enables salary trend
-- analysis over time.
--
-- Incremental merge on salary_estimate_key.
-- =============================================================

{{
    config(
        materialized='incremental',
        unique_key='salary_estimate_key',
        incremental_strategy='merge',
        tags=['facts']
    )
}}
with source as (
    select * from {{ ref('int_salary_estimates') }}

    {% if is_incremental() %}
        where _ingested_at > (select max(_ingested_at) from {{ this }})

    {% endif %}
),
-- =====================================================================
-- FK validationßß
-- Same apptern as fct_job_postings - falll back to unknown
-- memebr if key not found in dimension
-- ========================================================================

validated as (
    select
        s.salary_estimate_key,
        s.date_key,
        s.job_title,
        s.title_normalized,
        s.seniority_level,
        s.location,
        s.city,
        s.region,
        s.country,
        s.location_resolution,
        s.salary_period,
        s.salary_currency,
        s.publisher_name,
        s.salaries_updated_at,
        s.salary_min_annual,
        s.salary_median_annual,
        s.salary_max_annual,
        s.salary_midpoint_annual,
        s.salary_range_width,
        s.salary_range_pct,
        s.salary_bucket,
        s.base_salary_min_annual,
        s.base_salary_median_annual,
        s.base_salary_max_annual,
        s.base_salary_bucket,
        s.additional_pay_median_annual,
        s.additional_pay_pct,
        s.salary_count,
        s.confidence,
        s.estimate_reliability,
        s.record_quality,
        s.is_title_unknown,
        s.is_location_unknown,
        s.ingestion_date,
        s._ingested_at,

        case
            when d.job_title_key is not null
            then cast(s.job_title_key as bigint)
            else cast(-1 as bigint)
        end                                                 as job_title_key_validated,


        case
            when l.location_key is not null
            then cast(s.location_key as string)
            else cast('-1' as string)            -- string, not bigint
        end                                      as location_key_validated
    from source s

    left join {{ ref('dim_job_title') }} d
        on s.job_title_key = d.job_title_key

    left join {{ ref('dim_location') }} l
        on s.location_key = l.location_key

)

select
    salary_estimate_key,
    job_title_key_validated                                 as job_title_key,
    location_key_validated                                  as location_key,
    date_key,

    seniority_level,
    salary_period,
    salary_currency,
    location_resolution,
    publisher_name,

    -- ---------------------------------------------------------
    -- Total compensation measures
    -- Base + additional pay combined
    -- Use for overall market rate comparisons
    -- ---------------------------------------------------------
    salary_min_annual,
    salary_median_annual,
    salary_max_annual,
    salary_midpoint_annual,
    salary_range_width,
    salary_range_pct,
    salary_bucket,

    base_salary_min_annual,
    base_salary_median_annual,
    base_salary_max_annual,
    base_salary_bucket,

    additional_pay_median_annual,
    additional_pay_pct,

    -- ---------------------------------------------------------
    -- Data quality and reliability
    -- ---------------------------------------------------------
    salary_count,
    confidence,
    estimate_reliability,
    record_quality,
    is_title_unknown,
    is_location_unknown,

    -- When JSearch last refreshed this estimate
    salaries_updated_at,

    -- ---------------------------------------------------------
    -- Pipeline metadata
    -- ---------------------------------------------------------
    ingestion_date,
    _ingested_at

from validated



