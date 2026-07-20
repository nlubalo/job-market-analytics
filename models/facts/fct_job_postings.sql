-- models/facts/fct_job_postings.sql
-- ===========================================================================
-- Fact: job postings
-- Grain: one row per unique jop posting (by job_id)

-- Incremental mergr on job_id - int_jobs_enriched alreday deduplicates, so this model is a clean 
-- projection with FK validation and date key generation

--
-- Sill flaga are not in this table - they live in fct_job_postings_skills ( one row per job/skill pair)
-- =========================================================================================================

{{
    config(
        materialized='incremental',
        unique_key ='job_id',
        incremental_strategy='merge',
        tags=['facts']
    )
}}

with source as (
    select * from {{ ref('int_jobs_enriched') }}

    {% if is_incremental() %}
        where last_ingested_at > (select max(last_ingested_at) from {{ this }})
    {% endif %}

),

-- =============================================================
-- FK validation
-- Confirm every FK resolves to a real dimension member.
-- If the key is missing from the dim, fall back to the
-- unknown member (-1 or '-1') rather than leaving a dangling
-- reference that breaks joins silently.
-- =============================================================

validated as (
    select
        s.*,

        -- job_title_key: fall back to -1 if not in dim
        case
            when d.job_title_key is not null then s.job_title_key
            else cast(-1 as bigint)
        end as job_title_key_validated,

        -- location_key: fall back to '-1' if not in dim
        case
            when l.location_key is not null                 then s.location_key
            else '-1'
        end                                                 as location_key_validated,


        -- company_key: fall back to '-1' if not in dim
        case
            when c.company_key is not null                  then s.company_key
            else '-1'
        end                                                 as company_key_validated
    
    from source s

    left join {{ ref('dim_job_title') }} d
        on s.job_title_key = d.job_title_key

    left join {{ ref('dim_location') }} l
        on s.location_key = l.location_key

    left join {{ ref('dim_company') }} c
        on s.company_key = c.company_key
)

select
    job_id,
    -- ---------------------------------------------------------
    -- Foreign keys
    -- ---------------------------------------------------------
    job_title_key_validated                                 as job_title_key,
    location_key_validated                                  as location_key,
    company_key_validated                                   as company_key,
    date_posted_key,
    date_first_seen_key,
    cast(date_format(
            to_date(last_seen_at), 'yyyyMMdd'
        ) as int)                                               as date_last_seen_key,

    employment_type,
    work_arrangement,
    seniority_level,
    seniority_rank,

    has_salary_disclosed,
    salary_min_annualized,
    salary_max_annualized,
    salary_midpoint_annual,
    --salary_currency,

    days_active,
    posting_count,

    location_quality,
    record_quality,
    is_title_unknown,
    is_company_unknown,

    first_seen_at,
    last_seen_at,
    last_ingested_at

from validated


