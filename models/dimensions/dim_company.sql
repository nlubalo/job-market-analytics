-- models/dimensions/dim_company.sql
-- =============================================================
-- dim_company: conformed company dimension
-- Grain: one row per distinct normalized company name
--
-- Source: int_jobs_enriched only — the salary endpoint does
-- not return employer information, so this dimension is
-- built entirely from job postings data.
--
-- Normalization: company_name_clean and company_key are
-- already computed in int_jobs_enriched via the inline
-- normalization expression. This model deduplicates and
-- enriches those values.
--
-- Key type: STRING (md5 hash) — consistent with how
-- company_key is generated in int_jobs_enriched
-- =============================================================

{{
    config(
        materialized='table',
        tags=['dimensions']
    )
}}

with source as (
    select
        company_key,
        company_name_clean,
        company_website,
        company_logo,
        country_clean,
        last_ingested_at
    from {{ ref('int_jobs_enriched') }}
    where company_key != '-1'

),

-- =============================================================
-- Deduplicate: one row per company_key
-- Same company may appear across many postings — pick the
-- most complete record as the representative row
-- =============================================================

ranked as (
    select
        company_key,
        company_name_clean,
        company_website,
        company_logo,
        country_clean,
        count (*) over (
            partition by company_key
        ) as total_postings,

        -- First and last seen dates — useful for understanding
        -- which companies are actively hiring vs stale
        min(last_ingested_at) over (
            partition by company_key
        )                                                   as first_seen_at,

        max(last_ingested_at) over (
            partition by company_key
        )                                                   as last_seen_at,
        -- Pick the most complete record per company:
        -- prefer rows with website, logo, and type populated
        row_number() over (
            partition by company_key
            order by
                case when company_website  is not null then 0 else 1 end,
                case when company_logo is not null then 0 else 1 end,
                last_ingested_at desc
        )                                                   as _row_num

    from source
),
deduped as (
    select
        company_key,
        company_name_clean,
        company_website,
        company_logo,
        country_clean,
        total_postings,
        first_seen_at,
        last_seen_at
    from ranked
    where _row_num =1
),
enriched as (
    select
        company_key,
        company_name_clean,
        company_website,
        company_logo,
        country_clean,
        case
            when trim(upper(country_clean)) in ('KENYA', 'KE')
                                                            then 'Kenya'
            when trim(upper(country_clean)) in ('UNITED KINGDOM', 'UK', 'GB')
                                                            then 'UK'
            when trim(upper(country_clean)) in ('UNITED STATES', 'US', 'USA')
                                                            then 'US'
            when country_clean is null                     then 'Unknown'
            else 'Other'
        end                                                 as hq_market,

        case
            when total_postings >= 50                       then 'High (50+)'
            when total_postings >= 20                       then 'Active (20–49)'
            when total_postings >= 5                        then 'Moderate (5–19)'
            else                                                 'Low (1–4)'
        end                                                 as posting_volume_band,

        -- ---------------------------------------------------------
        -- Measures
        -- ---------------------------------------------------------
        total_postings,
        cast(first_seen_at as date)                         as first_seen_date,
        cast(last_seen_at as date)                          as last_seen_date,

        datediff(
            cast(last_seen_at as date),
            cast(first_seen_at as date)
        )                                                   as days_posting_active

    from deduped


),
-- Explicit unknown member
unknown_member as (

    select
        '-1'                                                as company_key,
        'Unknown'                                           as company_name_clean,
        cast(null as string)                                as company_website,
        cast(null as string)                                as company_logo,
        --'Unknown'                                           as employer_type,
        cast(null as string)                                as country_clean,
        'Unknown'                                           as hq_market,
        'Unknown'                                           as posting_volume_band,
        ---'low'                                               as profile_completeness,
        cast(0 as bigint)                                   as total_postings,
        cast(null as date)                                  as first_seen_date,
        cast(null as date)                                  as last_seen_date,
        cast(0 as int)                                      as days_posting_active

)
select
    company_key,
    company_name_clean,
    company_website,
    company_logo,
    country_clean,
    hq_market,
    posting_volume_band,
    total_postings,
    first_seen_date,
    last_seen_date,
    days_posting_active
from enriched

union all

select * from unknown_member
order by total_postings desc, company_name_clean