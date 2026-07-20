-- models/intermediate/int_salary_estimates.sql
-- =============================================================
-- Intermediate: cleaned and enriched salary estimates
-- from the JSearch estimated salary endpoint.
--
-- Grain: one row per (title_normalized, location_key,
--         salary_currency, ingestion_date)

-- Responsibilities:
--   1. Normalize job title (same macro as int_jobs_enriched)
--   2. Parse and normalize location from single string field
--   3. Annualize total comp and base salary figures
--   4. Currency-aware salary bucketing on median
--   5. Assign title_key for FK to dim_job_title
--   6. Assign location_key for FK to dim_location
--   7. Reliability scoring from confidence + salary_count
--   8. Deduplication within same ingestion_date
-- =============================================================

{{
    config(
        materialized='incremental',
        unique_key=['title_normalized', 'location_key', 'salary_currency', 'ingestion_date'],
        incremental_strategy='merge',
        tags=['intermediate', 'salary_estimates']
    )
}}

with source as (
    select *
    from {{ ref('stg_salary_history') }}

    {% if is_incremental() %}
        where _ingested_at >= (select max(_ingested_at) from {{ this }})
    {% endif %}
),

-- ============================================================
-- STEP 1: Title normalization
-- Reuse the same macro as the int_jobs_enriched so that
-- title nomalized values are identical across both models
-- this is what makes the FK to dim_job_title conformed
title_normalized as(
    select
    *,
    {{ normalize_job_title('job_title') }} as title_normalized,
    case
        when job_title is null
            or length(trim(job_title)) <= 2
            then cast(-1 as bigint)
        else cast(
            conv(substr(md5({{ normalize_job_title('job_title') }}), 1, 8), 16, 10) as bigint
        ) end as job_title_key,

   {{ derive_seniority_level('job_title') }} as seniority_level
   from source

),
-- =============================================================
-- STEP 2: Location parsing
-- location_raw is a single string e.g. "Nairobi, Kenya"
-- or "London, England, UK" — split on comma.
-- First segment = city, last segment = country,
-- middle segment(s) = region.
-- Location key hashes (city, country) using identical logic
-- to int_jobs_enriched so FK to dim_location is conformed.
-- =============================================================

location_parsed as (
    select
        *,
        initcap(trim(
            regexp_replace(
                split_part(location, ',', 1),
                '^\\s+|\\s+$', ''
            )
        )) as city,
        initcap(trim(
            regexp_replace(
                split(location, ',')[size(split(location, ',')) - 1],
                '^\\s+|\\s+$', ''
            )
        )) as country,
        case
            when size(split(location, ',')) >= 3
            then initcap(trim(split_part(location, ',', 2)))
            else null
        end as region,

        case
            when location is null
            then '-1'
            when size(split(location, ',')) = 1
            -- Single segment: treat as country-only, city = null
            then md5(concat('||', lower(trim(location))))
            else md5(
                concat(
                    lower(trim(split(location, ',')[0])),
                    '||',
                    lower(trim(
                        split(location, ',')[size(split(location, ',')) - 1]
                    ))
                )
            )
        end                                                             as location_key,

        -- Resolution indicates how precise the location is
        -- city = has at least city + country
        -- country = only one segment, treated as country-level
        -- missing = null location_raw
        case
            when location is null                           then 'missing'
            when size(split(location, ',')) >= 2            then 'city'
            else                                                     'country'
        end                                                     as location_resolution

    from title_normalized
),

-- =============================================================
-- STEP 3: Salary annualization — total compensation
-- Annualize all three total comp figures (min, median, max)
-- Period values from staging: HOUR, MONTH, YEAR
-- Multipliers: HOUR × 2080, MONTH × 12, YEAR × 1
-- DAY and WEEK included defensively in case they appear
-- =============================================================

salary_annualized as (

    select
        * except (salary_currency, salary_period),

        -- ---------------------------------------------------------
        -- Total compensation (base + additional pay)
        -- ---------------------------------------------------------
        case upper(salary_period)
            when 'HOUR'  then round(min_salary * 2080, 0)
            when 'DAY'   then round(min_salary * 260,  0)
            when 'WEEK'  then round(min_salary * 52,   0)
            when 'MONTH' then round(min_salary * 12,   0)
            when 'YEAR'  then min_salary
            else null
        end   as salary_median_annual,
     case upper(salary_period)
            when 'HOUR'  then round(median_salary * 2080, 0)
            when 'DAY'   then round(median_salary * 260,  0)
            when 'WEEK'  then round(median_salary * 52,   0)
            when 'MONTH' then round(median_salary * 12,   0)
            when 'YEAR'  then median_salary
            else null
        end  as salary_min_annual,
    case upper(salary_period)
            when 'HOUR'  then round(max_salary * 2080, 0)
            when 'DAY'   then round(max_salary * 260,  0)
            when 'WEEK'  then round(max_salary * 52,   0)
            when 'MONTH' then round(max_salary * 12,   0)
            when 'YEAR'  then max_salary
            else null
        end as salary_max_annual,

     -- ---------------------------------------------------------
    -- Base salary only
    -- Strips out bonus/commission/equity for like-for-like
    -- comparison across markets where additional pay varies
    -- ---------------------------------------------------------
    case upper(salary_period)
        when 'HOUR'  then round(min_base_salary * 2080, 0)
        when 'DAY'   then round(min_base_salary * 260,  0)
        when 'WEEK'  then round(min_base_salary * 52,   0)
        when 'MONTH' then round(min_base_salary * 12,   0)
        when 'YEAR'  then min_base_salary
        else null
    end                                                     as base_salary_min_annual,

    case upper(salary_period)
            when 'HOUR'  then round(median_base_salary * 2080, 0)
            when 'DAY'   then round(median_base_salary * 260,  0)
            when 'WEEK'  then round(median_base_salary * 52,   0)
            when 'MONTH' then round(median_base_salary * 12,   0)
            when 'YEAR'  then median_base_salary
            else null
        end                                                     as base_salary_median_annual,
    case upper(salary_period)
            when 'HOUR'  then round(max_base_salary * 2080, 0)
            when 'DAY'   then round(max_base_salary * 260,  0)
            when 'WEEK'  then round(max_base_salary * 52,   0)
            when 'MONTH' then round(max_base_salary * 12,   0)
            when 'YEAR'  then max_base_salary
            else null
        end                                                     as base_salary_max_annual,

        -- ---------------------------------------------------------
        -- Additional pay (bonus, commission, equity)
        -- Derived as total comp minus base — cross-check against
        -- raw additional pay fields for consistency
        -- ---------------------------------------------------------
        case upper(salary_period)
            when 'HOUR'  then round(median_additional_pay * 2080, 0)
            when 'DAY'   then round(median_additional_pay * 260,  0)
            when 'WEEK'  then round(median_additional_pay * 52,   0)
            when 'MONTH' then round(median_additional_pay * 12,   0)
            when 'YEAR'  then median_additional_pay
            else null
        end                                                     as additional_pay_median_annual,

        coalesce(salary_currency, 'USD')                    as salary_currency,
        salary_period                                       as salary_period

    from location_parsed
),

-- =============================================================
-- STEP 4: Derived salary measures
-- Midpoint, range width, range pct, additional pay share —
-- computed once here rather than in every downstream query
-- =============================================================

salary_measures as (

    select
        *,

        -- Midpoint as sanity check against median
        -- Large gap between midpoint and median signals skew
        round(
            (coalesce(salary_min_annual, 0) + coalesce(salary_max_annual, 0)) / 2,
            0
        )                                                       as salary_midpoint_annual,

        -- Range width: absolute spread of the estimate
        case
            when salary_min_annual is not null
             and salary_max_annual is not null
            then salary_max_annual - salary_min_annual
            else null
        end as salary_range_width,

        -- Range as % of median: >100% = very noisy estimate
        -- Use this to filter to reliable estimates in mart layer
        case
            when salary_median_annual is not null
             and salary_median_annual > 0
             and salary_min_annual    is not null
             and salary_max_annual    is not null
            then round(
                (
                    (salary_max_annual - salary_min_annual)
                    / salary_median_annual
                ) * 100,
                1
            )
            else null
        end                                                     as salary_range_pct,

        -- Additional pay as % of total comp median
        -- Tells you how bonus-heavy a role/market is
        -- High % in US tech vs lower in KE market is a
        -- meaningful signal for your benchmarking story
        case
            when salary_median_annual is not null
             and salary_median_annual > 0
             and additional_pay_median_annual is not null
            then round(
                (additional_pay_median_annual / salary_median_annual) * 100,
                1
            )
            else null
        end                                                     as additional_pay_pct

    from salary_annualized

),

-- =============================================================
-- STEP 5: Currency-aware salary bucketing
-- Buckets on total comp median — same thresholds as
-- int_jobs_enriched so salary_bucket is comparable when
-- joining postings to estimates in the mart layer
-- =============================================================

salary_bucketed as (

    select
        *,

        case salary_currency

            when 'USD' then
                case
                    when salary_median_annual is null           then 'Undisclosed'
                    when salary_median_annual < 50000           then 'Under $50K'
                    when salary_median_annual < 80000           then '$50K–$80K'
                    when salary_median_annual < 120000          then '$80K–$120K'
                    when salary_median_annual < 180000          then '$120K–$180K'
                    else                                             'Over $180K'
                end

             when 'GBP' then
                case
                    when salary_median_annual is null           then 'Undisclosed'
                    when salary_median_annual < 35000           then 'Under £35K'
                    when salary_median_annual < 60000           then '£35K–£60K'
                    when salary_median_annual < 90000           then '£60K–£90K'
                    when salary_median_annual < 130000          then '£90K–£130K'
                    else                                             'Over £130K'
                end

            when 'KES' then
                case
                    when salary_median_annual is null           then 'Undisclosed'
                    when salary_median_annual < 600000          then 'Under KES 600K'
                    when salary_median_annual < 1200000         then 'KES 600K–1.2M'
                    when salary_median_annual < 2400000         then 'KES 1.2M–2.4M'
                    when salary_median_annual < 4800000         then 'KES 2.4M–4.8M'
                    else                                             'Over KES 4.8M'
                end

            else 'Undisclosed'

        end                                                     as salary_bucket,

        -- Base salary bucket — same thresholds, separate column
        -- so mart layer can choose which to display

        case salary_currency

            when 'USD' then
                case
                    when base_salary_median_annual is null      then 'Undisclosed'
                    when base_salary_median_annual < 50000      then 'Under $50K'
                    when base_salary_median_annual < 80000      then '$50K–$80K'
                    when base_salary_median_annual < 120000     then '$80K–$120K'
                    when base_salary_median_annual < 180000     then '$120K–$180K'
                    else                                             'Over $180K'
                end

            when 'GBP' then
                case
                    when base_salary_median_annual is null      then 'Undisclosed'
                    when base_salary_median_annual < 35000      then 'Under £35K'
                    when base_salary_median_annual < 60000      then '£35K–£60K'
                    when base_salary_median_annual < 90000      then '£60K–£90K'
                    when base_salary_median_annual < 130000     then '£90K–£130K'
                    else                                             'Over £130K'
                end

            when 'KES' then
                case
                    when base_salary_median_annual is null      then 'Undisclosed'
                    when base_salary_median_annual < 600000     then 'Under KES 600K'
                    when base_salary_median_annual < 1200000    then 'KES 600K–1.2M'
                    when base_salary_median_annual < 2400000    then 'KES 1.2M–2.4M'
                    when base_salary_median_annual < 4800000    then 'KES 2.4M–4.8M'
                    else                                             'Over KES 4.8M'
                end

            else 'Undisclosed'

        end                                                     as base_salary_bucket

      from salary_measures

),
-- =============================================================
-- STEP 7: Deduplication
-- Keep one row per (title_normalized, location_key,
-- salary_currency, ingestion_date) — the snapshot grain.
-- Multiple rows on the same day from repeated API calls
-- for the same query params get deduped here; rows across
-- different days are kept intentionally for trend analysis.
-- =============================================================


reliability_scored as (

    select
        *,

        case
            when lower(confidence) = 'high'
             and salary_count >= 30                         then 'high'
            when lower(confidence) = 'high'
             and salary_count >= 10                         then 'medium'
            when lower(confidence) = 'high'
             and salary_count < 10                          then 'medium'
            when lower(confidence) = 'medium'
             and salary_count >= 10                         then 'medium'
            when lower(confidence) = 'low'                  then 'low'
            when salary_count < 5                           then 'low'
            when salary_median_annual is null                   then 'low'
            when salary_range_pct > 100                         then 'low'
            else                                                     'medium'
        end                                                     as estimate_reliability

    from salary_bucketed

),

-- =============================================================
-- STEP 7: Deduplication
-- Keep one row per (title_normalized, location_key,
-- salary_currency, ingestion_date) — the snapshot grain.
-- Multiple rows on the same day from repeated API calls
-- for the same query params get deduped here; rows across
-- different days are kept intentionally for trend analysis.
-- =============================================================

deduped as (

    select
        *,

        -- Surrogate key: stable hash of the natural grain
        md5(
            concat_ws('||',
                coalesce(title_normalized,  'unknown'),
                coalesce(location_key,      '-1'),
                coalesce(salary_currency,   'USD'),
                cast(ingestion_date as string)
            )
        )                                                       as salary_estimate_key,
        row_number() over (
            partition by
                title_normalized,
                location_key,
                salary_currency,
                ingestion_date
            order by
                -- Prefer higher confidence and higher count
                -- when multiple pulls exist for the same day
                case lower(confidence)
                    when 'high'   then 1
                    when 'medium' then 2
                    else               3
                end asc,
                coalesce(salary_count, 0) desc,
                _ingested_at desc
        )                                                       as _row_num

        from reliability_scored

)
select

        -- ---------------------------------------------------------
        -- Primary key
        -- ---------------------------------------------------------
        salary_estimate_key,

        -- ---------------------------------------------------------
        -- Foreign keys
        -- ---------------------------------------------------------
        job_title_key,
        location_key,

        -- Date key for dim_date join (integer YYYYMMDD)
        cast(
            date_format(ingestion_date, 'yyyyMMdd')
        as int)                                                 as date_key,

        -- ---------------------------------------------------------
        -- Descriptive attributes
        -- ---------------------------------------------------------
        job_title,
        title_normalized,
        seniority_level,
        location,
        city,
        region,
        country,
        location_resolution,
        salary_period,
        salary_currency,
        publisher_name,

        -- When JSearch last refreshed this estimate —
        -- more recent = more trustworthy for current benchmarking
        salaries_updated_at,

        -- ---------------------------------------------------------
        -- Total compensation measures
        -- ---------------------------------------------------------
        salary_min_annual,
        salary_median_annual,
        salary_max_annual,
        salary_midpoint_annual,
        salary_range_width,
        salary_range_pct,
        salary_bucket,

        -- ---------------------------------------------------------
        -- Base salary measures
        -- Use these for cross-market comparison where bonus
        -- structures differ significantly (US vs KE vs UK)
        -- ---------------------------------------------------------
        base_salary_min_annual,
        base_salary_median_annual,
        base_salary_max_annual,
        base_salary_bucket,

        -- ---------------------------------------------------------
        -- Additional pay measures
        -- ---------------------------------------------------------
        additional_pay_median_annual,
        additional_pay_pct,

        -- ---------------------------------------------------------
        -- Data quality and reliability
        -- ---------------------------------------------------------
        salary_count                                       as salary_count,
        confidence                                         as confidence,
        estimate_reliability,

        case
            when job_title_key = -1                             then true
            else false
        end                                                     as is_title_unknown,

        case
            when location_key = '-1'                            then true
            else false
        end                                                     as is_location_unknown,

        -- Overall record quality: mirrors convention from
        -- int_jobs_enriched so mart layer filters consistently
        case
            when location_key   != '-1'
             and job_title_key  != -1
             and salary_median_annual is not null
             and estimate_reliability = 'high'                  then 'high'
            when salary_median_annual is not null
             and estimate_reliability != 'low'                  then 'medium'
            else                                                     'low'
        end                                                     as record_quality,
        -- ---------------------------------------------------------
        -- Pipeline metadata
        -- ---------------------------------------------------------
        ingestion_date,
        _ingested_at

    from deduped
    where _row_num = 1


