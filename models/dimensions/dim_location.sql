-- models/dimensions/dim_location.sql
-- =============================================================
-- dim_location: conformed location dimension
-- Grain: one row per distinct (city, country) combination
--
-- Shared across fct_job_postings and fct_salary_estimates.
-- Location key is a hash of (city_clean, country_clean) —
-- identical hashing logic in int_jobs_enriched and
-- int_salary_estimates ensures FK values always align.
--
-- Sources:
--   int_jobs_enriched    → city/country as separate fields
--   int_salary_estimates → city/country parsed from location_raw
--
-- Key type: STRING (md5 hash) — consistent with how location_key
-- is generated in both intermediate models
-- =============================================================

{{
    config(
        materialized='table',
        tags=['dimensions']
    )
}}

with jobs_locations as (
    select distinct
        city_clean as city,
        state_clean as region,
        country_clean as country,
        location_key,
        location_quality as location_quality_jobs
    from {{ ref('int_jobs_enriched')}}
    where location_key != '-1'
),

salary_locations as (
    select distinct
        city,
        region,
        country,
        location_key,
        location_resolution                             as location_quality_salary
    from {{ ref('int_salary_estimates')}}
    where location_key != '-1'
),

-- Union both sources so the dimension covers every location
-- referenced by either fact table — a location that only
-- appears in salary estimates still needs a dim row or
-- the FK join silently drops those rows
combined as (
    select
        city,
        region,
        country,
        location_key,
        'jobs' as source
    from jobs_locations

    union all

    select
        city,
        region,
        country,
        location_key,
        'salary' as source
    from salary_locations
),

-- Deduplicate: same location_key may appear from both sources
-- Pick the most complete record (prefer row with region populated)
deduplicated as (
    select
        location_key,
        city,
        region,
        country,

        -- Track which fact tables reference this location
        -- useful for understanding data coverage per market
        max(case when source = 'jobs'   then true else false end)
                                                            as in_job_postings,
        max(case when source = 'salary' then true else false end)
                                                            as in_salary_estimates
    from combined
    group by location_key, city, region, country
),
-- Resolve conflicts: same location_key but different city/region/country
-- values can appear if normalization produces slightly different
-- cleaned values from the two intermediate models.
-- Take the longest (most complete) value per key.
resolved as (

    select
        location_key,

        -- Prefer non-null, longest value for city
        first_value(city) over (
            partition by location_key
            order by length(coalesce(city, '')) desc
        )                                                   as city_clean,

        -- Prefer non-null, longest value for region
        first_value(region) over (
            partition by location_key
            order by length(coalesce(region, '')) desc
        )                                                   as region_clean,

         -- Country should be consistent — take longest as tiebreaker
        first_value(country) over (
            partition by location_key
            order by length(coalesce(country, '')) desc
        )                                                   as country_clean,

        in_job_postings,
        in_salary_estimates,

        row_number() over (
            partition by location_key
            order by
                length(coalesce(region, '')) desc,
                length(coalesce(city, '')) desc
        )                                                   as _row_num

    from deduplicated

),

enriched as (

    select
        location_key,
        city_clean,
        region_clean,
        country_clean,
        in_job_postings,
        in_salary_estimates,

        -- ---------------------------------------------------------
        -- Derived geographic attributes
        -- ---------------------------------------------------------

        -- Market grouping: the primary lens for
        -- Kenya / UK / US benchmarking analysis
        case
            when trim(upper(country_clean)) like 'KENYA%'
            or trim(upper(country_clean)) = 'KE'                    then 'Kenya'
            when trim(upper(country_clean)) like 'UNITED KINGDOM%'
            or trim(upper(country_clean)) in ('UK', 'GB')           then 'UK'
            when trim(upper(country_clean)) like 'UNITED STATES%'
            or trim(upper(country_clean)) in ('US', 'USA')          then 'US'
            when country_clean is null                                 then 'Unknown'
            else 'Other'
        end                                                            as market,

        -- Region grouping: broader than country for
        -- high-level geographic rollups
        case
            when upper(country_clean) in ('Kenya', 'Uganda', 'Tanzania',
                'Rwanda', 'Ethiopia', 'Nigeria', 'Ghana', 'South Africa',
                'Egypt', 'Morocco')                         then 'Africa'
            when upper(country_clean) in ('United Kingdom', 'UK', 'GB',
                'Germany', 'France', 'Netherlands', 'Sweden',
                'Spain', 'Italy', 'Poland', 'Ireland',
                'Denmark', 'Switzerland', 'Belgium')        then 'Europe'
            when upper(country_clean) in ('United States', 'US', 'USA',
                'Canada')                                   then 'North America'
            when upper(country_clean) in ('India', 'Singapore',
                'Australia', 'New Zealand', 'Japan',
                'Philippines', 'Indonesia')                 then 'Asia Pacific'
            when country_clean is null                      then 'Unknown'
            else 'Other'
        end                                                 as geo_region,

        -- Is this one of your three primary benchmark markets
        case
            when upper(country_clean) in ('Kenya', 'KE',
                'United Kingdom', 'UK', 'GB',
                'United States', 'US', 'USA')               then true
            else false
        end                                                 as is_benchmark_market,

        -- City classification: is this a known tech hub
        -- in one of your benchmark markets

        case
            when lower(city_clean) in ('nairobi', 'mombasa',
                'kisumu')                                    then 'Kenya Tech Hub'
            when lower(city_clean) in ('london', 'manchester',
                'edinburgh', 'bristol', 'cambridge',
                'birmingham', 'leeds')                      then 'UK Tech Hub'
            when lower(city_clean) in ('new york', 'san francisco',
                'seattle', 'austin', 'boston', 'chicago',
                'los angeles', 'denver', 'atlanta',
                'washington')                               then 'US Tech Hub'
            else null
        end                                                 as tech_hub_label,
        -- Location completeness: mirrors location_quality
        -- from intermediate models for consistent filtering

        case
            when city_clean is not null
             and country_clean is not null                  then 'full'
            when city_clean is null
             and country_clean is not null                  then 'country-only'
            when city_clean is not null
             and country_clean is null                      then 'city-only'
            else                                                 'unknown'
        end                                                 as location_completeness

    from resolved
    where _row_num = 1

),

-- Explicit unknown member row
-- location_key = '-1' for postings with no location data
unknown_member as (

    select
        '-1'                                                as location_key,
        cast(null as string)                                as city_clean,
        cast(null as string)                                as region_clean,
        cast(null as string)                                as country_clean,
        false                                               as in_job_postings,
        false                                               as in_salary_estimates,
        'Unknown'                                           as market,
        'Unknown'                                           as geo_region,
        false                                               as is_benchmark_market,
        cast(null as string)                                as tech_hub_label,
        'unknown'                                           as location_completeness

)

select
    location_key,
    city_clean,
    region_clean,
    country_clean,
    market,
    geo_region,
    is_benchmark_market,
    tech_hub_label,
    location_completeness,
    in_job_postings,
    in_salary_estimates
from enriched

union all

select
    location_key,
    city_clean,
    region_clean,
    country_clean,
    market,
    geo_region,
    is_benchmark_market,
    tech_hub_label,
    location_completeness,
    in_job_postings,
    in_salary_estimates
from unknown_member
order by market, country_clean, city_clean