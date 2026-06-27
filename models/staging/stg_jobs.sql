{{
    config(
        materialized = 'incremental',
        unique_key   = 'job_id',
        incremental_strategy = 'merge',
    )
}}

{% set raw_path = env_var('JSEARCH_RAW_ZONE_ROOT', '/Volumes/job_market/raw/jsearch') ~ '/endpoint=job_search/' %}

with source as (
    select
        *,
        _metadata.file_path                as _file_path,
        _metadata.file_modification_time   as _file_modified_at
    from read_files(
        '{{ raw_path }}',
        format => 'json',
        recursiveFileLookup => true,
        pathGlobFilter => 'data.json'
    )
    {% if is_incremental() %}
    where _metadata.file_modification_time > (select max(_ingested_at) from {{ this }})
    {% endif %}
),

parsed as (
    select
        cast(job_id as string)                              as job_id,
        job_title,
        job_description,
        employer_name,
        employer_website,
        job_employment_type,
        job_is_remote,
        to_timestamp(job_posted_at_datetime_utc)            as posted_timestamp,
        job_city,
        job_state,
        job_country,
        cast(job_min_salary as double)                      as salary_min,
        cast(job_max_salary as double)                      as salary_max,
        _file_modified_at                                   as _ingested_at
    from source
    where job_id is not null
),

deduped as (
    select *
    from parsed
    qualify row_number() over (
        partition by job_id order by _ingested_at desc
    ) = 1
)

select
    job_id,
    lower(trim(job_title))                                  as title,
    job_description                                         as description,
    lower(trim(employer_name))                              as company_name,
    employer_website                                        as company_website,
    lower(trim(
        coalesce(nullif(job_city, ''), job_state, job_country)
    ))                                                      as location_name,
    lower(trim(job_country))                                as country_code,
    lower(coalesce(job_employment_type, 'unknown'))         as contract_type,
    job_is_remote,
    salary_min,
    salary_max,
    case
        when salary_min is not null and salary_max is not null
        then (salary_min + salary_max) / 2.0
    end                                                     as salary_midpoint,
    cast(posted_timestamp as date)                          as posted_date,
    posted_timestamp,
    _ingested_at
from deduped
