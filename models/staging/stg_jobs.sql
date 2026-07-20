{{
    config(
        materialized = 'incremental',
        unique_key   = ['job_id', 'ingestion_date'],
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
        cast(job_id as string)                                          as job_id,
        job_title,
        job_description,
        employer_name,
        employer_website,
        employer_logo,
        job_publisher,
        job_employment_type,
        job_employment_types,
        job_is_remote,
        to_timestamp(job_posted_at_datetime_utc)                        as posted_timestamp,
        job_city,
        job_state,
        job_country,
        job_latitude,
        job_longitude,
        job_benefits,
        job_benefits_strings,
        trim(upper(cast(job_salary_period as string)))          as salary_period,
        cast(job_min_salary as double)                                  as salary_min,
        cast(job_max_salary as double)                                  as salary_max,
        cast(regexp_extract(_file_path, 'ingestion_date=([^/]+)', 1) as date) as ingestion_date,
        _file_modified_at                                               as _ingested_at
    from source
    where job_id is not null
),

deduped as (
    select *
    from parsed
    qualify row_number() over (
        partition by job_id, ingestion_date order by _ingested_at desc
    ) = 1
)

select
    job_id,
    ingestion_date,
    lower(trim(job_title))                                              as title,
    job_description                                                     as description,
    lower(trim(employer_name))                                          as company_name,
    employer_website                                                    as company_website,
    employer_logo                                                       as company_logo,
    job_city                                                            as city,
    job_state                                                           as state,
    job_country                                                         as country,
    job_latitude                                                        as latitude,
    job_longitude                                                       as longitude,
    job_benefits,
    job_benefits_strings,
    lower(trim(
        coalesce(nullif(job_city, ''), job_state, job_country)
    ))                                                                  as location_name,
    lower(coalesce(job_employment_type, 'unknown'))                     as contract_type,
    job_employment_types                                                as contract_types,
    job_is_remote,
    salary_min,
    salary_max,
    salary_period,
    case
        when salary_min is not null and salary_max is not null
        then (salary_min + salary_max) / 2.0
    end                                                                 as salary_midpoint,
    cast(posted_timestamp as date)                                      as posted_date,
    posted_timestamp,
    _ingested_at
from deduped
