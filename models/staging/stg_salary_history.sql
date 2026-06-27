{% set raw_path = env_var('JSEARCH_RAW_ZONE_ROOT', '/Volumes/job_market/raw/jsearch') ~ '/endpoint=salary_estimate/' %}

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
        lower(trim(location))                           as location,
        lower(trim(job_title))                          as job_title,
        cast(min_salary as double)                      as min_salary,
        cast(max_salary as double)                      as max_salary,
        cast(median_salary as double)                   as median_salary,
        cast(min_base_salary as double)                 as min_base_salary,
        cast(max_base_salary as double)                 as max_base_salary,
        cast(median_base_salary as double)              as median_base_salary,
        cast(min_additional_pay as double)              as min_additional_pay,
        cast(max_additional_pay as double)              as max_additional_pay,
        cast(median_additional_pay as double)           as median_additional_pay,
        salary_period,
        salary_currency,
        salary_count,
        salaries_updated_at,
        publisher_name,
        confidence,
        cast(regexp_extract(_file_path, 'ingestion_date=([^/]+)', 1) as date) as ingestion_date,
        _file_modified_at                               as _ingested_at
    from source
),

deduped as (
    select *
    from parsed
    qualify row_number() over (
        partition by location, job_title, salaries_updated_at, publisher_name
        order by _ingested_at desc
    ) = 1
)

select
    job_title,
    location,
    min_salary,
    max_salary,
    median_salary,
    min_base_salary,
    max_base_salary,
    median_base_salary,
    min_additional_pay,
    max_additional_pay,
    median_additional_pay,
    salary_period,
    salary_currency,
    salary_count,
    salaries_updated_at,
    publisher_name,
    confidence,
    ingestion_date,
    _ingested_at
from deduped
where median_salary is not null
