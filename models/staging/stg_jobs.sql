{{
    config(
        materialized = 'incremental',
        unique_key   = 'job_id',
        incremental_strategy = 'merge',
    )
}}

{% set raw_path = env_var('ADZUNA_RAW_ZONE_ROOT', '/Volumes/job_market/raw/adzuna') ~ '/endpoint=job_search/' %}

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
        cast(id as string)                                  as job_id,
        title,
        description,
        company.display_name                                as company_display_name,
        location.display_name                               as location_display_name,
        array_join(location.area, ',')                      as location_area,
        category.label                                      as category_label,
        category.tag                                        as category_tag,
        contract_type,
        contract_time,
        cast(salary_min as double)                          as salary_min,
        cast(salary_max as double)                          as salary_max,
        cast(salary_is_predicted as int) = 1                as salary_is_predicted,
        redirect_url,
        to_timestamp(replace(created, 'Z', '+00:00'))       as created_at,
        regexp_extract(_file_path, 'country=([^/]+)', 1)    as _country,
        regexp_extract(_file_path, 'ingestion_date=([^/]+)', 1) as _ingestion_date,
        _file_modified_at                                   as _ingested_at
    from source
    where id is not null
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
    lower(trim(title))                          as title,
    description,
    lower(trim(company_display_name))           as company_name,
    lower(trim(location_display_name))          as location_name,
    location_area,
    lower(trim(category_label))                 as category_label,
    lower(trim(category_tag))                   as category_tag,

    lower(coalesce(contract_type, 'unknown'))    as contract_type,
    lower(coalesce(contract_time, 'unknown'))    as contract_time,
    salary_min,
    salary_max,
    case
        when salary_min is not null and salary_max is not null
        then (salary_min + salary_max) / 2.0
    end                                          as salary_midpoint,
    salary_is_predicted,
    cast(created_at as date)                     as posted_date,
    created_at                                   as posted_timestamp,
    _ingested_at,
    _country                                     as country_code
from deduped
