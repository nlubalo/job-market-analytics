{% set raw_path = env_var('ADZUNA_RAW_ZONE_ROOT', '/Volumes/job_market/raw/adzuna') ~ '/endpoint=geodata/' %}

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
),

parsed as (
    select
        location.display_name                               as location_name,
        location.area                                       as location_area,
        cast(count as bigint)                                as job_count,
        regexp_extract(_file_path, 'country=([^/]+)', 1)    as _country,
        regexp_extract(_file_path, 'ingestion_date=([^/]+)', 1) as _ingestion_date,
        _file_modified_at                                   as _ingested_at
    from source
    where location.display_name is not null
),

deduped as (
    select *
    from parsed
    qualify row_number() over (
        partition by location_name, _country
        order by _ingested_at desc
    ) = 1
)

select
    trim(location_name)                 as location_name,
    array_join(location_area, ',')      as location_area,
    job_count,
    _country                            as country_code,
    _ingested_at
from deduped
where job_count > 0
