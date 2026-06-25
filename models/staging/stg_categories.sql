{% set raw_path = env_var('ADZUNA_RAW_ZONE_ROOT', '/Volumes/job_market/raw/adzuna') ~ '/endpoint=categories/' %}

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
        tag,
        label,
        regexp_extract(_file_path, 'country=([^/]+)', 1)    as _country,
        _file_modified_at                                    as _ingested_at
    from source
    where tag is not null
),

deduped as (
    select *
    from parsed
    qualify row_number() over (
        partition by tag, _country
        order by _ingested_at desc
    ) = 1
)

select
    lower(trim(tag))    as category_tag,
    trim(label)         as category_label,
    _country            as country_code,
    _ingested_at
from deduped
