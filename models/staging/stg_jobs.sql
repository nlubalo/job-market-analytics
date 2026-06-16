with source as (
    select * from {{source('raw', 'jobs')}}
),

deduped as (
    -- keep the most recently ingested copy of each job posting
    select *
    from source
    qualify row_number() over (
        partition by job_id order by _ingested_at desc
        ) =1
)
select
    job_id,
    lower(trim(title)) as title,
    description,
    lower(trim(company_display_name)) as company_name,
    lower(trim(location_display_name)) as location_name,
    location_area,
    lower(trim(category_label)) as category_label,
    lower(trim(category_tag)) as catgory_tag,
    
    lower(coalesce(contract_type, 'unknown')) as contract_type,
    lower(coalesce(contract_time, 'unknown')) as contract_time,
    salary_min,
    salary_max,
    case 
        when salary_min is not null and salary_max is not null
        then (salary_min + salary_max) / 2.0
        end as salary_midpoint,
    salary_is_predicted,
    cast(created_at as date) as posted_date,
    created_at as posted_timestamp,
    _ingested_at,
    _country as country_code
from deduped