with geodata as (
    select
        lower(trim(location_name))  as location_name,
        location_area,
        job_count                   as adzuna_job_count,
        country_code,
        _ingested_at
    from {{ ref('stg_geodata') }}
),

job_locations as (
    select
        location_name,
        location_area,
        country_code,
        count(*)                                                as job_posting_count,
        avg(salary_midpoint)                                    as avg_salary_midpoint,
        min(salary_min)                                         as min_salary,
        max(salary_max)                                         as max_salary,
        sum(case when not salary_is_predicted then 1 else 0 end) as actual_salary_count,
        min(posted_date)                                        as earliest_posting,
        max(posted_date)                                        as latest_posting
    from {{ ref('int_jobs_enriched') }}
    group by location_name, location_area, country_code
),

fuzzy_matched as (
    select
        j.location_name,
        j.location_area,
        g.location_name             as geo_location_name,
        g.adzuna_job_count,
        j.job_posting_count,
        j.avg_salary_midpoint,
        j.min_salary,
        j.max_salary,
        j.actual_salary_count,
        j.earliest_posting,
        j.latest_posting,
        j.country_code,
        g._ingested_at              as geo_ingested_at,
        row_number() over (
            partition by j.location_name, j.country_code
            order by
                case
                    when lower(g.location_name) = j.location_name then 1
                    when j.location_name like '%' || trim(split(g.location_name, ',')[0]) || '%' then 2
                    when j.location_area like '%' || trim(split(g.location_area, ',')[1]) || '%' then 3
                    else 4
                end
        ) as match_rank
    from job_locations j
    left join geodata g
        on j.country_code = g.country_code
        and (
            lower(g.location_name) = j.location_name
            or j.location_name like '%' || lower(trim(split(g.location_name, ',')[0])) || '%'
            or j.location_area like '%' || trim(split(g.location_area, ',')[1]) || '%'
        )
)

select
    location_name,
    location_area,
    geo_location_name,
    adzuna_job_count,
    job_posting_count,
    avg_salary_midpoint,
    min_salary,
    max_salary,
    actual_salary_count,
    earliest_posting,
    latest_posting,
    country_code,
    geo_ingested_at
from fuzzy_matched
where match_rank = 1
