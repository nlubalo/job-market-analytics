with job_stats as (
    select
        location_name,
        location_area,
        country_code,
        count(*)                                                as total_jobs,
        count(distinct category_tag)                            as category_count,
        avg(salary_midpoint)                                    as avg_salary_midpoint,
        min(salary_min)                                         as min_salary,
        max(salary_max)                                         as max_salary,
        sum(case when not salary_is_predicted then 1 else 0 end) as actual_salary_count,

        mode(category_tag)                                      as primary_category,
        mode(seniority_band)                                    as primary_seniority,
        mode(work_arrangement)                                  as primary_work_arrangement,
        mode(contract_type)                                     as primary_contract_type,
        mode(company_name)                                      as primary_company,

        min(posted_date)                                        as first_seen,
        max(posted_date)                                        as last_seen,
        max(_ingested_at)                                       as _ingested_at
    from {{ ref('int_jobs_enriched') }}
    where location_name is not null
    group by location_name, location_area, country_code
),

geo as (
    select
        location_name,
        geo_location_name,
        adzuna_job_count
    from {{ ref('int_location_enriched') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['j.location_name', 'j.country_code']) }} as location_key,
    j.location_name,
    j.location_area,
    j.country_code,
    g.geo_location_name,
    g.adzuna_job_count,
    j.total_jobs,
    j.category_count,
    j.avg_salary_midpoint,
    j.min_salary,
    j.max_salary,
    j.actual_salary_count,
    j.primary_category,
    j.primary_seniority,
    j.primary_work_arrangement,
    j.primary_contract_type,
    j.primary_company,
    j.first_seen,
    j.last_seen,
    j._ingested_at
from job_stats j
left join geo g
    on j.location_name = g.location_name
