{{
    config(
        materialized = 'incremental',
        unique = 'trend_key',
        incremental_strategy = 'merge'
    )
}}

with jobs as (
    select
        date_trunc('month', posted_at) as salary_month,
        category_tag,
        seniority_band,
        country_code,
        salary_min,
        salary_max,
        salary_midpoint,
        salary_is_predicted
    from {{ ref('int_jobs_enriched')}}
    where salary_midpoint is not null
    {% if is_incremental()%}
    and posted_date <= (select max(salary_month) from {{ this }})
    {% endif %}

),
aggregated as (
    select
        salary_month,
        category_tag,
        seniority_band,
        country_code,
        count(*) as job_count,
        count(*) filter (where not salary_is_predicted) as jobs_with_actual_salary,
        round(avg(salary_midpoint) filter (where not salary_is_predicted), 0) as avg_salary
        round(percentile_approx(salary_midpoint, 0.5) filter (where not salary_is_predicted), 0) as median_salary,
        round(percentile_approx(salary_midpoint, 0.25) filter (where not salary_is_predicted), 0) as p25_salary,
        round(percentile_approx(salary_midpoint, 0.75) filter (where not salary_is_predicted), 0) as p75_salary,
        round(min(salary_min) filter (where not salary_is_predicted), 0) as min_salary
        round(max(salary_max) filter (where not salary_is_predicted), 0) as max_salary
    from job
    group by salary_month, category_tag, seniority_band, country_code

)
select
    {{ dbt_utils.generate_surrogate_key(['salary_month', 'category_tag', 'seniority_band', 'country_code']) }} as trend_key,
    salary_month,
    category_tag,
    seniority_band,
    country_code,
    job_count,
    jobs_with_actual_salary,
    avg_salary,
    median_salary,
    p25_salary,
    p75_salary,
    min_salary,
    max_salary
from aggregated
where jobs_with_actual_salary > 0