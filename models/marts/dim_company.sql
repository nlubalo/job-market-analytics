
with job_stats as (
    select
        company_name,
        country_code,
        count(*) as total_jobs,
        avg(salary_midpoint) as avg_salary_midpoint,
        min(salary_min) as min_salary,
        max(salary_max) as max_salary,
        sum(case when salary_midpoint is not null then 1 else 0 end) as actual_salary_count,
        mode(seniority_band)                                    as primary_seniority,
        mode(work_arrangement)                                  as primary_work_arrangement,
        mode(contract_type)                                     as primary_contract_type,
        mode(location_name)                                     as primary_location,

        round(avg(skill_python) * 100, 1)                       as pct_python,
        round(avg(skill_sql) * 100, 1)                          as pct_sql,
        round(avg(skill_aws) * 100, 1)                          as pct_aws,
        round(avg(skill_azure) * 100, 1)                        as pct_azure,
        round(avg(skill_gcp) * 100, 1)                          as pct_gcp,
        round(avg(skill_spark) * 100, 1)                        as pct_spark,
        round(avg(skill_databricks) * 100, 1)                   as pct_databricks,
        round(avg(skill_dbt) * 100, 1)                          as pct_dbt,
        round(avg(skill_kubernetes) * 100, 1)                   as pct_kubernetes,
        round(avg(skill_machine_learning) * 100, 1)             as pct_machine_learning,

        min(posted_date)                                        as first_seen,
        max(posted_date)                                        as last_seen,
        max(_ingested_at)                                       as _ingested_at
    from {{ ref('int_jobs_enriched') }}
    where company_name is not null
    group by company_name, country_code

)
select
    {{ dbt_utils.generate_surrogate_key(['company_name', 'country_code']) }} as company_key,
    company_name,
    country_code,
    total_jobs,
    avg_salary_midpoint,
    min_salary,
    max_salary,
    actual_salary_count,
    primary_seniority,
    primary_work_arrangement,
    primary_contract_type,
    primary_location,
    pct_python,
    pct_sql,
    pct_aws,
    pct_azure,
    pct_gcp,
    pct_spark,
    pct_databricks,
    pct_dbt,
    pct_kubernetes,
    pct_machine_learning,
    first_seen,
    last_seen,
    _ingested_at
from job_stats