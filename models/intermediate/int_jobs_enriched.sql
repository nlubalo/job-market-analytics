{{
    config(
        materialized = 'incremental',
        unique_key   = 'job_id',
        incremental_strategy = 'merge',
    )
}}

-- Adds skill flags, salary bucket, seniority, and remote detection to staging jobs.
-- This is where domain logic lives before the star schema.

with jobs as (
    select * from {{ ref('stg_jobs') }}
    {% if is_incremental() %}
    where _ingested_at > (select max(_ingested_at) from {{ this }})
    {% endif %}
)

select
    job_id,
    title,
    company_name,
    location_name,
    country_code,
    contract_type,
    job_is_remote,

    -- seniority derived from title keywords
    case
        when lower(title) like any ('%senior%', '%sr.%', '%lead%', '%principal%', '%staff%') then 'senior'
        when lower(title) like any ('%junior%', '%jr.%', '%graduate%', '%entry%')           then 'junior'
        when lower(title) like '%intern%'                                                    then 'intern'
        when lower(title) like any ('%head of%', '%director%', '%vp %', '%chief%')          then 'leadership'
        else 'mid'
    end as seniority_band,

    -- remote detection: use JSearch boolean first, fall back to text scan
    case
        when job_is_remote = true then 'remote'
        when lower(title || ' ' || coalesce(description, '')) like any (
            '%hybrid%', '%flexible working%'
        ) then 'hybrid'
        else 'onsite'
    end as work_arrangement,

    -- languages & frameworks
    {{ skill_flag('description', 'python') }}               as skill_python,
    {{ skill_flag_bounded('description', 'sql') }}          as skill_sql,
    {{ skill_flag('description', 'java') }}                 as skill_java,
    {{ skill_flag('description', 'scala') }}                as skill_scala,
    {{ skill_flag_bounded('description', 'go') }}           as skill_go,
    {{ skill_flag('description', 'rust') }}                 as skill_rust,
    {{ skill_flag('description', 'javascript') }}           as skill_javascript,
    {{ skill_flag('description', 'typescript') }}           as skill_typescript,
    {{ skill_flag_bounded('description', 'c\\+\\+') }}     as skill_cpp,
    {{ skill_flag_bounded('description', 'r') }}            as skill_r,

    -- data processing & storage
    {{ skill_flag('description', 'spark') }}                as skill_spark,
    {{ skill_flag('description', 'databricks') }}           as skill_databricks,
    {{ skill_flag('description', 'snowflake') }}            as skill_snowflake,
    {{ skill_flag('description', 'bigquery') }}             as skill_bigquery,
    {{ skill_flag('description', 'redshift') }}             as skill_redshift,
    {{ skill_flag('description', 'postgresql') }}           as skill_postgresql,
    {{ skill_flag('description', 'mongodb') }}              as skill_mongodb,
    {{ skill_flag('description', 'redis') }}                as skill_redis,
    {{ skill_flag('description', 'elasticsearch') }}        as skill_elasticsearch,
    {{ skill_flag('description', 'delta lake') }}           as skill_delta_lake,
    {{ skill_flag('description', 'iceberg') }}              as skill_iceberg,

    -- orchestration & streaming
    {{ skill_flag('description', 'kafka') }}                as skill_kafka,
    {{ skill_flag('description', 'airflow') }}              as skill_airflow,
    {{ skill_flag('description', 'dagster') }}              as skill_dagster,
    {{ skill_flag('description', 'prefect') }}              as skill_prefect,
    {{ skill_flag('description', 'flink') }}                as skill_flink,
    {{ skill_flag('description', 'nifi') }}                 as skill_nifi,
    {{ skill_flag('description', 'dbt') }}                  as skill_dbt,

    -- cloud & infrastructure
    {{ skill_flag('description', 'aws') }}                  as skill_aws,
    {{ skill_flag('description', 'azure') }}                as skill_azure,
    {{ skill_flag('description', 'gcp') }}                  as skill_gcp,
    {{ skill_flag('description', 'docker') }}               as skill_docker,
    {{ skill_flag('description', 'kubernetes') }}           as skill_kubernetes,
    {{ skill_flag('description', 'terraform') }}            as skill_terraform,

    -- ml & ai
    {{ skill_flag('description', 'machine learning') }}     as skill_machine_learning,
    {{ skill_flag('description', 'deep learning') }}        as skill_deep_learning,
    {{ skill_flag('description', 'mlflow') }}               as skill_mlflow,
    {{ skill_flag('description', 'pytorch') }}              as skill_pytorch,
    {{ skill_flag('description', 'tensorflow') }}           as skill_tensorflow,
    {{ skill_flag('description', 'scikit') }}               as skill_scikit_learn,
    {{ skill_flag('description', 'langchain') }}            as skill_langchain,
    {{ skill_flag_bounded('description', 'llm') }}          as skill_llm,
    {{ skill_flag('description', 'generative ai') }}        as skill_generative_ai,

    -- data quality & governance
    {{ skill_flag('description', 'great expectations') }}   as skill_great_expectations,
    {{ skill_flag('description', 'soda') }}                 as skill_soda,
    {{ skill_flag('description', 'monte carlo') }}          as skill_monte_carlo,
    {{ skill_flag('description', 'data lineage') }}         as skill_data_lineage,
    {{ skill_flag('description', 'data governance') }}      as skill_data_governance,

    -- bi & analytics
    {{ skill_flag('description', 'power bi') }}             as skill_powerbi,
    {{ skill_flag('description', 'tableau') }}              as skill_tableau,
    {{ skill_flag('description', 'looker') }}               as skill_looker,
    {{ skill_flag('description', 'metabase') }}             as skill_metabase,
    {{ skill_flag('description', 'superset') }}             as skill_superset,
    {{ skill_flag('description', 'excel') }}                as skill_excel,

    -- devops & methodology
    {{ skill_flag_bounded('description', 'git') }}          as skill_git,
    {{ skill_flag('description', 'ci/cd') }}                as skill_cicd,
    {{ skill_flag('description', 'jenkins') }}              as skill_jenkins,
    {{ skill_flag('description', 'github actions') }}       as skill_github_actions,
    {{ skill_flag('description', 'agile') }}                as skill_agile,

    salary_min,
    salary_max,
    salary_midpoint,
    {{ salary_bucket('salary_midpoint') }}          as salary_bucket,

    posted_date,
    posted_timestamp,
    _ingested_at

from jobs
