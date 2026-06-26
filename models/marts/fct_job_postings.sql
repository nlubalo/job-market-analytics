{{
    config(
        materialized = 'incremental',
        unique_key   = 'job_id',
        incremental_strategy = 'merge',
    )
}}

with jobs as (
    select * from {{ ref('int_jobs_enriched') }}
    {% if is_incremental() %}
    where _ingested_at > (select max(_ingested_at) from {{ this }})
    {% endif %}
)

select
    job_id,

    -- foreign keys (match surrogate key definitions in the dim tables)
    {{ dbt_utils.generate_surrogate_key(['company_name', 'country_code']) }}  as company_key,
    {{ dbt_utils.generate_surrogate_key(['location_name', 'country_code']) }} as location_key,
    {{ dbt_utils.generate_surrogate_key(['category_tag', 'country_code']) }}  as category_key,

    -- degenerate dimensions
    seniority_band,
    work_arrangement,
    contract_type,
    contract_time,
    salary_bucket,
    country_code,

    -- salary measures
    salary_min,
    salary_max,
    salary_midpoint,
    salary_is_predicted,

    -- languages & frameworks
    skill_python,
    skill_sql,
    skill_java,
    skill_scala,
    skill_go,
    skill_rust,
    skill_javascript,
    skill_typescript,
    skill_cpp,
    skill_r,

    -- data processing & storage
    skill_spark,
    skill_databricks,
    skill_snowflake,
    skill_bigquery,
    skill_redshift,
    skill_postgresql,
    skill_mongodb,
    skill_redis,
    skill_elasticsearch,
    skill_delta_lake,
    skill_iceberg,

    -- orchestration & streaming
    skill_kafka,
    skill_airflow,
    skill_dagster,
    skill_prefect,
    skill_flink,
    skill_nifi,
    skill_dbt,

    -- cloud & infrastructure
    skill_aws,
    skill_azure,
    skill_gcp,
    skill_docker,
    skill_kubernetes,
    skill_terraform,

    -- ml & ai
    skill_machine_learning,
    skill_deep_learning,
    skill_mlflow,
    skill_pytorch,
    skill_tensorflow,
    skill_scikit_learn,
    skill_langchain,
    skill_llm,
    skill_generative_ai,

    -- data quality & governance
    skill_great_expectations,
    skill_soda,
    skill_monte_carlo,
    skill_data_lineage,
    skill_data_governance,

    -- bi & analytics
    skill_powerbi,
    skill_tableau,
    skill_looker,
    skill_metabase,
    skill_superset,
    skill_excel,

    -- devops & methodology
    skill_git,
    skill_cicd,
    skill_jenkins,
    skill_github_actions,
    skill_agile,

    -- dates
    posted_date,
    posted_timestamp,
    _ingested_at

from jobs
