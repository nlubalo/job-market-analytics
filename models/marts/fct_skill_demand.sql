{{
    config(
        materialized = 'incremental',
        unique_key   = 'demand_key',
        incremental_strategy = 'merge',
    )
}}

with base as (
    select
        posted_date,
        seniority_band,
        country_code,

        skill_python, skill_sql, skill_java, skill_scala, skill_go,
        skill_rust, skill_javascript, skill_typescript, skill_cpp, skill_r,

        skill_spark, skill_databricks, skill_snowflake, skill_bigquery, skill_redshift,
        skill_postgresql, skill_mongodb, skill_redis, skill_elasticsearch,
        skill_delta_lake, skill_iceberg,

        skill_kafka, skill_airflow, skill_dagster, skill_prefect,
        skill_flink, skill_nifi, skill_dbt,

        skill_aws, skill_azure, skill_gcp, skill_docker, skill_kubernetes, skill_terraform,

        skill_machine_learning, skill_deep_learning, skill_mlflow, skill_pytorch,
        skill_tensorflow, skill_scikit_learn, skill_langchain, skill_llm, skill_generative_ai,

        skill_great_expectations, skill_soda, skill_monte_carlo,
        skill_data_lineage, skill_data_governance,

        skill_powerbi, skill_tableau, skill_looker, skill_metabase, skill_superset, skill_excel,

        skill_git, skill_cicd, skill_jenkins, skill_github_actions, skill_agile

    from {{ ref('int_jobs_enriched') }}
    {% if is_incremental() %}
    where posted_date > (select max(posted_date) from {{ this }})
    {% endif %}
),

unpivoted as (
    select
        posted_date,
        seniority_band,
        country_code,
        skill_name,
        skill_present
    from base
    unpivot (skill_present for skill_name in (
        skill_python, skill_sql, skill_java, skill_scala, skill_go,
        skill_rust, skill_javascript, skill_typescript, skill_cpp, skill_r,
        skill_spark, skill_databricks, skill_snowflake, skill_bigquery, skill_redshift,
        skill_postgresql, skill_mongodb, skill_redis, skill_elasticsearch,
        skill_delta_lake, skill_iceberg,
        skill_kafka, skill_airflow, skill_dagster, skill_prefect,
        skill_flink, skill_nifi, skill_dbt,
        skill_aws, skill_azure, skill_gcp, skill_docker, skill_kubernetes, skill_terraform,
        skill_machine_learning, skill_deep_learning, skill_mlflow, skill_pytorch,
        skill_tensorflow, skill_scikit_learn, skill_langchain, skill_llm, skill_generative_ai,
        skill_great_expectations, skill_soda, skill_monte_carlo,
        skill_data_lineage, skill_data_governance,
        skill_powerbi, skill_tableau, skill_looker, skill_metabase, skill_superset, skill_excel,
        skill_git, skill_cicd, skill_jenkins, skill_github_actions, skill_agile
    ))
),

aggregated as (
    select
        posted_date,
        skill_name,
        seniority_band,
        country_code,
        sum(skill_present)  as job_count,
        count(*)            as total_jobs
    from unpivoted
    group by posted_date, skill_name, seniority_band, country_code
)

select
    {{ dbt_utils.generate_surrogate_key(['posted_date', 'skill_name', 'seniority_band', 'country_code']) }} as demand_key,
    posted_date,
    skill_name,
    seniority_band,
    country_code,
    job_count,
    total_jobs,
    round(job_count / total_jobs * 100, 2) as pct_jobs_mentioning
from aggregated
