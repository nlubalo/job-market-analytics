-- models/facts/fct_job_posting_skills.sql
-- =============================================================
-- Fact: job posting skills (bridge table)
-- Grain: one row per (job_id_source, skill_name)
--
-- Unpivots the flat boolean skill flags from int_jobs_enriched
-- into a long format — one row per skill per posting.
--
-- This model enables clean skill-level aggregations:
--   count(distinct job_id_source) where skill_name = 'dbt'
-- without SUM(CASE WHEN) gymnastics on a wide fact table.
--
-- Materialized as table — relatively small, fast to rebuild,
-- and downstream mart queries benefit from the pre-unpivoted
-- format more than they benefit from incremental savings.
-- =============================================================

{{
    config(
        materialized='table',
        tags=['facts']
    )
}}

with source as (
    select
        job_id,
        job_title_key,
        location_key,
        date_first_seen_key,
        seniority_level,
        work_arrangement,
        record_quality,

        -- Skill flags
        skill_python,
        skill_sql,
        skill_spark,
        skill_dbt,
        skill_airflow,
        skill_kafka,
        skill_databricks,
        skill_bigquery,
        skill_aws,
        skill_azure,
        skill_gcp,
        skill_snowflake,
        --skill_ml,
        skill_deep_learning,
        skill_llm,
        skill_count
    from {{ ref('int_jobs_enriched') }}
),
-- =====================
-- unpivot: one row per (job, skill) pair
-- Only emit rows where the skill flag is true
-- absence of a skill is not a fact worth storing

unpivoted as (
    select job_id,
            job_title_key,
            location_key,
            date_first_seen_key,
            seniority_level,
            work_arrangement,
            record_quality,
            skill_count,
            'Python' as skill_name,
            'Data Stack' as skill_category
    from source where skill_python = 1

    union all

    select  job_id,
            job_title_key,
            location_key,
            date_first_seen_key,
            seniority_level,
            work_arrangement,
            record_quality,
            skill_count,
            'SQL' as skill_name,
            'Data Stack' as skill_category
    from source where skill_sql = 1

    union all

    select job_id,
            job_title_key,
            location_key,
            date_first_seen_key,
            seniority_level,
            work_arrangement,
            record_quality,
            skill_count,
            'Spark' as skill_name,
            'Data Stack' as skill_category
    from source where skill_spark = 1

    union all

    select job_id,
            job_title_key,
            location_key,
            date_first_seen_key,
            seniority_level,
            work_arrangement,
            record_quality,
            skill_count,
            'dbt' as skill_name,
            'Data Stack' as skill_category
    from source where skill_dbt = 1

    union all 

    select job_id,
            job_title_key,
            location_key,
            date_first_seen_key,
            seniority_level,
            work_arrangement,
            record_quality,
            skill_count,
            'Airflow' as skill_name,
            'Data Stack' as skill_category
    from source where skill_airflow = 1

    union all

    select job_id,
            job_title_key,
            location_key,
            date_first_seen_key,
            seniority_level,
            work_arrangement,
            record_quality,
            skill_count,
            'Kafka' as skill_name,
            'Data Stack' as skill_category
    from source where skill_kafka = 1

    union all

    select job_id,
            job_title_key,
            location_key,
            date_first_seen_key,
            seniority_level,
            work_arrangement,
            record_quality,
            skill_count,
            'Databricks' as skill_name,
            'Data Stack' as skill_category
    from source where skill_databricks = 1

    union all

    select job_id,
            job_title_key,
            location_key,
            date_first_seen_key,
            seniority_level,
            work_arrangement,
            record_quality,
            skill_count,
            'BigQuery' as skill_name,
            'Cloud' as skill_category
    from source where skill_databricks = 1

    union all

    select job_id,
            job_title_key,
            location_key,
            date_first_seen_key,
            seniority_level,
            work_arrangement,
            record_quality,
            skill_count,
            'Azure' as skill_name,
            'Cloud' as skill_category
    from source where skill_azure = 1

    union all

     select job_id,
            job_title_key,
            location_key,
            date_first_seen_key,
            seniority_level,
            work_arrangement,
            record_quality,
            skill_count,
            'GCP' as skill_name,
            'Cloud' as skill_category
    from source where skill_gcp = 1

    union all


    select job_id,
            job_title_key,
            location_key,
            date_first_seen_key,
            seniority_level,
            work_arrangement,
            record_quality,
            skill_count,
            'Snowflake' as skill_name,
            'Cloud' as skill_category
    from source where skill_snowflake = 1

    union all

    select job_id,
            job_title_key,
            location_key,
            date_first_seen_key,
            seniority_level,
            work_arrangement,
            record_quality,
            skill_count,
            'Deep Learning' as skill_name,
            'AI/ML' as skill_category
    from source where skill_deep_learning = 1

    union all

    select job_id,
            job_title_key,
            location_key,
            date_first_seen_key,
            seniority_level,
            work_arrangement,
            record_quality,
            skill_count,
            'LLM' as skill_name,
            'AI/ML' as skill_category
    from source where skill_llm = 1
)

select
    md5(concat_ws('||', job_id, skill_name))         as job_skill_key,
    job_id,
    job_title_key,
    location_key,
    date_first_seen_key                                     as date_key,

    skill_name,
    skill_category,

    seniority_level,
    work_arrangement,
    record_quality,

    skill_count
from unpivoted