-- models/intermediate/int_jobs_enriched.sql
-- =============================================================
-- Intermediate: enriched and deduplicated job postings
-- Grain: one row per unique job posting (by job_id_source)
--
-- Responsibilities:
--   1. Normalize company name
--   2. Normalize and classify location
--   3. Normalize and classify job title / seniority
--   4. Derive work arrangement (remote / hybrid / onsite)
--   5. Flag skills from description text
--   6. Normalize and annualize salary where disclosed
--   7. Assign salary bucket
--   8. Deduplicate across scrape runs (keep latest)
--   9. Assign unknown member keys for nulls
--  10. Set data quality flags
-- =============================================================

{{
    config(
        materialized = 'incremental',
        unique_key   = ['job_id'],
        incremental_strategy = 'merge',
    )
}}


{% set skills = {
    "python": ["python"],
    "r": ["r", "r language"],
    "sql": ["sql"],
    "bigquery": ["bigquery", "big query"],
    "javascript": ["javascript", "java script"],
    "postgres": ["postgres", "postgresql"],
    "csharp": ["c#", "c sharp"],
    "machine_learning": ["machine learning", "ml"],
    "artificial_intelligence": ["artificial intelligence", "ai"],
    "spark": ["spark", "apache spark", "pyspark", "spark sql"],
    "databricks": ["databricks"],
    "snowflake": ["snowflake"],
    "redshift": ["redshift"],
    "mongodb": ["mongodb", "mongo db"],
    "redis": ["redis"],
    "elasticsearch": ["elasticsearch", "elastic search"],
    "delta_lake": ["delta lake"],
    "iceberg": ["iceberg"],
    "kafka": ["kafka"],
    "airflow": ["airflow"],
    "dagster": ["dagster"],
    "prefect": ["prefect"],
    "flink": ["flink"],
    "nifi": ["nifi"],
    "dbt": ["dbt"],
    "aws": ["aws", "amazon web services"],
    "azure": ["azure", "microsoft azure"],
    "gcp": ["gcp", "google cloud platform"],
    "docker": ["docker"],
    "kubernetes": ["kubernetes", "k8s"],
    "terraform": ["terraform"],
    "deep_learning": ["deep learning", "dl"],
    "mlflow": ["mlflow"],
    "pytorch": ["pytorch"],
    "tensorflow": ["tensorflow"],
    "scikit_learn": ["scikit learn", "sklearn"],
    "langchain": ["langchain"],
    "llm": ["llm", "large language model"],
    "generative_ai": ["generative ai", "generative artificial intelligence"],
    "great_expectations": ["great expectations"],
    "soda": ["soda"],
    "monte_carlo": ["monte carlo"],
    "data_lineage": ["data lineage"],
    "data_governance": ["data governance"],
    "powerbi": ["power bi", "powerbi"],
    "tableau": ["tableau"],
    "looker": ["looker"],
    "metabase": ["metabase"],
    "superset": ["superset"],
    "excel": ["excel"],
    "git": ["git"],
    "cicd": ["ci/cd", "continuous integration", "continuous delivery"],
    "jenkins": ["jenkins"],
    "github_actions": ["github actions"],
    "agile": ["agile"]
} %}

-- Adds skill flags, salary bucket, seniority, and remote detection to staging jobs.
-- This is where domain logic lives before the star schema.

with jobs as (
    select * from {{ ref('stg_jobs') }}
    {% if is_incremental() %}
    where _ingested_at > (select max(_ingested_at) from {{ this }})
    {% endif %}
),

company_normalized as (
    select
        *,
        {{ employer_name_normalization('company_name') }} as company_name_clean,
        case
            when
                company_name is null then '-1'
                else md5({{ employer_name_normalization('company_name') }})
        end as company_key
    from jobs
),

-- =============================================================
-- STEP 2: Location normalization
-- =============================================================
location_nomalized as (
    select
        *,
        case
            when job_is_remote = true then 'remote'
            when lower(description) like any ('%hybrid%', '%flexible working%') then 'hybrid'
            when lower(description) like any ('%onsite%', '%on-site%', '%in person%', '%in-office%') then 'onsite'
            else 'onsite'
        end as work_arrangement,
        initcap(trim(city))                                 as city_clean,
        initcap(trim(state))                               as state_clean,
        initcap(trim(country))                              as country_clean,

        case
            when city is null and country is null       then '-1'
            else md5(
                concat(
                    coalesce(lower(trim(city)), ''),
                    '||',
                    coalesce(lower(trim(country)), '')
                )
            )
        end                                                     as location_key,

        case
            when job_is_remote = true then 'remote'
            when city is not null and country is not null then 'located'
            when city is null and country is null then 'missing'
            else 'partial'
        end as location_quality
    from company_normalized

),
-- =============================================================
-- STEP 3: Title normalization and seniority derivation
-- =============================================================

title_normalized as (
    select
        *,
        {{ normalize_job_title('title') }} as title_normalized,
        {{ derive_seniority_level('title') }} as seniority_level,
        {{ derive_seniority_rank('title') }} as seniority_rank
    from location_nomalized
),
skill_flagged as (
    select
        *,
    {% for skill, terms in skills.items() %}
        {{ skill_flag('description', terms) }} as skill_{{ skill }},
    {% endfor %}

    {{ skill_count('description', skills) }} as skill_count,
    case
            when title_normalized is null
            then cast(-1 as bigint)
            else cast(
                conv(
                    substr(md5(title_normalized), 1, 8),
                    16, 10
                ) as bigint
            )
        end                                                     as job_title_key
from title_normalized

),
salary_normalized as (
    select
        *,
        case upper(trim(salary_period))
            when 'YEARLY' then  salary_min
            when 'MONTHLY' then salary_min * 12
            when 'WEEKLY' then salary_min * 52
            when 'DAILY' then salary_min * 260
            when 'HOURLY' then salary_min * 2080
            else null
        end as salary_min_annualized,
        case upper(trim(salary_period))
            when 'YEARLY' then  salary_max
            when 'MONTHLY' then salary_max * 12
            when 'WEEKLY' then salary_max * 52
            when 'DAILY' then salary_max * 260
            when 'HOURLY' then salary_max * 2080
            else null
        end as salary_max_annualized,
        salary_min is not null
            and salary_max is not null                      as has_salary_disclosed
    from skill_flagged
),

-- =============================================================
-- STEP 7: Deduplication
-- =============================================================

deduped as (
    select
        *,

        row_number() over (
            partition by job_id
            order by _ingested_at desc
        )                                                       as _row_num,

        min(_ingested_at) over (
            partition by job_id
        )                                                       as first_seen_at,

        max(_ingested_at) over (
            partition by job_id
        )                                                       as last_seen_at

    from salary_normalized
)

select

        -- ---------------------------------------------------------
        -- Identifiers and keys
        -- ---------------------------------------------------------
        job_id,
        company_key,
        location_key,
        job_title_key,
        cast(date_format(
            to_date(posted_timestamp), 'yyyyMMdd'
        ) as int)                                               as date_posted_key,

        cast(date_format(
            to_date(first_seen_at), 'yyyyMMdd'
        ) as int)                                               as date_first_seen_key,

        -- ---------------------------------------------------------
        -- Descriptive attributes
        -- ---------------------------------------------------------
        title,
        title_normalized,
        company_name_clean,
        company_website,
        company_logo,
        city_clean,
        state_clean,
        country_clean,
        work_arrangement,
        seniority_level,
        seniority_rank,

         case upper(trim(contract_type))
            when 'FULLTIME'   then 'Full-Time'
            when 'PARTTIME'   then 'Part-Time'
            when 'CONTRACTOR' then 'Contract'
            when 'INTERN'     then 'Internship'
            else coalesce(initcap(trim(contract_type)), 'Unknown')
        end                                                     as employment_type,

        -- ---------------------------------------------------------
        -- Salary measures
        -- ---------------------------------------------------------
        has_salary_disclosed,
        salary_min_annualized,
        salary_max_annualized,
        round(
            (coalesce(salary_min_annualized, 0) + coalesce(salary_max_annualized, 0)) / 2,
            0
        )                                                       as salary_midpoint_annual,

        -- ---------------------------------------------------------
        -- Skill flags
        -- ---------------------------------------------------------
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
        skill_count,

        -- ---------------------------------------------------------
        -- Measures
        -- ---------------------------------------------------------
        datediff(
            to_date(last_seen_at),
            to_date(first_seen_at)
        )                                                       as days_active,

        1                                                       as posting_count,

        -- ---------------------------------------------------------
        -- Data quality flags
        -- ---------------------------------------------------------
        location_quality,

        case
            when job_title_key = -1                             then true
            else false
        end                                                     as is_title_unknown,

        case
            when company_key = '-1'                             then true
            else false
        end                                                     as is_company_unknown,

        case
            when location_quality = 'located'
             and job_title_key != -1
             and company_key != '-1'                            then 'high'
            when location_quality in ('remote', 'partial')
             and job_title_key != -1                            then 'medium'
            else                                                     'low'
        end                                                     as record_quality,

        -- ---------------------------------------------------------
        -- Pipeline metadata
        -- ---------------------------------------------------------
        first_seen_at,
        last_seen_at,
        _ingested_at                                             as last_ingested_at

    from deduped
    where _row_num = 1





