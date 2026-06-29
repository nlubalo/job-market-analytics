# Job Market Analytics

Job market analytics pipeline focused on Kenya / East Africa with global salary benchmarks, built on JSearch API data, Databricks Delta Lake, and dbt.

## Pipeline Overview

```
JSearch API (OpenWebNinja)
    │
    ▼
Raw Zone (Unity Catalog Volumes)
  endpoint=job_search/ingestion_date=YYYY-MM-DD/query={role+location}/data.jsonl
  endpoint=salary_estimate/ingestion_date=YYYY-MM-DD/query={role_location}/data.jsonl
    │
    ▼
dbt – Staging        (incremental)   stg_jobs, stg_salary_history
dbt – Intermediate   (incremental)   int_jobs_enriched
dbt – Marts          (tables)        dim_company, fct_job_postings, fct_skill_demand, fct_salary_trends
```

## Data Model

```mermaid
erDiagram

    dim_company {
        string company_key PK
        string company_name
        string country_code
        int    total_jobs
        int    actual_salary_count
        string primary_seniority
        string primary_work_arrangement
        string primary_location
        float  avg_salary_midpoint
        float  pct_python
        float  pct_sql
        float  pct_aws
        float  pct_spark
        float  pct_databricks
        float  pct_dbt
        float  pct_machine_learning
        date   first_seen
        date   last_seen
    }

    fct_job_postings {
        string job_id PK
        string company_key FK
        string location_name
        string country_code
        string seniority_band
        string work_arrangement
        string contract_type
        bool   job_is_remote
        string salary_bucket
        float  salary_min
        float  salary_max
        float  salary_midpoint
        int    skill_python
        int    skill_sql
        int    skill_spark
        int    skill_aws
        int    skill_dbt
        date   posted_date
    }

    fct_skill_demand {
        string demand_key PK
        date   posted_date
        string skill_name
        string seniority_band
        string country_code
        int    job_count
        int    total_jobs
        float  pct_jobs_mentioning
    }

    fct_salary_trends {
        string trend_key PK
        date   salary_month
        string seniority_band
        string country_code
        int    job_count
        int    jobs_with_actual_salary
        float  avg_salary
        float  median_salary
        float  p25_salary
        float  p75_salary
        float  min_salary
        float  max_salary
    }

    fct_job_postings }o--|| dim_company : "company_key"
```

## Mart Tables

### Dimension Tables

| Table | Grain | Description |
|---|---|---|
| `dim_company` | company + country | Aggregated employer profile: job volume, salary stats, top skill percentages |

### Fact Tables

| Table | Grain | Description |
|---|---|---|
| `fct_job_postings` | one row per job | Central fact table with FK to `dim_company` and 50+ skill flags |
| `fct_skill_demand` | date + skill + seniority + country | Daily skill mention counts and share of postings, for trend charts |
| `fct_salary_trends` | month + seniority + country | Monthly salary aggregates (avg, median, quartiles) from job postings |

## Ingestion Schedules

| Schedule | Endpoint | Queries |
|---|---|---|
| Daily | `job_search` | 8 queries: data/software/ML engineer + analyst roles in Nairobi, remote Africa, UK, US |
| Weekly | `salary_estimate` | 6 benchmarks: key roles in Nairobi, London, New York |

JSearch free tier: **200 req/month**. 8 daily queries = ~240/month — trim or skip weekends to stay within budget.

## Setup

Copy `profiles.yml.example` to `~/.dbt/profiles.yml` and set the following in `.env`:

```
JSEARCH_API_KEY=
JSEARCH_RAW_ZONE_ROOT=/Volumes/job_market/raw/jsearch
DATABRICKS_HOST=
DATABRICKS_HTTP_PATH=
DATABRICKS_TOKEN=
```

Create the Unity Catalog Volume before first run:
```sql
CREATE SCHEMA IF NOT EXISTS job_market.raw;
CREATE VOLUME IF NOT EXISTS job_market.raw.jsearch;
```

Run ingestion:
```bash
uv run --env-file .env python scripts/ingestion/ingest_jobs.py daily
uv run --env-file .env python scripts/ingestion/ingest_jobs.py weekly
```

Run dbt:
```bash
dbt build
```
