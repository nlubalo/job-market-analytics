# Job Market Analytics

UK job market analytics pipeline built on Adzuna API data, Databricks Delta Lake, and dbt.

## Pipeline Overview

```
Adzuna API
    │
    ▼
Raw Zone (Delta Lake / object storage)
  endpoint=job_search/country=gb/ingestion_date=YYYY-MM-DD/data.json
  endpoint=categories/...
  endpoint=geodata/...
    │
    ▼
dbt – Staging        (views)       stg_jobs, stg_categories, stg_geodata, stg_salary_history
dbt – Intermediate   (tables)      int_jobs_enriched, int_location_enriched
dbt – Marts          (tables)      dim_*, fct_*
```

## Data Model

```mermaid
erDiagram

    dim_company {
        string company_key PK
        string company_name
        string country_code
        int    total_jobs
        string primary_category
        string primary_seniority
        string primary_location
        float  avg_salary_midpoint
        float  pct_python
        float  pct_sql
        float  pct_aws
        date   first_seen
        date   last_seen
    }

    dim_location {
        string location_key PK
        string location_name
        string location_area
        string country_code
        string geo_location_name
        int    total_jobs
        float  avg_salary_midpoint
        string primary_category
        string primary_seniority
        date   first_seen
        date   last_seen
    }

    dim_category {
        string category_key PK
        string category_tag
        string category_label
        string country_code
    }

    fct_job_postings {
        string job_id PK
        string company_key FK
        string location_key FK
        string category_key FK
        string seniority_band
        string work_arrangement
        string contract_type
        string salary_bucket
        float  salary_min
        float  salary_max
        float  salary_midpoint
        bool   salary_is_predicted
        int    skill_python
        int    skill_sql
        int    skill_spark
        int    skill_aws
        date   posted_date
        string country_code
    }

    fct_skill_demand {
        string demand_key PK
        date   posted_date
        string skill_name
        string category_tag
        string seniority_band
        string country_code
        int    job_count
        int    total_jobs
        float  pct_jobs_mentioning
    }

    fct_salary_trends {
        string trend_key PK
        date   salary_month
        string category_tag
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

    fct_job_postings }o--|| dim_company  : "company_key"
    fct_job_postings }o--|| dim_location : "location_key"
    fct_job_postings }o--|| dim_category : "category_key"
```

## Mart Tables

### Dimension Tables

| Table | Grain | Description |
|---|---|---|
| `dim_company` | company + country | Aggregated profile per employer: job volume, salary stats, top skills |
| `dim_location` | location + country | Geographic dimension enriched with Adzuna geodata |
| `dim_category` | category tag + country | Adzuna job category taxonomy |

### Fact Tables

| Table | Grain | Description |
|---|---|---|
| `fct_job_postings` | one row per job | Central fact table with FK references to all dims and 50+ skill flags |
| `fct_skill_demand` | date + skill + category + seniority | Daily skill mention counts and share of postings, for trend analysis |
| `fct_salary_trends` | month + category + seniority + country | Monthly salary aggregates (avg, median, quartiles) from actual postings only |

## Ingestion Schedules

| Schedule | Endpoints | Reason |
|---|---|---|
| Daily | `job_search` | Job postings change every day |
| Weekly | `categories`, `geodata` | Reference data — rarely changes; running daily would burn the 250 req/day free-tier budget |

## Setup

Copy `profiles.yml.example` to `~/.dbt/profiles.yml` and set the following environment variables (or add to `.env`):

```
DATABRICKS_HOST=
DATABRICKS_HTTP_PATH=
DATABRICKS_TOKEN=
ADZUNA_APP_ID=
ADZUNA_APP_KEY=
ADZUNA_RAW_ZONE_ROOT=/Volumes/job_market/raw/adzuna
```

Run ingestion:
```bash
uv run python scripts/ingestion/ingest_jobs.py daily
uv run python scripts/ingestion/ingest_jobs.py weekly
```

Run dbt:
```bash
dbt run
dbt test
```
