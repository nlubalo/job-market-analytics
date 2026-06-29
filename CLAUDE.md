# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

Job market analytics pipeline focused on Kenya / East Africa with global salary benchmarks: **JSearch API (OpenWebNinja) → raw zone (Databricks Unity Catalog Volumes) → dbt on Databricks → Streamlit**. The ingestion layer lands raw JSON untouched; all transformation happens in dbt.

## Commands

Use `uv` for Python dependency management. Always load env vars with `--env-file`:

```bash
uv run --env-file .env python scripts/ingestion/ingest_jobs.py daily    # daily ingestion
uv run --env-file .env python scripts/ingestion/ingest_jobs.py weekly   # weekly salary benchmarks
uv run python -m flake8 scripts/                                         # lint Python
uv run pyright scripts/                                                  # type-check Python
```

dbt:

```bash
dbt run                                  # run all models
dbt run --select staging.*               # run one layer
dbt run --select int_jobs_enriched+      # run model and all dependents
dbt build                                # run + test all models
dbt test --select stg_jobs               # test one model
sqlfluff lint models/                    # lint SQL
sqlfluff fix models/                     # auto-fix SQL
```

## Required environment variables

Set in `.env` locally (see `profiles.yml.example`). On Databricks, secrets are read from the `job-market` scope via `dbutils.secrets`:

| Variable | Purpose |
|---|---|
| `JSEARCH_API_KEY` | JSearch (OpenWebNinja) API key |
| `JSEARCH_RAW_ZONE_ROOT` | Root path for raw partitions (default: `/Volumes/job_market/raw/jsearch`) |
| `DATABRICKS_HOST` | Workspace URL |
| `DATABRICKS_HTTP_PATH` | SQL warehouse HTTP path |
| `DATABRICKS_TOKEN` | Personal access token |
| `DBT_CATALOG` | Unity Catalog name (default: `job_market`) |

JSearch free tier is **200 requests/month**. `DAILY_QUERIES` has 8 queries — at daily cadence that's ~240/month, slightly over budget. Trim queries or skip weekends if staying on the free tier.

Before first run, create the Unity Catalog Volume:
```sql
CREATE SCHEMA IF NOT EXISTS job_market.raw;
CREATE VOLUME IF NOT EXISTS job_market.raw.jsearch;
```

## Architecture

### Ingestion layer (`scripts/`)

`scripts/ingestion/ingest_jobs.py` is the main ingestion module. Key design:

- **`ENDPOINTS` registry**: Two JSearch endpoints (`job_search`, `salary_estimate`) declared as `EndpointConfig` dataclass instances. Adding a new endpoint means adding one entry here — no logic changes elsewhere.
- **`results_subkey`**: JSearch `search-v2` wraps results at `data["data"]["jobs"]` and cursor at `data["data"]["cursor"]`. `results_subkey="jobs"` on the `job_search` config handles this — other endpoints without a subkey get `data["data"]` directly.
- **`_fetch()`**: Retries on network errors and 5xx with exponential backoff (tenacity). 4xx errors raise immediately without retrying.
- **`_write_text()`**: Detects `/Volumes/` paths and routes to `dbutils.fs.put()` — standard `pathlib` doesn't work on Unity Catalog Volumes.
- **Raw zone partition path**: `{root}/endpoint={name}/ingestion_date={date}/query={query}/data.jsonl` plus `_run_metadata.json` alongside. Re-running the same query+date overwrites the partition.
- **Two schedules**: `run_daily_ingestion` (8 job search queries for Kenya/remote/benchmark markets) and `run_weekly_reference_ingestion` (6 salary estimates by role + location). Weekly failures per benchmark are caught and logged — one bad location won't kill the rest.

### dbt layer (`models/`)

| Layer | Materialization | Models |
|---|---|---|
| `staging/` | Incremental (merge on `job_id`) | `stg_jobs`, `stg_salary_history` |
| `intermediate/` | Incremental (merge on `job_id`) | `int_jobs_enriched` |
| `marts/` | Table | `dim_company`, `fct_job_postings`, `fct_skill_demand`, `fct_salary_trends` |

**Staging** reads raw `.jsonl` files via Databricks `read_files()`. Incremental predicate is on `_metadata.file_modification_time`. `stg_jobs` maps JSearch fields (`employer_name` → `company_name`, `job_employment_type` → `contract_type`, `job_posted_at_datetime_utc` → `posted_timestamp`). `location_name` is derived from `job_city` with fallback to `job_state` then `job_country`.

**`int_jobs_enriched`** adds all 50+ skill flags, seniority derivation, work arrangement detection, and salary bucketing. `job_is_remote` (JSearch boolean) is used directly for remote detection — text scanning only as fallback for hybrid. No category filter — JSearch has no category taxonomy.

**Mart grains:**
- `dim_company` — company + country
- `fct_job_postings` — one row per job, FK to `dim_company` only
- `fct_skill_demand` — date + skill + seniority + country (no category)
- `fct_salary_trends` — month + seniority + country (no category)

### Macros (`macros/`)

- `skill_flag(column, skill)` — `LIKE '%skill%'` match, returns 0/1
- `skill_flag_bounded(column, skill)` — regex word-boundary match for short tokens (`r`, `go`, `sql`)
- `salary_bucket(column)` — maps salary midpoint to a string band (under_30k → over_150k)

Use `skill_flag_bounded` for single-word skills prone to false positives.

### dbt packages

- `dbt_utils` — `generate_surrogate_key` used in `dim_company` and fact tables
- `dbt_expectations` — salary range tests in `_staging.yml` and `_marts.yml`
