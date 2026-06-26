# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

UK job market analytics pipeline: **Adzuna API → raw zone (Delta Lake / object storage) → dbt on Databricks → Streamlit**. The ingestion layer lands raw JSON untouched; all transformation happens in dbt.

## Commands

Use `uv` for Python dependency management:

```bash
uv run python scripts/ingestion/ingest_jobs.py daily    # run daily ingestion locally
uv run python scripts/ingestion/ingest_jobs.py weekly   # run weekly reference ingestion
uv run python -m flake8 scripts/                        # lint Python
uv run pyright scripts/                                 # type-check Python
```

dbt:

```bash
dbt run                                  # run all models
dbt run --select staging.*               # run one layer
dbt run --select int_jobs_enriched+      # run model and all dependents
dbt test                                 # run all tests
dbt test --select stg_jobs               # test one model
sqlfluff lint models/                    # lint SQL
sqlfluff fix models/                     # auto-fix SQL
```

## Required environment variables

Locally, set these in a `.env` file (see `profiles.yml.example`):

| Variable | Purpose |
|---|---|
| `ADZUNA_APP_ID` / `ADZUNA_APP_KEY` | Adzuna API credentials |
| `DATABRICKS_HOST` | Workspace URL |
| `DATABRICKS_HTTP_PATH` | SQL warehouse HTTP path |
| `DATABRICKS_TOKEN` | Personal access token |
| `ADZUNA_RAW_ZONE_ROOT` | Root path for raw Delta partitions (default: `/Volumes/job_market/raw/adzuna`) |
| `DBT_CATALOG` | Unity Catalog name (default: `job_market`) |

On Databricks, secrets are read from the `job-market` secrets scope via `dbutils.secrets` — see `scripts/config.py`.

Adzuna free tier is **250 requests/day**. Don't run daily and weekly ingestion in the same dev session.

## Architecture

### Ingestion layer (`scripts/`)

`scripts/ingestion/ingest_jobs.py` is the main ingestion module. Key design:

- **`ENDPOINTS` registry**: All three Adzuna endpoints (`job_search`, `categories`, `geodata`) are declared as `EndpointConfig` dataclass instances. Adding a new endpoint means adding one entry here — no logic changes elsewhere.
- **`_fetch()`**: Retries on network errors and 5xx with exponential backoff (tenacity). 4xx errors raise immediately without retrying.
- **Raw zone partition path**: `{root}/endpoint={name}/country={country}/ingestion_date={date}/data.json` plus a `_run_metadata.json` audit file alongside.
- **Two schedules**: `run_daily_ingestion` (job_search only) and `run_weekly_reference_ingestion` (categories + geodata). The split exists to conserve the 250 req/day API budget — reference data doesn't change daily.
- **`ingest_endpoint()`** is the orchestrator entry point (Airflow/Dagster task). It raises on zero records fetched — silent empty writes must fail loudly.

`scripts/ingestion/fetch_salaries.py` writes salary histogram and history data directly to Delta tables (`raw.salary_histogram`, `raw.salary_history`) via Spark, not to the file-based raw zone.

### dbt layer (`models/`)

Four layers, materialized differently:

| Layer | Materialization | Purpose |
|---|---|---|
| `staging/` | Incremental (merge on `job_id`) | Type-cast, deduplicate, extract fields from raw JSON files via `read_files()` |
| `intermediate/` | Incremental (merge on `job_id`) | Business logic: skill flags, seniority, work arrangement, salary buckets, location enrichment |
| `core/` | (empty currently) | Reserved for fact tables |
| `marts/` | Table | Dimensional models (`dim_*`) aggregated for consumers — `dim_company`, `dim_location`, `dim_category` |

Tests for all mart models are defined in `models/marts/_marts.yml`: surrogate key uniqueness/not-null, business key not-null, referential integrity (`dim_category` → `stg_categories`), and salary/job-count range checks via `dbt_expectations`.

**Staging reads raw files directly** using Databricks `read_files()` with `_metadata.file_path` to extract partition values (country, ingestion_date). Incremental predicate is on `_file_modification_time` / `_ingested_at`.

**`int_jobs_enriched`** only processes `tech_categories` (`it-jobs`, `engineering-jobs` by default — override via dbt variable `tech_categories`). All skill flags, seniority derivation, and salary bucketing live here, not in staging or marts.

**`int_location_enriched`** does a fuzzy match joining job location strings to Adzuna geodata using tiered `LIKE` matching with `row_number()` to pick the best match.

### Macros (`macros/`)

Three macros in `macros/extract_skills.sql`:

- `skill_flag(column, skill)` — simple `LIKE '%skill%'` match, returns 0/1
- `skill_flag_bounded(column, skill)` — regex word-boundary match for short tokens that appear inside other words (e.g. `r`, `go`, `sql`)
- `salary_bucket(column)` — maps salary midpoint to a string band (under_30k → over_150k)

Use `skill_flag_bounded` for single-word skills prone to false positives.

### dbt packages

- `dbt_utils` — `generate_surrogate_key` used in `dim_company`
- `dbt_expectations` — salary range tests in `_staging.yml`
- `dbt_date` — available but not yet widely used
