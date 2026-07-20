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

Five layers: `staging/` → `intermediate/` → `dimensions/` + `facts/` → `marts/`.

| Layer | Materialization | Models |
|---|---|---|
| `staging/` | Incremental (merge on `job_id`, `ingestion_date`) | `stg_jobs`, `stg_salary_history` |
| `intermediate/` | Mixed — see below | `int_jobs_enriched`, `int_job_titles_normalized`, `int_company_duplicates`, `int_salary_estimates` |
| `dimensions/` | Table | `dim_company`, `dim_date`, `dim_job_title`, `dim_location` |
| `facts/` | Mixed — see below | `fct_job_postings`, `fct_job_posting_skills`, `fct_salary_estimates` |
| `marts/` | Table | `mart_job_market_benchmarks`, `mart_salary_trends`, `mart_skill_demand` |

**Staging** reads raw `.jsonl` files via Databricks `read_files()`. Incremental predicate is on `_metadata.file_modification_time`. `stg_jobs` maps JSearch fields (`employer_name` → `company_name`, `job_employment_type` → `contract_type`, `job_posted_at_datetime_utc` → `posted_timestamp`). `location_name` is derived from `job_city` with fallback to `job_state` then `job_country`.

**Intermediate:**
- `int_jobs_enriched` (incremental, merge on `job_id`) — adds all 50+ skill flags, seniority derivation, work arrangement detection, and salary bucketing. `job_is_remote` (JSearch boolean) is used directly for remote detection — text scanning only as fallback for hybrid. No category filter — JSearch has no category taxonomy.
- `int_salary_estimates` (incremental, merge on `title_normalized` + `location_key` + `salary_currency` + `ingestion_date`) — cleans and enriches raw salary-estimate endpoint results: title/location normalization, annualization, currency-aware bucketing, FK key assignment, reliability scoring.
- `int_job_titles_normalized` (view) — one row per distinct normalized title, deduped from staging.
- `int_company_duplicates` (table) — informational-only data-quality model surfacing probable duplicate companies via Levenshtein distance + website matching. Does not feed any dimension; run periodically, not on every `dbt run`.

**Dimensions** — conformed, shared across facts:
- `dim_company` — one row per normalized company; built only from `int_jobs_enriched` (salary endpoint has no employer field). STRING (md5) key.
- `dim_job_title` — one row per normalized job title. BIGINT key (hash exceeds INT range).
- `dim_location` — one row per distinct (city, country); exposes `city_clean`/`country_clean` (not `city`/`country`). STRING (md5) key, identical hashing logic in both intermediate models so FKs align.
- `dim_date` — static calendar dimension, 2020-01-01 to 2030-12-31.

**Facts:**
- `fct_job_postings` (incremental, merge on `job_id`) — one row per posting; FK-validated projection of `int_jobs_enriched`. Skill flags are NOT in this table — see `fct_job_posting_skills`.
- `fct_job_posting_skills` (table) — bridge table, one row per (`job_id_source`, `skill_name`); unpivots the wide boolean skill flags into long format for clean `count(distinct job_id) where skill_name = 'x'` aggregation.
- `fct_salary_estimates` (incremental, merge on `salary_estimate_key`) — one row per (title, location, currency, ingestion_date) snapshot; carries both total-comp and base-salary columns so marts can pick either without recomputing annualization.

**Marts:**
- `mart_job_market_benchmarks` — grain: (title_normalized, market). Primary Kenya/UK/US benchmarking output; combines posting volume with the most recent salary estimate per (title, location, currency). Disclosed salaries on postings are not used — Kenya disclosure rates are too low to be meaningful.
- `mart_salary_trends` — grain: (title_normalized, market, salary_currency, ingestion_date). Retains every historical salary snapshot (unlike the benchmarks mart) with lag/first-value window functions for period-over-period and cumulative change metrics.
- `mart_skill_demand` — grain: (skill_name, role_family, market). Aggregates `fct_job_posting_skills` to show skill prevalence and rate by market/seniority.

Both `mart_job_market_benchmarks` and `mart_salary_trends` document an intended `record_quality`/`estimate_reliability` filter (high/medium only) in their header comments, but neither currently applies it — worth checking before trusting low-confidence estimates in either mart.

### Macros (`macros/`)

- `skill_flag(column, terms)` — `LIKE '%term%'` match (any of a list of terms), returns 0/1
- `skill_count(column, skills)` — sums `skill_flag` across a dict of `{skill: terms}`, for a per-posting skill-count column
- `skill_flag_bounded(column, skill)` — regex word-boundary match for short tokens (`r`, `go`, `sql`)
- `salary_bucket(column)` — maps salary midpoint to a string band (under_30k → over_150k)
- `derive_seniority_level(column_name)` / `derive_seniority_rank(column_name)` — classify a normalized title into a seniority label and an integer rank (for correct BI sort order, since "Junior" < "Senior" alphabetically is wrong)
- `normalize_job_title(column_name)` — cleans/standardizes raw job title text
- `location_normalization(location)` / `employer_name_normalization(company_name)` — lowercase/strip normalization for location and company name matching

Use `skill_flag_bounded` for single-word skills prone to false positives.

### dbt packages

- `dbt_utils`, `dbt_expectations` — declared in `packages.yml` but currently unused in any model or schema `.yml`. Dimension/fact surrogate keys are hand-rolled (`md5()`, `conv(substr(md5(...)), ...)`) rather than `dbt_utils.generate_surrogate_key`. `_marts.yml` (which used `dbt_expectations` for salary range tests) was deleted and not yet replaced — `fct_*` and `mart_*` models currently have no schema tests or docs.
