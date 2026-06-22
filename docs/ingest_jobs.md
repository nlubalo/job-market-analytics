# `ingest_jobs.py` — Overview

Pulls raw job data from the Adzuna API and lands it untouched into a partitioned file system (Delta Lake / object storage). No transformation happens here.

---

## Key building blocks

### `EndpointConfig` + `ENDPOINTS` registry

Defines three Adzuna endpoints as config objects rather than hardcoded logic:

| Endpoint | Type | Description |
|---|---|---|
| `job_search` | Paginated | Fetches jobs posted in the last 2 days |
| `categories` | One-shot | Fetches the category taxonomy |
| `geodata` | One-shot | Fetches location / regional hierarchy |

Adding a new endpoint only requires a new entry in `ENDPOINTS` — no code changes elsewhere.

### `_fetch()`

Low-level HTTP call with `tenacity` retry logic. Retries on network errors and 5xx with exponential backoff, but immediately raises on 4xx (bad auth / params — no point retrying).

### `fetch_endpoint_data()`

Orchestrates the full fetch for any endpoint — handles both single-call (categories, geodata) and paginated (job_search, up to 50 pages × 50 results). Deduplicates within a run using `seen_ids`. Returns the records plus an `IngestionResults` audit object.

### `write_raw_zone()`

Writes data to a Hive-style partition path:

```
{root}/endpoint={name}/country={country}/ingestion_date={date}/data.json
```

List payloads are written as newline-delimited JSON (one record per line). Also writes a `_run_metadata.json` alongside for auditability.

### `ingest_endpoint()`

The entry point called by an orchestrator (Airflow / Dagster). Fails the task loudly if zero records were fetched — a silent empty write would look like "no jobs today" downstream.

### Two schedules

| Function | Cadence | Endpoints |
|---|---|---|
| `run_daily_ingestion` | Daily | `job_search` |
| `run_weekly_reference_ingestion` | Weekly | `categories`, `geodata` |

Categories and geodata are dimension / reference data that barely changes. Running them daily would burn the 250 req/day free-tier budget for zero new information.

---
