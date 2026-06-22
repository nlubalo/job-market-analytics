"""
Adzuna job postings ingestion module

Pulls job postings from the Adzuna API and lands them, untouched, into
a partitioned raw zone (Delta Lake / object storage). No parsing or
transformation happens here.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential
)
logger = logging.getLogger("aduna_ingestion")

ADZUNA_BASE_URL = "https://api.adzuna.com/v1/api/jobs"
RESULTS_PER_PAGE = 50  # Adzuna's max per page
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 3

class AdzunaAPIError(Exception):
    """Raised when the Aduna API returns a non-retryable error"""

@dataclass
class EndpointConfig:
    """
    Describes ond Adzuna endpoint shape
    name: short identifier used as the raw-zone partition name
    path_template: appended to ADZUNA_BASE_URL/{country} e.g "search/{page} or 
                    "categories"
    paginated:    Whether this endpoint supports page-by-page results
    results_key:  Dict key in the JSON json holding the list of records
    extra_params: Endpoint-specific query params; a value of None marks the key
                  as required - the caller must supply it at call time
    record_in_fn: Given one record, return a stable id for within-run dedup;
                    None if the ednpoint doesn't return dedupeable records
    """
    
    name: str
    path_template: str
    paginated: bool = True
    results_key: str | None = "results"
    extra_params: dict[str, Any] = field(default_factory=dict)
    record_id_fn: Callable[[dict[str, Any]], str | None] | None = None
    
# --- Endpoint registry --------------------------------------------------
# Adding a new Adzuna endpoint = adding one entry here, nothing else.

ENDPOINTS: dict[str, EndpointConfig]={
    "job_search": EndpointConfig(
        name="job_search",
        path_template="search/{page}",
        paginated=True,
        results_key="results",
        extra_params={"sort_by": "date", "max_days_old": 2},
        record_id_fn=lambda r: r.get("id")
        
    ),
    "categories": EndpointConfig(
        name="categories",
        path_template="categories",
        paginated=False,
        results_key="results",
        record_id_fn=lambda r: r.get("tag")
    ),
    "geodata": EndpointConfig(
        name="geodata",
        path_template="geodata",
        paginated=False,
        results_key="locations",
        extra_params={"location0": "UK"},  # sensible default; caller can override per-country
        record_id_fn=lambda r: (r.get("location") or {}).get("display_name"),
    )
}
@dataclass
class IngestionResults:
    """Metadata about a single ingestion run - written alongside the raw data so
    failures and partial runs are auditable
    """
    endpoint: str
    country: str
    category: str
    ingestion_date: date
    pages_fetched: int =0
    pages_failed: int = 0
    records_fetched: int = 0
    started_at: datetime = field(default_factory=datetime.utcnow)
    finished_at: datetime | None = None
    status: str = "running" # running | success | partial_failure | failed
    error_messages: list[str] = field(default_factory=list)
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "endpoint": self.endpoint,
            "country": self.country,
            "category": self.category,
            "ingestion_date": self.ingestion_date.isoformat(),
            "pages_fetched": self.pages_fetched,
            "pages_failed": self.pages_failed,
            "records_fetched": self.records_fetched,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "status": self.status,
            "error_messages": self.error_messages
        }
        

@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    retry=retry_if_exception_type((requests.exceptions.RequestException,)),
    reraise=True,
)
def _fetch(
    endpoint: EndpointConfig,
    country: str,
    app_id: str,
    app_key: str,
    page: int = 1,
    call_params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Fetch a single page/response from any Adzuna endpoint. Retries on
    network/5xx errors with exponential backoff. Does NOT retry on 4xx —
    those are our bug (bad params, bad auth), so they should surface
    immediately rather than burning 3 retries on a guaranteed failure.
    """
    path = endpoint.path_template.format(page=page) if endpoint.paginated else endpoint.path_template
    url = f"{ADZUNA_BASE_URL}/{country}/{path}"
 
    params: dict[str, Any] = {"app_id": app_id, "app_key": app_key}
    if endpoint.paginated:
        params["results_per_page"] = RESULTS_PER_PAGE
 
    # Defaults from the endpoint config, then caller overrides/fills required ones
    params.update({k: v for k, v in endpoint.extra_params.items() if v is not None})
    if call_params:
        params.update(call_params)
 
    missing_required = [
        k for k, v in endpoint.extra_params.items()
        if v is None and k not in params
    ]
    if missing_required:
        raise AdzunaAPIError(
            f"Endpoint '{endpoint.name}' requires params {missing_required}, "
            f"none supplied via call_params"
        )
 
    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
 
    if 400 <= response.status_code < 500:
        raise AdzunaAPIError(
            f"Adzuna API client error {response.status_code} on "
            f"endpoint={endpoint.name} page={page}: {response.text[:500]}"
        )
 
    response.raise_for_status()  # raises for 5xx, which IS retried
    return response.json()
    
    
def fetch_endpoint_data(
    endpoint_name: str,
    country: str,
    app_id: str,
    app_key: str,
    call_params: dict[str, Any] | None = None,
    max_pages: int = 50,
) -> tuple[list[dict[str, Any]] | dict[str, Any], IngestionResults]:
    """
    Fetch data from any registered Adzuna endpoint.
 
    Returns either a list of records (paginated endpoints, or
    non-paginated endpoints with a list-shaped results_key) or a single
    dict (endpoints with results_key=None, e.g. salary_histogram).
 
    Behavior is branch-free per call site — the SAME function handles
    job_search's 40-page loop and categories' single call, because the
    pagination logic only activates when endpoint.paginated is True.
    """
    endpoint = ENDPOINTS.get(endpoint_name)
    if endpoint is None:
        raise ValueError(
            f"Unknown endpoint '{endpoint_name}'. Registered: {list(ENDPOINTS)}"
        )
 
    run_result = IngestionResults(
        endpoint=endpoint_name,
        country=country,
        category=(call_params or {}).get("category", "all"),
        ingestion_date=date.today(),
    )
    
    # --- Non-paginated endpoints: one call, done ---
    if not endpoint.paginated:
        try:
            data = _fetch(endpoint, country, app_id, app_key, call_params=call_params)
        except (AdzunaAPIError, requests.exceptions.RequestException) as e:
            run_result.pages_failed = 1
            run_result.status = "failed"
            run_result.error_messages.append(str(e))
            run_result.finished_at = datetime.now(UTC)
            return ([] if endpoint.results_key else {}), run_result
        
        run_result.pages_fetched = 1
        run_result.status = "success"
        run_result.finished_at = datetime.now(UTC)
        
        if endpoint.results_key is None:
            # Single-object response (e.g. salary_histogram) — no list to dedup
            run_result.records_fetched = 1
            return data, run_result
 
        records = data.get(endpoint.results_key, [])
        run_result.records_fetched = len(records)
        return records, run_result
    
    # --- Paginated endpoints (currently: job_search) ---
    all_records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    page = 1
    total_pages_estimate: int | None = None
    
    while page <= max_pages:
        try:
            data = _fetch(endpoint, country, app_id, app_key, page=page, call_params=call_params)
        except AdzunaAPIError as e:
            logger.error("Page %d failed (non-retryable): %s", page, e)
            run_result.pages_failed += 1
            run_result.error_messages.append(str(e))
            page += 1
            continue
        except requests.exceptions.RequestException as e:
            logger.error("Page %d failed after retries: %s", page, e)
            run_result.pages_failed += 1
            run_result.error_messages.append(f"page {page}: {e}")
            page += 1
            continue
 
        results = data.get(endpoint.results_key, [])
        
        if total_pages_estimate is None:
            total_count = data.get("count", 0)
            total_pages_estimate = max(1, -(-total_count // RESULTS_PER_PAGE))
            logger.info(
                "Endpoint %s reports %d total results (~%d pages) for country=%s",
                endpoint_name, total_count, total_pages_estimate, country,
            )
        
        if not results:
            logger.info("Page %d returned no results — stopping pagination.", page)
            break
        
        new_count = 0
        for record in results:
            record_id = endpoint.record_id_fn(record) if endpoint.record_id_fn else None
            if record_id is None or record_id not in seen_ids:
                if record_id is not None:
                    seen_ids.add(record_id)
                all_records.append(record)
                new_count += 1
 
        run_result.pages_fetched += 1
        run_result.records_fetched += new_count
        
        logger.info(
            "Page %d/%s: fetched %d records (%d new)",
            page, total_pages_estimate, len(results), new_count,
        )
 
        if page >= total_pages_estimate:
            break
 
        page += 1
        time.sleep(0.25)  # gentle pacing — stay well under rate limits
 
    run_result.finished_at = datetime.now(UTC)
    if run_result.pages_failed == 0:
        run_result.status = "success"
    elif run_result.records_fetched > 0:
        run_result.status = "partial_failure"
    else:
        run_result.status = "failed"
 
    return all_records, run_result
    
    
    
    
def write_raw_zone(
    payload: list[dict[str, Any]] | dict[str, Any],
    run_result: IngestionResults,
    raw_zone_root: str,
) -> Path:
    """
    Write the raw, untouched response to a partitioned landing zone:
 
        {raw_zone_root}/endpoint={name}/country={country}/ingestion_date={date}/data.json
        {raw_zone_root}/endpoint={name}/country={country}/ingestion_date={date}/_run_metadata.json
 
    Partitioning by endpoint first means each endpoint's staging model
    reads from its own clean subtree — job_search staging never
    accidentally globs in top_companies files. ingestion_date (not
    posted_date) means any day can be reprocessed without re-hitting
    the API. The raw zone is append-only — never overwritten in place.
    """
    partition_dir = (
        Path(raw_zone_root)
        / f"endpoint={run_result.endpoint}"
        / f"country={run_result.country}"
        / f"ingestion_date={run_result.ingestion_date.isoformat()}"
    )
    partition_dir.mkdir(parents=True, exist_ok=True)
 
    data_path = partition_dir / "data.json"
    with data_path.open("w") as f:
        if isinstance(payload, list):
            for record in payload:
                f.write(json.dumps(record) + "\n")  # newline-delimited JSON
        else:
            json.dump(payload, f)  # single-object response, written as-is
 
    metadata_path = partition_dir / "_run_metadata.json"
    with metadata_path.open("w") as f:
        json.dump(run_result.to_dict(), f, indent=2)
 
    logger.info(
        "Wrote endpoint=%s data to %s (status=%s, pages_failed=%d)",
        run_result.endpoint, data_path, run_result.status, run_result.pages_failed,
    )
 
    return data_path



def ingest_endpoint(
    endpoint_name: str,
    country: str,
    app_id: str,
    app_key: str,
    raw_zone_root: str,
    call_params: dict[str, Any] | None = None,
) -> IngestionResults:
    """
    Entry point — what the orchestrator (Airflow/Dagster task, or a
    Foundry transform) calls. One call per endpoint per country.
 
    Raises AdzunaAPIError only if the run produced zero usable data —
    a fully empty/failed run is a signal worth failing the DAG task on,
    not silently writing nothing and letting it propagate as "no data
    today" downstream.
    """
    logger.info(
        "Starting Adzuna ingestion: endpoint=%s country=%s params=%s",
        endpoint_name, country, call_params,
    )
 
    payload, run_result = fetch_endpoint_data(
        endpoint_name=endpoint_name,
        country=country,
        app_id=app_id,
        app_key=app_key,
        call_params=call_params,
    )
 
    write_raw_zone(payload, run_result, raw_zone_root)
 
    if run_result.status == "failed":
        raise AdzunaAPIError(
            f"Ingestion failed completely for endpoint={endpoint_name} "
            f"country={country}: {run_result.error_messages}"
        )
 
    return run_result


def run_daily_ingestion(
    country: str,
    app_id: str,
    app_key: str,
    raw_zone_root: str,
) -> list[IngestionResults]:
    """
    Daily cadence: job postings change every day, so this is the only
    endpoint that needs to run on a daily schedule.
    """
    return [
        ingest_endpoint("job_search", country, app_id, app_key, raw_zone_root),
    ]
    

def run_weekly_reference_ingestion(
    country: str,
    app_id: str,
    app_key: str,
    raw_zone_root: str,
) -> list[IngestionResults]:
    """
    Weekly cadence: categories and geodata are reference/dimensional
    data, not facts. Adzuna's category taxonomy and regional hierarchy
    don't change day to day — running these daily would burn rate-limit
    budget (250 req/day on the free tier) for zero new information.
 
    These feed dim_category and dim_location in the mart layer, joined
    against job_search facts rather than unioned with them.
    """
    return [
        ingest_endpoint("categories", country, app_id, app_key, raw_zone_root),
        ingest_endpoint(
            "geodata", country, app_id, app_key, raw_zone_root,
            call_params={"location0": "UK"},
        ),
    ]
 


if __name__ == "__main__":
    import os
    import sys
 
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
 
    app_id = os.environ["ADZUNA_APP_ID"]
    app_key = os.environ["ADZUNA_APP_KEY"]
    raw_zone_root = os.environ.get("ADZUNA_RAW_ZONE_ROOT", "/data/raw/adzuna")
    country = os.environ.get("ADZUNA_COUNTRY", "gb")
 
    # Orchestrator (Airflow/Dagster) calls these as two separate tasks
    # on two separate schedules — not one undifferentiated "run everything"
    # job. Pass "daily" or "weekly" to simulate which DAG task is firing.
    mode = sys.argv[1] if len(sys.argv) > 1 else "daily"
 
    if mode == "daily":
        results = run_daily_ingestion(country, app_id, app_key, raw_zone_root)
    elif mode == "weekly":
        results = run_weekly_reference_ingestion(country, app_id, app_key, raw_zone_root)
    else:
        raise ValueError(f"Unknown mode '{mode}', expected 'daily' or 'weekly'")
 
    for r in results:
        print(json.dumps(r.to_dict(), indent=2))