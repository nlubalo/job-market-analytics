"""
JSearch job postings ingestion module

Pulls job postings from the JSearch API (OpenWeb Ninja) and lands them,
untouched, into a partitioned raw zone (Delta Lake / object storage).
No parsing or transformation happens here.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Callable

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential
)

logger = logging.getLogger("jsearch_ingestion")

JSEARCH_BASE_URL = "https://api.openwebninja.com/jsearch"
RESULTS_PER_PAGE = 10   # JSearch max per request
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 3

class JsearchAPIError(Exception):
    """Raised when the JSearch API returns a non-retryable error."""

# ---------------------------------------------------------------------------
# Endpoint registry
# ---------------------------------------------------------------------------
# Adding a new JSearch endpoint = one

@dataclass
class EndpointConfig:
    """
    Describes one JSearch endpoint shape

    name: Short identifier used as the raw-zone partition name.
    path: Path segment appended to JSEARCH_BASE_URL
    paginated: Whether this endpoint uses cursor-based pagination
    results_key: Dict key in the JSON response holding the list of records
    extra_params: Given query params. None value = required at call time
    record_id_fn: Given one record, return a stable id for within-run dedup.
                  None if the endpoint doesn't return dedupeable records.
    """
    name: str
    path: str
    paginated: bool = True
    results_key: str | None = "data"
    results_subkey: str | None = None
    extra_params: dict[str, Any] = field(default_factory=dict)
    record_id_fn: Callable[[dict[str, Any]], str | None] | None = None

ENDPOINTS: dict[str, EndpointConfig] = {
    "job_search": EndpointConfig(
        name="job_search",
        path="search-v2",
        paginated=True,
        results_key="data",
        results_subkey="jobs",
        extra_params={
            "date_posted": "today",
            "results_per_page": RESULTS_PER_PAGE,
            # query is required — caller must supply it via call_params
            "query": None,
        },
        record_id_fn=lambda r: r.get('job_id')

    ),
    "salary_estimate": EndpointConfig(
        name="salary_estimate",
        path="estimated-salary",
        paginated=False,
        results_key="data",
        extra_params={
            # job_title and location are required — caller must supply
            "job_title": None,
            "location": None,
        },
        record_id_fn=lambda r: f"{r.get('job_title')}|{r.get('location')}",

    )
}

@dataclass
class IngestionResult:
    """
    Metadata about a single ingestion run. Written alongside the raw data
    so failures and aprtial runs are fully auditable
    """
    endpoint: str
    query: str
    ingestion_date: date
    pages_fetched: int = 0
    pages_failed: int = 0
    records_fetched: int = 0
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None
    status: str = "running" # running | success | partial_failure | failed
    error_messages: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "endpoint": self.endpoint,
            "query":    self.query,
            "ingestion_date":  self.ingestion_date.isoformat(),
            "pages_fetched":   self.pages_fetched,
            "pages_failed":    self.pages_failed,
            "records_fetched": self.records_fetched,
            "started_at":      self.started_at.isoformat(),
            "finished_at":     self.finished_at.isoformat() if self.finished_at else None,
            "status":          self.status,
            "error_messages":  self.error_messages,
        }

@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    reraise=True,
)
def _fetch(
    endpoint: EndpointConfig,
    api_key: str,
    call_params: dict[str, Any],
    cursor: str | None = None
    ) -> dict[str, Any]:
    """
    Fetch a single page/response from any JSearch endpoint

    Retries on newtwork errors and 5xx responses with exponetial backoff
    Does NOT retry on 4xx - those are caller bugs (bad params, bad auth)
    and should surface immediately
    """
    url = f"{JSEARCH_BASE_URL}/{endpoint.path}"

    headers ={
        "x-api-key": api_key,
        "Content-Type": "application/json",
    }
    # Strat with endpoint defaults, then apply caller overrides
    params: dict[str, Any] ={
        k: v for k, v in endpoint.extra_params.items() if v is not None
    }
    params.update(call_params)
    # Cursor drives pagination — only set when we're past page 1
    if cursor:
        params["cursor"] = cursor
    # Guard: surface missing required params before hitting the API
    missing = [
        k for k, v in endpoint.extra_params.items()
        if v is None and k not in params
    ]
    if missing:
        raise JsearchAPIError(
            f"Endpoint '{endpoint.name}' requires params {missing} "
            f"but none were supplied via call_params."
        )
    response = requests.get(
        url, headers=headers, params=params, timeout=REQUEST_TIMEOUT_SECONDS
    )
    # 4xx = our bug — don't retry, surface immediately
    if 400 <= response.status_code < 500:
        raise JsearchAPIError(
            f"JSearch API client error {response.status_code} on "
            f"endpoint={endpoint.name}: {response.text[:500]}"
        )
    response.raise_for_status() # 5xx raises RequestException — IS retried
    return response.json()



# Pagination + collection

def fetch_endpoint_data(
    endpoint_name: str,
    api_key: str,
    call_params: dict[str, Any] | None = None,
    max_pages: int = 50
    ) -> tuple[list[dict[str, Any]] | dict[str, Any], IngestionResult]:
    """
    Fetch data from any registered JSearch enpoint

    For paginated endpoints: iterates cursor-by-curosr until the APU
    returns no next cursor or no reults. For non-paginated endpoints: single record call is done

    Returns (records, run_metadata). The caller never needs to know
    which branch ran.
    """
    endpoint = ENDPOINTS.get(endpoint_name)
    if endpoint is None:
        raise ValueError(
            f"Unknown endpoint '{endpoint_name}'."
            f"Registered: {list(ENDPOINTS)}"
        )
    call_params = call_params or {}
    query_label = call_params.get("query", endpoint_name)

    run = IngestionResult(
        endpoint=endpoint_name,
        query=query_label,
        ingestion_date=date.today()
    )
    # Non-paginated endpoints: single call
    if not endpoint.paginated:
        try:
            data = _fetch(endpoint, api_key, call_params)
        except (JsearchAPIError, requests.exceptions.RequestException) as e:
            run.pages_failed = 1
            run.status = "failed"
            run.error_messages.append(str(e))
            run.finished_at = datetime.now(UTC)
            return ([] if endpoint.results_key else {}), run

        run.pages_fetched = 1
        run.finished_at = datetime.now(UTC)
        run.status = "success"

        if endpoint.results_key is None:
            run.records_fetched = 1
            return data, run

        records = data.get(endpoint.results_key, [])
        run.records_fetched = len(records)
        return records, run

    # -----------------------------------------------------------------------
    # Paginated endpoints: cursor loop
    # The API tells us there's a next page by returning a cursor value.
    # When cursor is absent or empty, we're done.
    # -----------------------------------------------------------------------

    all_records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    cursor: str | None = None
    page_num = 0
    
    while page_num < max_pages:
        try:
            data = _fetch(endpoint, api_key, call_params, cursor=cursor)
        except JsearchAPIError as e:
            # Non-retryable (4xx) — log and stop; don't skip to next page
            logger.error("Non-retryable error on page %d: %s", page_num + 1, e)
            run.pages_failed += 1
            run.error_messages.append(str(e))
            break
        except requests.exceptions.RequestException as e:
            # Network/5xx exhausted retries — log and continue to next page
            logger.error("Page %d failed after retries: %s", page_num + 1, e)
            run.pages_failed += 1
            run.error_messages.append(f"page {page_num + 1}: {e}")
            # Advance cursor is unknown — we can't safely continue pagination
            break

        outer = data.get(endpoint.results_key or "", [])
        page_results: list[dict] = outer.get(endpoint.results_subkey, []) if endpoint.results_subkey else outer
        page_num += 1
        run.pages_fetched += 1

        if not page_results:
            logger.info("Page %d returned no results — stopping.", page_num)
            break
        # Within-run dedup on job_id (same job can appear across queries)
        new_count = 0
        for record in page_results:
            record_id = (
                endpoint.record_id_fn(record)
                if endpoint.record_id_fn
                else None
            )
            if record_id is None or record_id not in seen_ids:
                if record_id is not None:
                    seen_ids.add(record_id)
                all_records.append(record)
                new_count += 1
        run.records_fetched += new_count
        logger.info(
            "Page %d: %d results, %d new (total so far: %d)",
            page_num, len(page_results), new_count, run.records_fetched,
        )
        # JSearch cursor lives at data["data"]["cursor"]
        # Absence of cursor = no more pages
        cursor = data.get("data", {}).get("cursor")
        if not cursor:
            logger.info("No next cursor — pagination complete.")
            break

        time.sleep(0.3)   # stay well under rate limits

    run.finished_at = datetime.now(UTC)
    if run.pages_failed == 0:
        run.status = "success"
    elif run.records_fetched > 0:
        run.status = "partial_failure"
    else:
        run.status = "failed"

    return all_records, run


def write_raw_zone(
    payload: list[dict[str, Any]] | dict[str, Any],
    run: IngestionResult,
    raw_zone_root: str
    ) -> Path:
    """
    Write the raw, untouched response to a partitioned landing zone:

        {root}/endpoint={name}/ingestion_date={date}/query={query}/data.jsonl
        {root}/endpoint={name}/ingestion_date={date}/query={query}/_run_metadata.json

    The raw zone is append-only — never overwritten in place.
    """
    # Sanitise query for use as a directory name
    safe_query = run.query.replace(" ", "_").replace("/", "-")[:80]
    
    partition_dir = (
        Path(raw_zone_root)
        / f"endpoint={run.endpoint}"
        / f"ingestion_date={run.ingestion_date.isoformat()}"
        / f"query={safe_query}"
    )
    partition_dir.mkdir(parents=True, exist_ok=True)

    # Newline-delimited JSON for lists (Spark reads these natively)
    # Single JSON object for non-list responses (salary estimates etc.)
    data_path = partition_dir / "data.jsonl"
    with data_path.open("w") as f:
        if isinstance(payload, list):
            for record in payload:
                f.write(json.dumps(record) + "\n")
        else:
            json.dump(payload, f)

    metadata_path = partition_dir / "_run_metadata.json"
    with metadata_path.open("w") as f:
        json.dump(run.to_dict(), f, indent=2)

    logger.info(
        "Wrote endpoint=%s query=%s → %s (status=%s, records=%d)",
        run.endpoint, run.query, data_path, run.status, run.records_fetched,
    )
    return data_path

# Orchestration entry points

def ingest_endpoint(
    endpoint_name: str,
    api_key: str,
    raw_zone_root: str,
    call_params: dict[str, Any] | None = None,
    ) -> IngestionResult:
    """
    Entry point for the orchestrator. One call per endpoint per query.

    Raises JSearchAPIError only on complete failure (zero usable records) —
    a partial failure still writes what it got and returns a result with
    status='partial_failure', letting the DAG decide whether to retry.
    """
    logger.info(
        "Starting JSearch ingestion: endpoint=%s params=%s",
        endpoint_name, call_params,
    )
    payload, run = fetch_endpoint_data(
        endpoint_name=endpoint_name,
        api_key=api_key,
        call_params=call_params,
    )
    write_raw_zone(payload, run, raw_zone_root)
    if run.status == "failed":
        raise JsearchAPIError(
            f"Ingestion failed completely for endpoint={endpoint_name} "
            f"query={run.query}: {run.error_messages}"
        )

    return run



DAILY_QUERIES = [
    # Kenya / East Africa — primary market
    "data engineer jobs in Nairobi Kenya",
    "software engineer jobs in Nairobi Kenya",
    "data analyst jobs in Nairobi Kenya",
    "machine learning engineer jobs in Nairobi Kenya",

    # Remote roles accessible from Kenya
    "remote data engineer jobs Africa",
    "remote software engineer jobs Africa",

    # Benchmark markets for salary comparison
    "data engineer jobs in United Kingdom",
    "data engineer jobs in United States",
]

WEEKLY_SALARY_BENCHMARKS = [
    # Kenya benchmarks — what the local market pays
    {"job_title": "Data Engineer",             "location": "Nairobi, Kenya"},
    {"job_title": "Software Engineer",         "location": "Nairobi, Kenya"},
    {"job_title": "Data Analyst",              "location": "Nairobi, Kenya"},

    # Global benchmarks — for comparison in salary marts
    {"job_title": "Data Engineer",             "location": "London, UK"},
    {"job_title": "Data Engineer",             "location": "New York, US"},
    {"job_title": "Machine Learning Engineer", "location": "London, UK"},
]


def run_daily_ingestion(
    api_key: str,
    raw_zone_root: str,
) -> list[IngestionResult]:
    """
    Daily cadence: job postings. One ingest call per query.
    Free tier = 200 req/month, so 7 queries/day uses ~210/month —
    trim DAILY_QUERIES if staying strictly on free tier.
    """
    results = []
    for query in DAILY_QUERIES:
        result = ingest_endpoint(
            endpoint_name="job_search",
            api_key=api_key,
            raw_zone_root=raw_zone_root,
            call_params={"query": query},
        )
        results.append(result)
    return results


def run_weekly_reference_ingestion(
    api_key: str,
    raw_zone_root: str,
) -> list[IngestionResult]:
    """
    Weekly cadence: salary benchmarks. These feed bridge_job_salary
    and mart_salary_by_role as reference data, not as job posting facts.
    Salary data changes slowly — daily would waste request budget.
    """
    results = []
    for params in WEEKLY_SALARY_BENCHMARKS:
        result = ingest_endpoint(
            endpoint_name="salary_estimate",
            api_key=api_key,
            raw_zone_root=raw_zone_root,
            call_params=params,
        )
        results.append(result)
    return results


if __name__ == "__main__":
    import os
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    api_key       = os.environ["JSEARCH_API_KEY"]
    raw_zone_root = os.environ.get("JSEARCH_RAW_ZONE_ROOT", "/data/raw/jsearch")
    mode          = sys.argv[1] if len(sys.argv) > 1 else "daily"

    if mode == "daily":
        results = run_daily_ingestion(api_key, raw_zone_root)
    elif mode == "weekly":
        results = run_weekly_reference_ingestion(api_key, raw_zone_root)
    else:
        raise ValueError(f"Unknown mode '{mode}', expected 'daily' or 'weekly'")

    for r in results:
        print(json.dumps(r.to_dict(), indent=2))