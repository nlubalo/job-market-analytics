"""Thin HTTP client for the Adzuna Jobs API."""

import time
import logging
from typing import Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from scripts.config import ADZUNA_APP_ID, ADZUNA_APP_KEY, ADZUNA_BASE_URL

logger = logging.getLogger(__name__)

_RETRY_STRATEGY = Retry(
    total=3,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
)


def _session() -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=_RETRY_STRATEGY)
    session.mount("https://", adapter)
    return session


def _base_params() -> dict[str, str]:
    return {"app_id": ADZUNA_APP_ID, "app_key": ADZUNA_APP_KEY}


def get_jobs(
    country: str = "gb",
    page: int = 1,
    results_per_page: int = 50,
    what: str | None = None,
    where: str | None = None,
    category: str | None = None,
    sort_by: str = "date",
    full_time: bool | None = None,
    permanent: bool | None = None,
) -> dict[str, Any]:
    """Fetch a page of job postings."""
    params: dict[str, Any] = {
        **_base_params(),
        "results_per_page": results_per_page,
        "content-type": "application/json",
        "sort_by": sort_by,
    }
    if what:
        params["what"] = what
    if where:
        params["where"] = where
    if category:
        params["category"] = category
    if full_time is not None:
        params["full_time"] = 1 if full_time else 0
    if permanent is not None:
        params["permanent"] = 1 if permanent else 0

    url = f"{ADZUNA_BASE_URL}/jobs/{country}/search/{page}"
    return _get(url, params)


def get_categories(country: str = "gb") -> dict[str, Any]:
    url = f"{ADZUNA_BASE_URL}/jobs/{country}/categories"
    return _get(url, _base_params())


def get_salary_histogram(
    country: str = "gb",
    what: str | None = None,
    location: str | None = None,
) -> dict[str, Any]:
    params = {**_base_params(), "content-type": "application/json"}
    if what:
        params["what"] = what
    if location:
        params["location0"] = location
    url = f"{ADZUNA_BASE_URL}/jobs/{country}/histogram"
    return _get(url, params)


def get_salary_history(
    country: str = "gb",
    what: str | None = None,
    location: str | None = None,
) -> dict[str, Any]:
    """Monthly average salary trend."""
    params = {**_base_params(), "content-type": "application/json"}
    if what:
        params["what"] = what
    if location:
        params["location0"] = location
    url = f"{ADZUNA_BASE_URL}/jobs/{country}/history"
    return _get(url, params)


def get_geodata(country: str = "gb", what: str | None = None) -> dict[str, Any]:
    params = {**_base_params(), "content-type": "application/json"}
    if what:
        params["what"] = what
    url = f"{ADZUNA_BASE_URL}/jobs/{country}/geodata"
    return _get(url, params)


def _get(url: str, params: dict[str, Any]) -> dict[str, Any]:
    session = _session()
    logger.debug("GET %s", url)
    response = session.get(url, params=params, timeout=30)
    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", 60))
        logger.warning("Rate limited — sleeping %ds", retry_after)
        time.sleep(retry_after)
        response = session.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()
