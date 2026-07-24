"""
Weekly JSearch salary-benchmark ingestion.

No enrichment step here: the salary_estimate endpoint returns aggregate
compensation stats, not job descriptions — there's no free text for the
LLM enrichment step to parse.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

default_args = {
    "owner": "job-market-analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="jsearch_weekly_salary_ingestion",
    schedule="0 3 * * 1",
    # See jsearch_daily_pipeline.py for why this is static and kept close to
    # "now" rather than far in the past or dynamically computed.
    start_date=datetime(2026, 7, 23),
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "jsearch"],
)
def jsearch_weekly_salary_ingestion():

    @task
    def ingest_weekly() -> None:
        from scripts.ingestion.ingest_jobs import run_weekly_reference_ingestion

        api_key = os.environ["JSEARCH_API_KEY"]
        raw_zone_root = os.environ["JSEARCH_RAW_ZONE_ROOT"]

        results = run_weekly_reference_ingestion(api_key, raw_zone_root)
        for r in results:
            logger.info(
                "endpoint=%s query=%s status=%s records=%d",
                r.endpoint, r.query, r.status, r.records_fetched,
            )

    ingest_weekly()


jsearch_weekly_salary_ingestion()
