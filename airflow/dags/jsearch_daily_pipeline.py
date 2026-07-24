"""
Daily JSearch pipeline: ingest job postings, then enrich them via OpenAI.

No pipeline logic lives here — this only sequences and schedules
scripts/ingestion/ingest_jobs.py and scripts/llm/enrich.py, which are
bind-mounted into the container at /opt/project/scripts (see
docker-compose.yaml). Runs off Databricks, so the raw-zone helpers those
scripts use fall back to the Databricks Files API for Volume access
instead of the dbutils/FUSE path they'd use on a cluster.
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
    dag_id="jsearch_daily_pipeline",
    schedule="0 3 * * *",
    # Static and kept close to "now" on purpose — Airflow docs advise against
    # a dynamic start_date (e.g. datetime.now()) since it's re-evaluated on
    # every DAG file parse. A start_date far in the past is what caused a
    # small backlog burst the first time this DAG was unpaused, since
    # catchup=False only stops a *full* replay back to start_date, not a
    # handful of recently-missed intervals. Bump this forward occasionally
    # if the DAG stays paused for a long stretch.
    start_date=datetime(2026, 7, 23),
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "enrichment", "jsearch"],
)
def jsearch_daily_pipeline():

    @task
    def ingest_daily() -> None:
        from scripts.ingestion.ingest_jobs import run_daily_ingestion

        api_key = os.environ["JSEARCH_API_KEY"]
        raw_zone_root = os.environ["JSEARCH_RAW_ZONE_ROOT"]

        results = run_daily_ingestion(api_key, raw_zone_root)
        for r in results:
            logger.info(
                "endpoint=%s query=%s status=%s records=%d",
                r.endpoint, r.query, r.status, r.records_fetched,
            )
        failed = [r for r in results if r.status == "failed"]
        if failed:
            raise RuntimeError(
                f"{len(failed)} of {len(results)} queries failed completely: "
                f"{[r.query for r in failed]}"
            )

    @task
    def enrich_daily(ds: str | None = None) -> None:
        # ds = Airflow's logical run date (YYYY-MM-DD), auto-injected by
        # the TaskFlow API — matches the ingestion_date partition
        # ingest_daily() just wrote, since JSearch's job_search endpoint
        # is queried with date_posted="today" at run time.
        from openai import OpenAI
        from scripts.llm.enrich import run_enrichment_batch

        raw_root = os.environ["JSEARCH_RAW_ZONE_ROOT"]
        client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
        # NOTE: dedup is scoped per (ingestion_date, query) output partition
        # — a posting that reappears under a different date or a different
        # query with the same content_hash won't be caught here, so it can
        # get re-enriched (and re-paid for). See run_enrichment_batch's
        # docstring in scripts/llm/enrich.py.
        run_enrichment_batch(client, raw_root, ingestion_date=ds, max_workers=8)

    ingest_daily() >> enrich_daily()  # type: ignore[operator]  -- apache-airflow only installed in the Docker image, not locally


jsearch_daily_pipeline()
