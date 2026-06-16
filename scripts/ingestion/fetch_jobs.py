"""
Ingest job postings from Adzuna → Databricks Delta raw table.

Run on Databricks:
    spark-submit scripts/ingestion/fetch_jobs.py

Run locally (writes parquet to ./data/raw/jobs/ — no Java/Spark needed):
    python -m scripts.ingestion.fetch_jobs --local
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from scripts.config import DEFAULT_COUNTRY, RESULTS_PER_PAGE, MAX_PAGES, DBT_CATALOG
from scripts.ingestion.adzuna_client import get_jobs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _parse_job(job: dict, page: int, country: str, ingested_at: datetime) -> dict:
    location = job.get("location", {})
    area = location.get("area", [])
    category = job.get("category", {})
    return {
        "job_id": job.get("id"),
        "title": job.get("title"),
        "description": job.get("description"),
        "company_display_name": job.get("company", {}).get("display_name"),
        "location_display_name": location.get("display_name"),
        "location_area": ",".join(area) if area else None,
        "category_label": category.get("label"),
        "category_tag": category.get("tag"),
        "contract_type": job.get("contract_type"),
        "contract_time": job.get("contract_time"),
        "salary_min": job.get("salary_min"),
        "salary_max": job.get("salary_max"),
        "salary_is_predicted": bool(int(job["salary_is_predicted"])) if job.get("salary_is_predicted") is not None else None,
        "redirect_url": job.get("redirect_url"),
        "created_at": datetime.fromisoformat(job["created"].replace("Z", "+00:00")) if job.get("created") else None,
        "_ingested_at": ingested_at,
        "_country": country,
        "_source_page": page,
    }


def fetch_all_jobs(country: str = DEFAULT_COUNTRY, max_days_old: int | None = 1) -> list[dict]:
    ingested_at = datetime.now(timezone.utc)
    all_jobs: list[dict] = []

    for page in range(1, MAX_PAGES + 1):
        logger.info("Fetching page %d/%d", page, MAX_PAGES)
        response = get_jobs(
            country=country,
            page=page,
            results_per_page=RESULTS_PER_PAGE,
            max_days_old=max_days_old,
        )
        results = response.get("results", [])
        if not results:
            logger.info("No more results at page %d — stopping", page)
            break
        all_jobs.extend(_parse_job(j, page, country, ingested_at) for j in results)
        logger.info("Total so far: %d jobs", len(all_jobs))

    logger.info("Fetched %d jobs total", len(all_jobs))
    return all_jobs


def write_local(records: list[dict]) -> None:
    """Write to parquet locally using pandas — no Java/Spark needed."""
    import pandas as pd

    df = pd.DataFrame(records)
    df["_ingested_date"] = pd.to_datetime(df["_ingested_at"]).dt.date.astype(str)

    out_dir = Path("data/raw/jobs")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"jobs_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.parquet"
    df.to_parquet(out_path, index=False)
    logger.info("Wrote %d rows → %s", len(df), out_path)


def write_to_databricks(records: list[dict]) -> None:
    """Write to Databricks Delta Lake using PySpark (runs on Databricks)."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        BooleanType, DoubleType, LongType,
        StringType, StructField, StructType, TimestampType,
    )

    schema = StructType([
        StructField("job_id", StringType(), False),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("company_display_name", StringType(), True),
        StructField("location_display_name", StringType(), True),
        StructField("location_area", StringType(), True),
        StructField("category_label", StringType(), True),
        StructField("category_tag", StringType(), True),
        StructField("contract_type", StringType(), True),
        StructField("contract_time", StringType(), True),
        StructField("salary_min", DoubleType(), True),
        StructField("salary_max", DoubleType(), True),
        StructField("salary_is_predicted", BooleanType(), True),
        StructField("redirect_url", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("_ingested_at", TimestampType(), False),
        StructField("_country", StringType(), False),
        StructField("_source_page", LongType(), True),
    ])

    spark = SparkSession.builder.appName("adzuna-jobs-ingestion").getOrCreate()
    df = spark.createDataFrame(records, schema=schema)
    df = df.withColumn("_ingested_date", F.to_date("_ingested_at"))

    table = f"{DBT_CATALOG}.raw.jobs"
    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy("_ingested_date", "_country")
        .option("mergeSchema", "true")
        .saveAsTable(table)
    )
    logger.info("Wrote %d rows → %s (Delta)", df.count(), table)


def main(local: bool = False, max_days_old: int | None = 1) -> None:
    records = fetch_all_jobs(max_days_old=max_days_old)
    if not records:
        logger.info("No records fetched — nothing to write")
        return

    if local:
        write_local(records)
    else:
        write_to_databricks(records)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true", help="Write parquet locally (no Spark needed)")
    parser.add_argument("--max-days-old", type=int, default=1, help="Only fetch jobs posted in the last N days (default: 1)")
    args = parser.parse_args()
    main(local=args.local, max_days_old=args.max_days_old)
