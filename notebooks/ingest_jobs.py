# Databricks notebook source

# COMMAND ----------

# MAGIC %pip install tenacity requests

# COMMAND ----------

import sys
import os
import json
import logging

sys.path.insert(0, os.path.join(os.getcwd(), ".."))

from scripts.ingestion.ingest_jobs import (
    run_daily_ingestion,
    run_weekly_reference_ingestion,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

# COMMAND ----------

try:
    api_key = dbutils.secrets.get("job-market", "JSEARCH_API_KEY")  # type: ignore[name-defined]
except NameError:
    api_key = os.environ["JSEARCH_API_KEY"]

raw_zone_root = os.environ.get("JSEARCH_RAW_ZONE_ROOT", "/Volumes/job_market/raw/jsearch")

# COMMAND ----------

dbutils.widgets.dropdown("mode", "daily", ["daily", "weekly"])  # type: ignore[name-defined]
mode = dbutils.widgets.get("mode")  # type: ignore[name-defined]

# COMMAND ----------

if mode == "daily":
    results = run_daily_ingestion(api_key, raw_zone_root)
elif mode == "weekly":
    results = run_weekly_reference_ingestion(api_key, raw_zone_root)
else:
    raise ValueError(f"Unknown mode '{mode}', expected 'daily' or 'weekly'")

for r in results:
    print(json.dumps(r.to_dict(), indent=2))
