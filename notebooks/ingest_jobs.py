# Databricks notebook source

# COMMAND ----------

# MAGIC %pip install tenacity requests

# COMMAND ----------

import sys
import os

# Add repo root to path so the scripts package is importable
sys.path.insert(0, os.path.join(os.getcwd(), ".."))

from scripts.ingestion.ingest_jobs import (
    run_daily_ingestion,
    run_weekly_reference_ingestion,
)

# COMMAND ----------

try:
    app_id  = dbutils.secrets.get("adzuna", "app_id")   # type: ignore[name-defined]
    app_key = dbutils.secrets.get("adzuna", "app_key")  # type: ignore[name-defined]
except NameError:
    # Fallback for local runs outside Databricks
    app_id  = os.environ["ADZUNA_APP_ID"]
    app_key = os.environ["ADZUNA_APP_KEY"]

raw_zone_root = os.environ.get("ADZUNA_RAW_ZONE_ROOT", "/Volumes/job_market/raw/adzuna")
country       = os.environ.get("ADZUNA_COUNTRY", "gb")

# COMMAND ----------

# Daily — job postings (run every day)
#results = run_daily_ingestion(country, app_id, app_key, raw_zone_root)
#for r in results:
#    print(r.to_dict())

# COMMAND ----------

# Weekly — categories + geodata (run once a week)
results = run_weekly_reference_ingestion(country, app_id, app_key, raw_zone_root)
for r in results:
     print(r.to_dict())
