# Databricks notebook source

# COMMAND ----------

# MAGIC %pip install openai pydantic databricks-sdk

# COMMAND ----------

import sys
import os
import json
import logging

sys.path.insert(0, os.path.join(os.getcwd(), ".."))

from openai import OpenAI

from scripts.config import _get_secret
from scripts.llm.enrich import (
    load_jsearch_raw,
    load_seen_hashes,
    pending_jobs,
    enrich_job_sync,
    run_sync,
)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

# COMMAND ----------

# Widgets — keep the test output separate from the real enrichment table so a
# manual run never pollutes production dedup state.
dbutils.widgets.text("raw_root", os.environ.get("JSEARCH_RAW_ZONE_ROOT", "/Volumes/job_market/raw/jsearch"))  # type: ignore[name-defined]
dbutils.widgets.text("ingestion_date", "")  # blank = sweep all dates  # type: ignore[name-defined]
dbutils.widgets.text("output_path", "/Volumes/job_market/raw/llm_enrichment/_manual_test/enrichment.jsonl")  # type: ignore[name-defined]
dbutils.widgets.text("batch_limit", "5")  # type: ignore[name-defined]

raw_root = dbutils.widgets.get("raw_root")  # type: ignore[name-defined]
ingestion_date = dbutils.widgets.get("ingestion_date") or None  # type: ignore[name-defined]
output_path = dbutils.widgets.get("output_path")  # type: ignore[name-defined]
batch_limit = int(dbutils.widgets.get("batch_limit"))  # type: ignore[name-defined]

# COMMAND ----------

# 1. Confirm load_seen_hashes accepts a plain string (widgets always hand you
#    strings, not Path objects — this is the bug that was just fixed).
seen = load_seen_hashes(output_path)
print(f"seen hashes in {output_path}: {len(seen)}")

# COMMAND ----------

# 2. Sweep the raw zone and confirm the JSONL-per-line parsing + cross-partition
#    job_id dedup actually finds postings.
raw_jobs = load_jsearch_raw(raw_root, ingestion_date)
print(f"raw postings with descriptions: {len(raw_jobs)}")
if raw_jobs:
    sample = raw_jobs[0]
    print("sample:", sample["job_id"], sample["title"], sample["description"][:120])

# COMMAND ----------

# 3. Content-hash dedup against what's already enriched.
jobs = pending_jobs(raw_jobs, seen)
print(f"pending (unseen) postings: {len(jobs)}")
for j in jobs[:batch_limit]:
    print(" -", j["job_id"], j["title"])

# COMMAND ----------

# 4. Single live call — cheapest possible sanity check of MODEL + prompt
#    before spending money on a full batch. Inspect parse_status closely:
#    "ok" means strict schema validation passed; "coerced" means the model
#    drifted from the schema and the lenient fallback kicked in; "failed"
#    means the API call itself errored out after retries.
client = OpenAI(api_key=_get_secret("OPENAI_API_KEY"))

if jobs:
    test_record = enrich_job_sync(client, jobs[0])
    print(json.dumps(test_record.__dict__, indent=2))
else:
    print("no pending jobs to test against — widen ingestion_date or raw_root")

# COMMAND ----------

# 5. Small batch run against the real pipeline (run_sync), capped by
#    batch_limit so a manual test run can't accidentally burn the whole
#    pending queue. Writes/appends to output_path.
batch = jobs[:batch_limit]
if batch:
    run_sync(client, batch, output_path, max_workers=4)
    print(f"enriched {len(batch)} postings -> {output_path}")
else:
    print("nothing to run")

# COMMAND ----------

# 6. Inspect what actually landed.
result_text = dbutils.fs.head(output_path, 65536)  # type: ignore[name-defined]
for line in result_text.strip().splitlines()[-batch_limit:]:
    rec = json.loads(line)
    print(rec["job_id"], "|", rec["parse_status"], "|", rec["role_family"], "|", rec["seniority"])
