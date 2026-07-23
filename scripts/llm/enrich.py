from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from pathlib import Path
from datetime import datetime, timezone
from typing import Literal, get_args
from pydantic import BaseModel, ConfigDict, ValidationError

from openai import OpenAI, RateLimitError, APIStatusError, APIConnectionError
from openai.types.responses import EasyInputMessageParam, ResponseInputItemParam
from scripts.config import _get_secret

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("enrich")

MODEL = ""
PROMPT_VERSION = "v1"
MAX_TOKENS = 1024
MAX_DESCRIPTION = 8000

OPENAI_API_KEY = _get_secret("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)



RoleFamily = Literal[
    "data_engineer", "analytics_engineer", "data_analyst", "data_scientist",
    "ml_engineer", "ai_engineer", "platform_engineer", "software_engineer",
    "bi_developer", "data_architect", "database_administrator",
    "engineering_manager", "data_product_manager", "other",
]

Seniority = Literal["intern", "junior", "mid", "senior", "staff", "lead", "manager", "director", "unspecified"]

class JobExtraction(BaseModel):
    model_config = ConfigDict(extra="forbid")
    
    role_family: RoleFamily
    seniority: Seniority
    skills_required: list[str]
    skills_preferred: list[str]
    confidence: Literal["high", "medium", "low"]


OUTPUT_SCHEMA = JobExtraction.model_json_schema()
OUTPUT_CONFIG = {"format": {"type": "json_schema", "schema": OUTPUT_SCHEMA}}

# Derived lists, kept for coercion fallback and prompt text
ROLE_FAMILIES = list(get_args(RoleFamily))
SENIORITIES = list(get_args(Seniority))


SYSTEM_PROMPT = f"""You are a job posting parser. Given a job title and description, return ONLY a JSON object
(no markdown fences, no prose) with this exact schema:

Rules:
- role_family: pick the single best fit based on actual responsibilities, not just the title. Use "other" only if nothing fits
- seniority: infer from title AND description (years of experience, scope). "unspecified" if genuinely unclear. Ignore inflated words like "ninja", "rockstar", "guru".
- skills:  extract concrete, specific technologies, tools, languages, frameworks, and named methodologies (e.g. "PySpark", "dbt", "Kimball dimensional modeling"). Do NOT include soft skills ("communication", "teamwork") or vague terms ("big data", "cloud"). Preserve the canonical spelling of well-known tools (e.g. "PostgreSQL" not "postgres").
- skills_required vs skills_preferred: if the posting doesn't distinguish, put everything in skills_required.
- confidence: "low" if the description is very short, garbled, or in a language you can't fully parse.
"""


FEW_SHOTS: list[ResponseInputItemParam] = [
    {
        "role": "user",
        "content": 'TITLE: Senior Data Engineer II\nDESCRIPTION: We need 5+ years building pipelines with Apache Spark (Python), Airflow and dbt on Databricks. SQL mastery required. Nice to have: Kafka, Terraform. Strong communication skills essential.',
    },
    {
        "role": "assistant",
        "content": '{"role_family": "data_engineer", "seniority": "senior", "skills_required": ["PySpark", "Apache Airflow", "dbt", "Databricks", "SQL"], "skills_preferred": ["Apache Kafka", "Terraform"], "confidence": "high"}',
    },
    {
        "role": "user",
        "content": "TITLE: Data Ninja (Analyst/Engineer)\nDESCRIPTION: Fast-growing Nairobi startup seeks a data ninja to own reporting in Power BI and build ELT into BigQuery using Python. 1-2 years experience.",
    },
    {
        "role": "assistant",
        "content": '{"role_family": "data_analyst", "seniority": "junior", "skills_required": ["Power BI", "BigQuery", "Python", "ELT"], "skills_preferred": [], "confidence": "medium"}',
    },

]

@dataclass
class EnrichmentRecord:
    job_id: str
    content_hash: str
    role_family: str
    seniority: str
    skills_required: list
    skills_preferred: list
    confidence: str
    model_version: str
    prompt_version: str
    extracted_at: str
    raw_response: str
    parse_status: str  # "ok" | "coerced" | "failed"


def content_hash(title: str, description: str) -> str:
    """Dedup key: same title+description => same enrichment, no new API call"""
    normalized = f"{title.strip().lower()}\n{description.strip().lower()}"
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def build_message(title: str, description: str) -> list[ResponseInputItemParam]:
    desc = description[:MAX_DESCRIPTION]
    return FEW_SHOTS + [{"role": "user", "content": f"TITLE: {title}\nDESCRIPTION: {desc}"}]


def parse_response(job_id: str, chash: str, raw_text: str) -> EnrichmentRecord:
    status = "ok"
    clean = raw_text.strip().removeprefix("```json").removesuffix("```").strip()
    try:
        ext = JobExtraction.model_validate_json(clean)
        return EnrichmentRecord(
            job_id=job_id,
            content_hash=chash,
            role_family=ext.role_family,
            seniority=ext.seniority,
            skills_required=[s.strip() for s in ext.skills_required if s.strip()],
            skills_preferred=[s.strip() for s in ext.skills_preferred if s.strip()],
            confidence=ext.confidence,
            model_version=MODEL,
            prompt_version=PROMPT_VERSION,
            extracted_at=_now(),
            raw_response=raw_text,
            parse_status="ok"
        )
    except (json.JSONDecodeError, ValidationError, AttributeError):
        pass

    status = "coerced"  # anything down here failed strict validation
    try:
        obj = json.loads(clean)
    except json.JSONDecodeError:
        return EnrichmentRecord(
            job_id=job_id, content_hash=chash, role_family="other",
            seniority="unspecified", skills_required=[], skills_preferred=[],
            confidence="low", model_version=MODEL, prompt_version=PROMPT_VERSION,
            extracted_at=_now(), raw_response=raw_text, parse_status="failed",
        )
    role = obj.get('role_family')
    if role not in ROLE_FAMILIES:
        role, status = 'other', 'coerced'
    seniority = obj.get('seniority')
    if seniority not in SENIORITIES:
        seniority, status = "unspecified", "coerced"

    def _str_list(key):
        v = obj.get(key, [])
        return [s.strip() for s in v if isinstance(s, str) and s.strip()] if isinstance(v, list) else []
    return EnrichmentRecord(
        job_id=job_id, content_hash=chash, role_family=role, seniority=seniority,
        skills_required=_str_list("skills_required"),
        skills_preferred=_str_list("skills_preferred"),
        confidence=obj.get("confidence", "medium"),
        model_version=MODEL, prompt_version=PROMPT_VERSION,
        extracted_at=_now(), raw_response=raw_text, parse_status=status,
    )

def failed_record(job_id: str, chash: str, reason: str) -> EnrichmentRecord:
    return EnrichmentRecord(
        job_id=job_id, content_hash=chash, role_family="other",
        seniority="unspecified", skills_required=[], skills_preferred=[],
        confidence="low", model_version=MODEL, prompt_version=PROMPT_VERSION,
        extracted_at=_now(), raw_response=f"API_ERROR: {reason}", parse_status="failed",
    )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

# DEDUP: only cal the API for hashed we haven't enriched

def load_seen_hashes(existing_output: Path) -> set:
    seen = set()
    if existing_output.exists():
        with existing_output.open() as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    if rec.get('model_version') == MODEL and rec.get("prompt_version") == PROMPT_VERSION:
                        seen.add(rec["content_hash"])
                except json.JSONDecodeError:
                    continue
    return seen


def pending_jobs(input_path: Path, seen: set) -> list:
    """Yield unique-by-hash jobs not yet enriched. Duplicate hashes within the
    input are collapsed to one representative; join back on content_hash in dbt."""
    out, batch_seen = [], set()
    with input_path.open() as f:
        for line in f:
            job = json.loads(line)
            chash = content_hash(job["title"], job["description"])
            if chash in seen or chash in batch_seen:
                continue
            batch_seen.add(chash)
            job["content_hash"] = chash
            out.append(job)
    return out

def enrich_job_sync(client: OpenAI, job: dict, retries: int =3) -> EnrichmentRecord:
    last_error = "unknown"
    for attempt in range(retries):
        try:
            resp = client.responses.parse(
                model=MODEL,
                temperature=0,
                max_output_tokens=MAX_TOKENS,
                input=[
                    EasyInputMessageParam(role="system", content=SYSTEM_PROMPT)
                ] + build_message(job["title"], job["description"]),
                text_format=JobExtraction,
            )
            return parse_response(
                job['job_id'],
                job['content_hash'],
                resp.output_text
            )
        except (RateLimitError, APIStatusError, APIConnectionError) as e:
            last_error = f"{type(e).__name__}: {e}"
            wait = 2 ** attempt * 5
            log.warning("job %s attempt %d failed (%s), retrying in %ds", job["job_id"], attempt + 1, type(e).__name__, wait)
            time.sleep(wait)
    return failed_record(job['job_id'], job['content_hash'], last_error)



def run_sync(jobs: list, output: Path, max_workers: int) -> None:
    with output.open('a') as f, ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(enrich_job_sync, client, j): j for j in jobs}
        for i, fut in enumerate(as_completed(futures), 1):
            rec = fut.result()
            f.write(json.dumps(asdict(rec)) + "\n")
            if i % 25 == 0:
                f.flush()
                log.info("enriched %d/%d", i, len(jobs))