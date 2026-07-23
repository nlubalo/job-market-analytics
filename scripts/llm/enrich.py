from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from pathlib import Path
from datetime import datetime, timezone
from typing import Literal, get_args
from pydantic import BaseModel, ConfigDict, ValidationError

from openai import OpenAI, RateLimitError, APIStatusError, APIConnectionError
from openai.types.responses import ResponseInputParam, EasyInputMessageParam
from scripts.config import _get_secret
from scripts.ingestion.ingest_jobs import (
    list_raw_partitions,
    read_raw_zone_text,
    write_raw_zone_text,
    ensure_raw_zone_dir,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("enrich")

MODEL = "gpt-5.4-mini"
PROMPT_VERSION = "v1"
MAX_TOKENS = 1024
MAX_DESCRIPTION = 8000

# NOTE: no module-level OpenAI() client — created in main() and passed down,
# so importing this module (e.g. in tests) never touches secrets.


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


# Derived lists, kept for coercion fallback
ROLE_FAMILIES = list(get_args(RoleFamily))
SENIORITIES = list(get_args(Seniority))


# Shape is enforced by the API via text_format=JobExtraction; the prompt only
# carries the semantic rules the schema can't express.
SYSTEM_PROMPT = """You are a job posting parser. Extract structured data from the given job title and description.

Rules:
- role_family: pick the single best fit based on actual responsibilities, not just the title. Use "other" only if nothing fits.
- seniority: infer from title AND description (years of experience, scope). "unspecified" if genuinely unclear. Ignore inflated words like "ninja", "rockstar", "guru".
- skills: extract concrete, specific technologies, tools, languages, frameworks, and named methodologies (e.g. "PySpark", "dbt", "Kimball dimensional modeling"). Do NOT include soft skills ("communication", "teamwork") or vague terms ("big data", "cloud"). Preserve the canonical spelling of well-known tools (e.g. "PostgreSQL" not "postgres").
- skills_required vs skills_preferred: if the posting doesn't distinguish, put everything in skills_required.
- confidence: "low" if the description is very short, garbled, or in a language you can't fully parse.
"""


FEW_SHOTS: list[EasyInputMessageParam] = [
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
    """Dedup key: same title+description => same enrichment, no new API call."""
    normalized = f"{title.strip().lower()}\n{description.strip().lower()}"
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def build_message(title: str, description: str) -> list[EasyInputMessageParam]:
    desc = description[:MAX_DESCRIPTION]
    return FEW_SHOTS + [
        {
            "role": "user",
            "content": f"TITLE: {title}\nDESCRIPTION: {desc}",
        }
    ]


def parse_response(job_id: str, chash: str, raw_text: str) -> EnrichmentRecord:
    clean = raw_text.strip().removeprefix("```json").removesuffix("```").strip()

    # Strict path: with text_format enforcement this should almost always succeed.
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
            parse_status="ok",
        )
    except (ValidationError, json.JSONDecodeError):  # FIX: was (JSONDecodeError, AttributeError);
        pass  # pydantic raises ValidationError — fall through to lenient coercion

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

    role = obj.get("role_family")
    if role not in ROLE_FAMILIES:
        role = "other"
    seniority = obj.get("seniority")
    if seniority not in SENIORITIES:
        seniority = "unspecified"

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
    """Explicit sentinel for API-level failures; reason preserved in raw_response
    so you can distinguish retryable API errors from genuine parse failures."""
    return EnrichmentRecord(
        job_id=job_id, content_hash=chash, role_family="other",
        seniority="unspecified", skills_required=[], skills_preferred=[],
        confidence="low", model_version=MODEL, prompt_version=PROMPT_VERSION,
        extracted_at=_now(), raw_response=f"API_ERROR: {reason}", parse_status="failed",
    )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# DEDUP: only call the API for hashes we haven't enriched under this model+prompt version.

def load_seen_hashes(existing_output: Path | str) -> set:
    existing_output = Path(existing_output)
    seen = set()
    if existing_output.exists():
        with existing_output.open() as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    if rec.get("model_version") == MODEL and rec.get("prompt_version") == PROMPT_VERSION:
                        seen.add(rec["content_hash"])  # FIX: was rec["content_has"]
                except (json.JSONDecodeError, KeyError):
                    continue
    return seen


def load_jsearch_raw(raw_root: str, ingestion_date: str | None = None) -> list[dict]:
    """Sweep JSearch raw-zone partitions into the pending-jobs schema.

    Expects layout: {raw_root}/endpoint=job_search/ingestion_date=YYYY-MM-DD/query=*/data.json
    Each data.json is newline-delimited JSON (one job posting dict per line) —
    write_raw_zone() in ingest_jobs.py JSONL-encodes job_search payloads despite
    the .json extension, so this reads line-by-line rather than as one JSON blob.

    Dedups by JSearch job_id across partitions; content-hash dedup happens later
    in pending_jobs().
    """
    jobs, seen_ids = [], set()
    paths = list_raw_partitions(raw_root, endpoint="job_search", ingestion_date=ingestion_date)
    log.info("raw sweep: %d partition files under %s", len(paths), raw_root)
    for path in paths:
        try:
            content = read_raw_zone_text(path)
        except FileNotFoundError:
            log.warning("skipping missing raw file: %s", path)
            continue
        for line_num, line in enumerate(content.splitlines(), 1):
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                log.warning("skipping unparsable line %d in %s", line_num, path)
                continue
            jid = item.get("job_id")
            title, desc = item.get("job_title"), item.get("job_description")
            if not jid or not title or not desc or jid in seen_ids:
                continue  # postings without a description can't be enriched
            seen_ids.add(jid)
            jobs.append({"job_id": jid, "title": title, "description": desc})
    log.info("raw sweep: %d unique postings with descriptions", len(jobs))
    return jobs


def pending_jobs(jobs: list[dict], seen: set) -> list[dict]:
    """Filter to unique-by-hash jobs not yet enriched. Duplicate hashes within the
    input are collapsed to one representative; join back on content_hash in dbt."""
    out, batch_seen = [], set()
    for job in jobs:
        chash = content_hash(job["title"], job["description"])
        if chash in seen or chash in batch_seen:
            continue
        batch_seen.add(chash)
        out.append({**job, "content_hash": chash})
    return out


def enrich_job_sync(client: OpenAI, job: dict, retries: int = 3) -> EnrichmentRecord:
    last_error = "unknown"
    for attempt in range(retries):
        try:
            messages: ResponseInputParam = [
                {
                    "role": "system",
                    "content": SYSTEM_PROMPT,
                },
                *build_message(job["title"], job["description"]),
            ]

            resp = client.responses.parse(
                model=MODEL,
                temperature=0,  # drop this line if the model rejects it (reasoning models 400 on temperature)
                max_output_tokens=MAX_TOKENS,
                input=messages,
                text_format=JobExtraction,
            )
            return parse_response(
                job["job_id"],
                job["content_hash"],
                resp.output_text,
            )
        except APIStatusError as e:
            # 4xx (bad model, bad request, auth, etc.) is a config/request
            # error — it will fail identically on every retry, so don't
            # burn the retry budget on it. Only RateLimitError (429) and
            # 5xx are worth retrying. Mirrors _fetch()'s split in ingest_jobs.py.
            if e.status_code and 400 <= e.status_code < 500 and not isinstance(e, RateLimitError):
                log.error("job %s non-retryable error (%s): %s", job["job_id"], type(e).__name__, e)
                return failed_record(job["job_id"], job["content_hash"], f"{type(e).__name__}: {e}")
            last_error = f"{type(e).__name__}: {e}"
            wait = 2 ** attempt * 5
            log.warning("job %s attempt %d failed (%s): %s — retrying in %ds", job["job_id"], attempt + 1, type(e).__name__, e, wait)
            time.sleep(wait)
        except APIConnectionError as e:
            last_error = f"{type(e).__name__}: {e}"
            wait = 2 ** attempt * 5
            log.warning("job %s attempt %d failed (%s): %s — retrying in %ds", job["job_id"], attempt + 1, type(e).__name__, e, wait)
            time.sleep(wait)
    return failed_record(job["job_id"], job["content_hash"], last_error)


def run_sync(client: OpenAI, jobs: list, output: Path | str, max_workers: int) -> None:
    # FIX: client is now a parameter instead of a module-level global
    output_path = str(output)
    ensure_raw_zone_dir(str(Path(output_path).parent))

    # Unity Catalog Volumes don't support append-mode file I/O — open("a")
    # implicitly seeks to EOF, which raises OSError: [Errno 29] Illegal seek
    # on the Volumes FUSE mount. So results are buffered in memory and the
    # whole file is rewritten (never appended to) on each periodic flush.
    try:
        existing_lines = read_raw_zone_text(output_path).splitlines()
    except FileNotFoundError:
        existing_lines = []

    def _flush(new_lines: list[str]) -> None:
        all_lines = existing_lines + new_lines
        if all_lines:
            write_raw_zone_text(output_path, "\n".join(all_lines) + "\n")

    new_lines: list[str] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(enrich_job_sync, client, j): j for j in jobs}
        for i, fut in enumerate(as_completed(futures), 1):
            rec = fut.result()
            new_lines.append(json.dumps(asdict(rec)))
            if i % 25 == 0:
                _flush(new_lines)
                log.info("enriched %d/%d", i, len(jobs))

    _flush(new_lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--raw-root",
        default=os.environ.get("JSEARCH_RAW_ZONE_ROOT", "/data/raw/jsearch"),
        help="JSearch raw-zone root (default: $JSEARCH_RAW_ZONE_ROOT)",
    )
    ap.add_argument(
        "--ingestion-date",
        help="filter to one ingestion_date (YYYY-MM-DD); default sweeps all dates",
    )
    ap.add_argument("--output", type=Path, required=True, help="JSONL enrichment table (append)")
    ap.add_argument("--max-workers", type=int, default=8)
    ap.add_argument("--limit", type=int, help="cap pending jobs (useful for testing)")
    args = ap.parse_args()

    seen = load_seen_hashes(args.output)
    raw_jobs = load_jsearch_raw(args.raw_root, args.ingestion_date)
    jobs = pending_jobs(raw_jobs, seen)
    if args.limit:
        jobs = jobs[: args.limit]
    log.info("%d unique pending postings (%d hashes already enriched)", len(jobs), len(seen))
    if not jobs:
        return

    client = OpenAI(api_key=_get_secret("OPENAI_API_KEY"))  # FIX: created here, not at import time
    run_sync(client, jobs, args.output, args.max_workers)


if __name__ == "__main__":
    main()