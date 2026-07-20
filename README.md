# Job Market Analytics Platform — Technical Documentation

## Overview

A dbt data pipeline built on Databricks/SQL that ingests job posting
and salary data from the JSearch API to power a benchmarking platform
comparing the data and tech job market across Kenya, the UK, and the US.

The pipeline follows a medallion architecture:
raw → staging → intermediate → dimensions + facts → marts

---

## Data Sources

### JSearch API (via RapidAPI)

Two endpoints are consumed:

**Job Search endpoint** — returns individual job postings with title,
company, location, employment type, description, and occasionally
an embedded salary range. Ingested into `jsearch.job_search_raw`.

**Estimated Salary endpoint** — returns aggregated salary statistics
(min, median, max) for a given job title and location combination.
Includes base salary, total compensation, and additional pay splits.
Ingested into `jsearch.salary_raw`.


---

## Repository Structure

job_market_analytics/
├── models/
│   ├── staging/
│   │   ├── stg_jsearch_job_search.sql
│   │   └── stg_jsearch_salary.sql
│   ├── intermediate/
│   │   ├── int_jobs_enriched.sql
│   │   ├── int_job_titles_normalized.sql
│   │   ├── int_salary_estimates.sql
│   │   └── int_company_duplicates.sql
│   ├── dimensions/
│   │   ├── dim_date.sql
│   │   ├── dim_job_title.sql
│   │   ├── dim_location.sql
│   │   └── dim_company.sql
│   ├── facts/
│   │   ├── fct_job_postings.sql
│   │   ├── fct_job_posting_skills.sql
│   │   └── fct_salary_estimates.sql
│   └── marts/
│       ├── mart_job_market_benchmarks.sql
│       ├── mart_salary_trends.sql
│       └── mart_skill_demand.sql
├── macros/
│   ├── normalize_job_title.sql
│   └── derive_seniority.sql



---

## Model Inventory

### Total: 16 files

| Layer | Model | Materialization | Grain |
|---|---|---|---|
| Staging | stg_jsearch_job_search | view | one row per raw API record |
| Staging | stg_jsearch_salary | view | one row per raw API record |
| Intermediate | int_jobs_enriched | incremental (merge) | one row per unique job posting |
| Intermediate | int_job_titles_normalized | view | one row per distinct normalized title |
| Intermediate | int_salary_estimates | incremental (merge) | one row per (title, location, currency, date) |
| Intermediate | int_company_duplicates | table | one row per probable duplicate company pair |
| Dimension | dim_date | table | one row per calendar day |
| Dimension | dim_job_title | table | one row per normalized job title |
| Dimension | dim_location | table | one row per (city, country) combination |
| Dimension | dim_company | table | one row per normalized company name |
| Fact | fct_job_postings | incremental (merge) | one row per unique job posting |
| Fact | fct_job_posting_skills | table | one row per (job posting, skill) pair |
| Fact | fct_salary_estimates | incremental (merge) | one row per (title, location, currency, date) |
| Mart | mart_job_market_benchmarks | — | role × market salary comparison |
| Mart | mart_salary_trends | — | salary movement over time |
| Mart | mart_skill_demand | — | skill frequency by role and market |


---

## DAG (Dependency Order)
jsearch.job_search_raw          jsearch.salary_raw
│                               │
stg_jsearch_job_search          stg_jsearch_salary
│                               │
├───────────────────────────────┤
│                               │
int_jobs_enriched          int_salary_estimates
int_job_titles_normalized          │
│                            │
├────────────────────────────┤
│                            │
dim_job_title ◄── job_title_family_map (seed)
dim_location
dim_company ──► int_company_duplicates
dim_date
│
├────────────────────────────┐
│                            │
fct_job_postings            fct_salary_estimates
fct_job_posting_skills
│                            │
└────────────────────────────┘
│
mart_job_market_benchmarks
mart_salary_trends
mart_skill_demand


---

## Macros

### `normalize_job_title(column_name)`

Normalizes a raw job title string through 17 sequential passes.
Called in `int_jobs_enriched`, `int_job_titles_normalized`, and
`int_salary_estimates` to ensure title normalization is identical
across all models — a requirement for the `job_title_key` FK to
be conformed.

**Pass sequence:**

| Pass | Operation | Example |
|---|---|---|
| 1 | Replace punctuation with spaces | `architect/engineer` → `architect engineer` |
| 2 | Split fused compound words | `architectengineer` → `architect engineer` |
| 3 | Strip roman numeral suffixes | `Engineer III` → `Engineer` |
| 4 | Strip seniority suffix abbreviations | `Data Engineer Ssr` → `Data Engineer` |
| 5 | Strip trailing numeric req IDs (4+ digits) | `Programmer 61096` → `Programmer` |
| 6 | Strip trailing alphanumeric req IDs | `Architect Is002` → `Architect` |
| 7 | Strip trailing country/region codes | `Engineer Uk` → `Engineer` |
| 8 | Strip contract/work type noise | `Engineer Remote Contract` → `Engineer` |
| 9 | Strip clearance/compliance noise | `Engineer Public Trust` → `Engineer` |
| 10 | Strip trailing qualification descriptions | `Engineer Strong Sql...` → `Engineer` |
| 11 | Strip trailing org/function descriptors | `Head Of Engineering Productivity` → `Head Of Engineering` |
| 12 | Strip known product/platform names | `Lead Engineer Epic Bridges` → `Lead Engineer` |
| 13 | Strip location noise patterns | `Engineer Job At X In Y` → `Engineer` |
| 14 | Strip dash-suffix noise | `Engineer - Metamask` → `Engineer` |
| 15 | Expand seniority abbreviations | `sr` → `senior` |
| 16 | Expand role abbreviations | `swe` → `software engineer` |
| 17 | Collapse multiple spaces | — |

**Key design decision:** Pass 1 replaces punctuation with spaces
rather than stripping it. This prevents adjacent words from fusing
when punctuation is removed — `architect/engineer` becomes
`architect engineer` not `architectengineer`.


### `derive_seniority_level(column_name)`

Derives a seniority label from a normalized title string using
keyword matching.

| Output | Keywords matched |
|---|---|
| Intern | intern, graduate, trainee |
| Junior | junior, associate, level i |
| Mid | (no seniority keyword present) |
| Senior | senior |
| Lead | lead |
| Staff | staff |
| Principal | principal |
| Director+ | director, head of, vp, vice president |
| C-Suite | chief, cto, cdo |

### `derive_seniority_rank(column_name)`

Returns an integer rank (1–9) corresponding to the seniority level.
Used for correct ordering in BI tools — prevents alphabetic sort
placing Junior before Senior.
