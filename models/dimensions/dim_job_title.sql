-- models/dimensions/dim_job_title.sql
-- =============================================================
-- dim_job_title: conformed job title dimension
-- Grain: one row per normalized job title
--
-- Shared across fct_job_postings and fct_salary_estimates.
-- Surrogate key hashes title_normalized (not raw) so that
-- two postings with different raw titles but the same
-- normalized form resolve to the same dimension member.
--
-- Key type: BIGINT — conv() output exceeds INT range
-- =============================================================

{{
    config(
        materialized='table',
        tags=['dimension'],
    )
}}

with normalized_titles as (
    select
        title_normalized,
        title_raw_representative,
        case
            when title_normalized is null
            then cast(-1 as bigint)
            else cast(
                conv(
                    substr(md5(title_normalized), 1, 8),
                    16, 10
                ) as bigint
            )
        end      as job_title_key,                              
        case

    -- ---------------------------------------------------------
    -- Cybersecurity first — prevents "security engineer" from
    -- being swallowed by generic Software Engineering pattern
    -- ---------------------------------------------------------
    when lower(title_normalized) rlike '\\b(cyber security|cybersecurity|information security|infosec|sso|single sign on|data protection engineer|security automation|security architect|security engineer)\\b'
                                                        then 'Cybersecurity'

    -- ---------------------------------------------------------
    -- AI / ML
    -- ---------------------------------------------------------
    when lower(title_normalized) rlike '\\b(machine learning|ml engineer|ai engineer|llm|deep learning|ai trainer|nlp|computer vision)\\b'
                                                        then 'AI/ML'

    -- ---------------------------------------------------------
    -- Data architecture
    -- ---------------------------------------------------------
    when lower(title_normalized) rlike '\\b(data architect|big data architect|enterprise data architect|edw architect|solutions architect data)\\b'
                                                        then 'Data Architecture'

    -- ---------------------------------------------------------
    -- Data engineering
    -- database engineer moved here from Infrastructure —
    -- SQL/Teradata database engineers are data engineering roles
    -- ---------------------------------------------------------
    when lower(title_normalized) rlike '\\b(data engineer|data platform|analytics engineer|etl|elt|data engineering|databricks engineer|data integration|integration engineer|integration lead|database engineer|data pipeline|gis data engineer|gisdata engineer)\\b'
                                                        then 'Data Engineering'

    -- ---------------------------------------------------------
    -- Data science
    -- ---------------------------------------------------------
    when lower(title_normalized) rlike '\\b(data scientist|data science|research scientist|applied scientist)\\b'
                                                        then 'Data Science'

    -- ---------------------------------------------------------
    -- Data & BI
    -- ---------------------------------------------------------
    when lower(title_normalized) rlike '\\b(data analyst|business intelligence|bi engineer|bi developer|power platform|powerbi|power bi|analyst engineer|reporting analyst|insights analyst|grafana analyst)\\b'
                                                        then 'Data & BI'

    -- ---------------------------------------------------------
    -- Software engineering
    -- Covers adjacent tokens (software [x] engineer) and
    -- QA/test/db variants
    -- ---------------------------------------------------------
    when lower(title_normalized) rlike '\\b(software engineer|software developer|swe|sde|backend|frontend|front end|front-end|fullstack|full stack|developer|android|ios|qa engineer|test engineer|design engineer|db development engineer| software engineers)\\b'
                                                        then 'Software Engineering'
    -- Catch "software [anything] engineer" where tokens aren't adjacent
    when lower(title_normalized) rlike '\\bsoftware\\b'
     and lower(title_normalized) rlike '\\bengineer\\b'
                                                        then 'Software Engineering'

    -- ---------------------------------------------------------
    -- Infrastructure
    -- database engineer removed — moved to Data Engineering
    -- ---------------------------------------------------------
    when lower(title_normalized) rlike '\\b(devops|platform engineer|site reliability|sre|cloud engineer|infrastructure engineer|systems engineer)\\b'
                                                        then 'Infrastructure'

    -- ---------------------------------------------------------
    -- Geospatial / domain engineering
    -- Was in dead code block before — now reachable
    -- ---------------------------------------------------------
    when lower(title_normalized) rlike '\\b(geospatial|gis engineer|virtualization engineer)\\b'
                                                        then 'Domain Engineering'

    -- ---------------------------------------------------------
    -- Pre-sales and solutions
    -- ---------------------------------------------------------
    when lower(title_normalized) rlike '\\b(solution engineer|solutions engineer|sales engineer|presales|pre-sales|field engineer|enterprise solutions)\\b'
                                                        then 'Pre-Sales & Solutions'

    -- ---------------------------------------------------------
    -- Product and program
    -- ---------------------------------------------------------
    when lower(title_normalized) rlike '\\b(product manager|product analyst|program manager|project manager|scrum master)\\b'
                                                        then 'Product & Program'

    -- ---------------------------------------------------------
    -- Leadership
    -- svp/evp added, head of moved here from missing
    -- ---------------------------------------------------------
    when lower(title_normalized) rlike '\\b(director|vp |vice president|head of|chief|svp|evp)\\b'
                                                        then 'Leadership'

    -- ---------------------------------------------------------
    -- Engineering management
    -- manager engineering pattern added for inverted titles
    -- ---------------------------------------------------------
    when lower(title_normalized) rlike '\\b(engineering manager|data manager|team lead|tech lead|manager engineering|qa manager|manager engineer|lead engineer|manager software engineering|principal software engineering|lead software engineering)\\b'
                                                        then 'Engineering Management'

    -- ---------------------------------------------------------
    -- Out of scope
    -- Physical/domain engineering that landed via broad
    -- keyword matching — exclude from benchmarking marts
    -- ---------------------------------------------------------
    when lower(title_normalized) rlike '\\b(structural engineer|mechanical engineer|electrical engineer|civil engineer|reservoir engineer|fixed wire|flight software|basing programmer)\\b'
     and lower(title_normalized) not rlike '\\b(data|software|platform|cloud|ml|ai)\\b'
                                                        then 'Out Of Scope'

    else 'Unmapped'

    end                                                     as role_family,


        -- ---------------------------------------------------------
        -- is_technical and is_management via keyword
        -- ---------------------------------------------------------
        case
            when lower(title_normalized) rlike '\\b(manager|director|vp |head of|chief|lead)\\b'
                                                                then true
            else false
        end                                                     as is_management,

        case
            when lower(title_normalized) rlike '\\b(product manager|project manager|program manager|scrum|agile|recruiter|hr |people ops)\\b'
                                                                then false
            else true
        end                                                     as is_technical

    from {{ ref('int_job_titles_normalized') }}
)

select * from normalized_titles