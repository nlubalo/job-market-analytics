-- models/marts/mart_company_hiring.sql
-- Grain: one row per (company, market, role_family)

select
    c.company_name_clean,
    c.employer_type,
    c.hq_market,
    c.posting_volume_band,
    c.is_staffing_agency,
    c.profile_completeness,
    dl.market,
    dl.country_clean,
    dt.role_family,
    dt.seniority_level,

    count(f.job_id_source)              as total_postings,
    count(distinct dt.role_family)      as distinct_role_families,

    -- Is this company hiring across multiple markets?
    count(distinct dl.market)           as markets_hiring_in,

    -- Seniority mix — what level does this company hire most?
    max(case when dt.seniority_rank = (
        select max(dt2.seniority_rank)
        from job_market.dev.fct_job_postings f2
        join job_market.dev.dim_job_title dt2
            on f2.job_title_key = dt2.job_title_key
        where f2.company_key = f.company_key
    ) then dt.seniority_level end)      as most_senior_level_hired,

    min(f.first_seen_at)                as first_posting_seen,
    max(f.last_seen_at)                 as last_posting_seen

from {{ ref('fct_job_postings') }} f

join {{ ref('dim_company') }} c
    on  f.company_key           = c.company_key
    and c.company_key          != '-1'
    and c.is_staffing_agency    = false

join {{ ref('dim_job_title') }} dt
    on  f.job_title_key         = dt.job_title_key
    and dt.is_out_of_scope      = false
    and dt.job_title_key       != -1
    and dt.role_family not in ('Out Of Scope', 'Unmapped')

join {{ ref('dim_location') }} dl
    on  f.location_key          = dl.location_key
    and dl.is_benchmark_market  = true

where f.record_quality in ('high', 'medium')

group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

order by total_postings desc, c.company_name_clean