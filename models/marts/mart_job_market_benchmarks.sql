-- models/marts/mart_job_market_benchmarks.sql
-- =============================================================
-- Mart: job market benchmarks
-- Grain: one row per (title_normalized, market)

--
-- Primary analytical output for the Kenya / UK / US
-- benchmarking platform. Combines job posting volume from
-- fct_job_postings with salary estimates from
-- fct_salary_estimates to produce a unified view of
-- role demand and compensation by market.

-- Key design decisions:
--  1. Salary figures come exclusively from fct_salary_estimates.
--    Disclosed salaries on job postings are not used here —
--    disclosure rates in Kenya are too low to be analytically
--    meaningful

-- 2. Most recent salary estimate per (title, location, currency)
--    is selected via recency_rank = 1. Only high/medium quality
--    estimates are considered — low quality estimates (sparse
--    sample, wide range, low confidence) are excluded to avoid
--    misleading salary figures in the benchmarking output.

-- 3. record_quality filter applied to both facts independently.
--    Postings with missing location or unknown title are excluded
--    from posting counts so volume figures reflect clean,
--    classifiable postings only.


-- Upstream dependencies:
--   fct_job_postings        → posting volume and disclosure rate
--   fct_salary_estimates    → all salary figures
--   dim_job_title           → role family, seniority, scope flag
--   dim_location            → market, country, city, benchmark flag
--
-- Filters applied:
--   dt.is_out_of_scope = false   exclude physical/domain engineering
--   dt.job_title_key != -1       exclude unknown titles
--   dl.is_benchmark_market = true Kenya, UK, US only
--   record_quality in (high, medium) on both facts
--   at least one of postings or salary must exist per row
-- =============================================================


with latest_estimates as (
    select
        job_title_key,
        location_key,
        salary_currency,
        salary_median_annual,
        salary_min_annual,
        salary_max_annual,
        base_salary_median_annual,
        additional_pay_pct,
        salary_bucket,
        base_salary_bucket,
        estimate_reliability,
        salary_count,
        row_number() over (
            partition by job_title_key, location_key, salary_currency
            order by date_key desc
        ) as recency
    from {{ ref('fct_salary_estimates') }}
    --where record_quality in ('high', 'medium')

),
salary as (
    select * from latest_estimates
    where recency =1
),

postings as (
    -- Aggregate posting counts per (title, location).
    -- salary_disclosure_rate_pct surfaces how transparent each
    -- market is about compensation — a meaningful benchmarking
    -- signal in its own right, particularly for Kenya where
    -- disclosure rates are materially lower than UK and US.
    select
        job_title_key,
        location_key,
        count(*) as total_postings,

        -- Count of postings that included any salary information
        count(case when has_salary_disclosed then 1 end) as postings_with_salary,
        -- Disclosure rate as a percentage
        round(
            count(case when has_salary_disclosed then 1 end) * 100/ count(*), 1
        ) as salary_disclosure_rate_pct
    
    from {{ ref('fct_job_postings') }}
    --where record_quality in ('high', 'medium')
    group by 1,2 
)
select
    -- ---------------------------------------------------------
    -- Role attributes (from dim_job_title)
    -- ---------------------------------------------------------
    dt.title_normalized,
    dt.role_family,
    dt.is_management,
    dt.is_technical,

    -- ---------------------------------------------------------
    -- Market attributes (from dim_location)
    -- ---------------------------------------------------------

    dl.market,
    dl.country_clean,
    dl.city_clean,
    dl.is_benchmark_market,

    -- ---------------------------------------------------------
    -- Posting volume metrics
    -- COALESCE to 0 so roles with salary estimates but no
    -- postings still appear with clean zero counts rather
    -- than nulls that break SUM() aggregations in BI tools
    -- ---------------------------------------------------------
    coalesce(p.total_postings, 0)                       as total_postings,
    coalesce(p.postings_with_salary, 0)                 as postings_with_salary,

    -- Disclosure rate: null when no postings exist for this
    -- (title, location) so BI tools can distinguish "0 postings"
    -- from "postings exist but none disclosed salary"
    p.salary_disclosure_rate_pct,

    -- ---------------------------------------------------------
    -- Salary figures — from estimates only, never from postings
    --
    -- Both total compensation and base salary are carried so
    -- dashboards can choose which to display:
    --   salary_median_annual     = base + bonus + equity
    --   base_salary_median_annual = base only
    --
    -- Figures are in local currency (salary_currency) —
    -- markets are presented side by side in their own currency
    -- rather than converting to a common currency. This is more
    -- honest and preserves purchasing power context:
    --   "KES 2.4M / £90K / $160K" is more meaningful than
    --   three USD figures that obscure local cost of living.
    -- ---------------------------------------------------------

    s.salary_currency,
    s.salary_median_annual,
    s.salary_min_annual,
    s.salary_max_annual,
    s.base_salary_median_annual,

    s.additional_pay_pct,
    s.salary_bucket,
    s.base_salary_bucket,
    s.estimate_reliability,
    s.salary_count                                      as estimate_sample_size,

    case
        when s.salary_median_annual is not null         then 'estimated'
        else                                                 'unavailable'
    end                                                 as salary_source

from {{ ref('dim_job_title') }} dt
-- Drive from dim_job_title × dim_location (benchmark markets)
-- so all (title, market) combinations are represented regardless
-- of whether postings or salary data exists for that combination.
-- Inner join on is_benchmark_market keeps output to Kenya/UK/US only.
join {{ ref('dim_location') }} dl
    on dl.is_benchmark_market = true

-- Left joins: a title may have postings but no salary estimate,
-- or a salary estimate but no postings — both are valid rows.
left join postings p
    on  p.job_title_key = dt.job_title_key
    and p.location_key  = dl.location_key

left join salary s
    on  s.job_title_key = dt.job_title_key
    and s.location_key  = dl.location_key

where dt.role_family not in ("Out Of Scope", "Unmapped")     -- exclude structural/mechanical/civil engineering
  and dt.job_title_key   != -1        -- exclude postings with unclassifiable titles


-- Require at least one data source to exist for this (title, market) pair.
-- Prevents the dim × dim cross join from producing rows for every
-- title in every market with no supporting data at all.
and (
    p.total_postings          is not null
    or s.salary_median_annual is not null
)
order by
    dl.market,
    dt.role_family,
    dt.title_normalized