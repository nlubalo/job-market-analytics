-- models/marts/mart_salary_trends.sql
-- =============================================================
-- Mart: salary trends over time
-- Grain: one row per (title_normalized, market, salary_currency,
--         ingestion_date)

-- Tracks how salary estimates move over time for each
-- (title, location) combination across Kenya, UK, and US.
-- Useful for answering:
--   - Are Data Engineer salaries in Nairobi trending up?
--   - How has the UK/US salary gap for Senior DE roles changed?
--   - Which roles saw the biggest salary movement month-on-month?
--
--
-- Unlike mart_job_market_benchmarks which takes only the most
-- recent estimate, this mart retains all historical snapshots
-- from fct_salary_estimates to enable time-series analysis.
--
-- Upstream dependencies:
--   fct_salary_estimates  → all salary snapshots over time
--   dim_job_title         → role family, seniority
--   dim_location          → market, country, benchmark flag
--   dim_date              → calendar attributes for time grouping
--
-- Filters:
--   dl.is_benchmark_market = true    Kenya, UK, US only
--   dt.is_out_of_scope = false       exclude domain engineering
--   record_quality in (high, medium) exclude low quality estimates
-- =============================================================

with salary_snapshots as(
    -- All historical salary estimate snapshots — no recency filter.
    -- Every ingestion date is a data point in the trend line.
    -- Low quality estimates excluded to avoid noisy trend signals.
    select
        s.salary_estimate_key,
        s.job_title_key,
        s.location_key,
        s.date_key,
        s.salary_currency,
        s.salary_median_annual,
        s.salary_min_annual,
        s.salary_max_annual,
        s.base_salary_median_annual,
        s.additional_pay_median_annual,
        s.additional_pay_pct,
        s.salary_bucket,
        s.base_salary_bucket,
        s.salary_range_width,
        s.salary_range_pct,
        s.estimate_reliability,
        s.salary_count,
        s.ingestion_date
    from {{ ref('fct_salary_estimates') }} as s
),
-- =============================================================
-- Period-over-period calculations
-- Computes month-on-month and vs-first-observation changes
-- using window functions over the ingestion date sequence.
-- Partitioned by (title, location, currency) so comparisons
-- are always within the same role/market/currency combination.
-- =============================================================

trends as (
    select
        *,
        -- Previous snapshot for this (title, location, currency)
        -- Used for period-on-period change calculations

        lag(salary_median_annual) over (
            partition by job_title_key, location_key, salary_currency
            order by ingestion_date
        ) as prev_salary_median_annual,

        lag(base_salary_median_annual) over (
            partition by job_title_key, location_key, salary_currency
            order by ingestion_date
        ) as prev_base_salary_median_annual,

        lag(ingestion_date) over (
            partition by job_title_key, location_key, salary_currency
            order by ingestion_date
        ) as prev_ingestion_date,

        -- First observed salary for this (title, location, currency)
        -- Used for cumulative change since observation started

        first_value(salary_median_annual) over (
                partition by job_title_key, location_key, salary_currency
                order by ingestion_date
                rows between unbounded preceding and unbounded following
            )                                               as first_salary_median_annual,

        first_value(ingestion_date) over (
                partition by job_title_key, location_key, salary_currency
                order by ingestion_date
                rows between unbounded preceding and unbounded following
            )                                               as first_observation_date,

        -- Snapshot sequence number — useful for filtering to
        -- "at least N observations" in downstream analysis
        row_number() over (
            partition by job_title_key, location_key, salary_currency
            order by ingestion_date
        )                                               as snapshot_number,

        -- Total snapshots available for this (title, location, currency)
        count(*) over (
            partition by job_title_key, location_key, salary_currency
        )                                               as total_snapshots

    from salary_snapshots

),

enriched as (
    select
        t.*,
        -- ---------------------------------------------------------
        -- Period-on-period change (vs previous snapshot)
        -- Null on first observation — no previous to compare to
        -- ---------------------------------------------------------
        case
            when prev_salary_median_annual is not null
            then salary_median_annual - prev_salary_median_annual
            else null
        end as median_change_vs_prev,

        case
            when prev_salary_median_annual is not null
            and prev_salary_median_annual > 0
            then
                round(
                    (
                        (salary_median_annual - prev_salary_median_annual) / prev_salary_median_annual
                    ) * 100,
                    2

                )
            else null
        end as median_pct_change_vs_prev,

        -- Absolute change in base salary median
        case
            when prev_base_salary_median_annual is not null
            then base_salary_median_annual - prev_base_salary_median_annual
            else null
        end                                             as base_median_change_vs_prev,

        -- Days since previous snapshot — tells you whether
        -- this is a daily, weekly, or irregular observation
        case
            when prev_ingestion_date is not null
            then datediff(ingestion_date, prev_ingestion_date)
            else null
        end                                             as days_since_prev_snapshot,

    
    -- ---------------------------------------------------------
    -- Cumulative change (vs first observation)
    -- Shows overall salary movement since tracking began
    -- ---------------------------------------------------------

    -- Absolute change since first observation

    case
        when first_salary_median_annual is not null and snapshot_number >0
        then salary_median_annual - first_salary_median_annual
        else null
    end as median_change_vs_first,

    -- Percentage change since first observation
    case
        when first_salary_median_annual is not null
            and first_salary_median_annual > 0
            and snapshot_number > 1
        then round(
            (
                (salary_median_annual - first_salary_median_annual)
                / first_salary_median_annual
            ) * 100,
            2
        )
        else null
    end                                             as median_pct_change_vs_first,

    -- Days since first observation - x-axis for trend charts

    datediff(
        ingestion_date,
        first_observation_date
    ) as days_since_first_observation,

    -- ---------------------------------------------------------
    -- Trend direction signal
    -- Simple categorical label for filtering/colouring
    -- in dashboards without requiring consumers to interpret
    -- raw percentage change values
    -- ---------------------------------------------------------

    case
        when prev_salary_median_annual is null then 'first observation'
        when salary_median_annual > prev_salary_median_annual * 1.02 then 'increasing'
        when salary_median_annual < prev_salary_median_annual * 0.98      then 'decreasing'
        else                                             'stable'
    end                                             as trend_direction

    from trends t

)
select

    -- ---------------------------------------------------------
    -- Role attributes
    -- ---------------------------------------------------------
    dt.title_normalized,
    dt.role_family,
    dt.is_management,
    dt.is_technical,

    -- ---------------------------------------------------------
    -- Market attributes
    -- ---------------------------------------------------------
    dl.market,
    dl.country_clean,
    dl.city_clean,

    -- ---------------------------------------------------------
    -- Time attributes
    -- Calendar columns from dim_date for grouping/filtering
    -- e.g. aggregate to monthly average for smoother trend lines
    -- ---------------------------------------------------------
    e.ingestion_date,
    d.year,
    d.month_num,
    d.month_name_short,
    d.year_month,
    d.quarter_num,
    d.year_quarter,

    -- ---------------------------------------------------------
    -- Salary snapshot values
    -- ---------------------------------------------------------
    e.salary_currency,
    e.salary_median_annual,
    e.salary_min_annual,
    e.salary_max_annual,
    e.base_salary_median_annual,
    e.additional_pay_median_annual,
    e.additional_pay_pct,
    e.salary_bucket,
    e.base_salary_bucket,
    e.salary_range_width,
    e.salary_range_pct,

    -- ---------------------------------------------------------
    -- Period-on-period change metrics
    -- ---------------------------------------------------------
    e.prev_ingestion_date,
    e.days_since_prev_snapshot,
    e.median_change_vs_prev,
    e.median_pct_change_vs_prev,
    e.base_median_change_vs_prev,


    -- ---------------------------------------------------------
    -- Cumulative change metrics
    -- ---------------------------------------------------------
    e.first_observation_date,
    e.first_salary_median_annual,
    e.days_since_first_observation,
    e.median_change_vs_first,
    e.median_pct_change_vs_first,

    -- ---------------------------------------------------------
    -- Trend signals
    -- ---------------------------------------------------------
    e.trend_direction,
    e.snapshot_number,
    e.total_snapshots,
    e.salary_count                                      as estimate_sample_size

from enriched e

join {{ ref('dim_job_title') }} dt
    on  e.job_title_key  = dt.job_title_key
    and dt.role_family not in ("Out Of Scope", "Unmapped")
    and dt.job_title_key != -1

join {{ ref('dim_location') }} dl
    on  e.location_key          = dl.location_key

join {{ ref('dim_date') }} d
    on e.date_key = d.date_key

order by
    dl.market,
    dt.role_family,
    dt.title_normalized,
    e.ingestion_date


