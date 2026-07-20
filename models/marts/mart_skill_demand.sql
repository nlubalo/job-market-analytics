-- models/marts/mart_skill_demand.sql
-- =============================================================
-- Mart: skill demand by role and market
-- Grain: one row per (skill_name, role_family, market)

-- Aggregates fct_job_posting_skills to surface which skills
-- are most frequently required, how demand differs across
-- Kenya, UK, and US, and how skill requirements vary by
-- seniority level.
--
-- Useful for answering:
--   - What % of Senior Data Engineer postings in Kenya require dbt?
--   - How does cloud platform demand differ between UK and US?
--   - Which skills are growing fastest in Nairobi tech roles?
--   - Do Kenyan postings require more skills per role than US ones?
--
--
-- Upstream dependencies:
--   fct_job_posting_skills  → skill flags at posting level
--   fct_job_postings        → total posting counts for rates
--   dim_job_title           → role family, seniority
--   dim_location            → market, country, benchmark flag

-- Filters:
--   dl.is_benchmark_market = true    Kenya, UK, US only
--   dt.is_out_of_scope = false       exclude domain engineering
--   record_quality = 'high'/'medium' on both facts

with skill_postings as (
    select
        sk.job_id,
        sk.skill_name,
        sk.skill_category,
        sk.skill_count,
        dt.role_family,
        dt.is_management,
        dt.is_technical,
        dl.market,
        dl.country_clean,
        dl.city_clean

    from {{ ref('fct_job_posting_skills') }} sk

    join {{ ref('dim_job_title') }} dt
        on sk.job_title_key = dt.job_title_key
        and  dt.role_family not in ("Out Of Scope", "Unmapped")
        and dt.job_title_key   != -1

    join {{ ref('dim_location') }} dl
        on sk.location_key = dl.location_key

),
-- Total postings per (role_family, seniority, market) —
-- denominator for skill frequency rate calculation.
-- Must come from fct_job_postings (not the skills bridge)
-- to include postings that matched zero skills — excluding
-- them would inflate skill rates artificially.

total_postings as (
    select
        dt.role_family,
        dl.market,
        count(distinct f.job_id) as total_postings,

        -- Average skill count per posting in this segment —
        -- proxy for role complexity / requirement breadth
        round(avg(sk.skill_count), 1)                   as avg_skills_per_posting
    
    from {{ ref('fct_job_postings') }} f

    join {{ ref('dim_job_title') }} dt
        on  f.job_title_key     = dt.job_title_key
        and dt.job_title_key   != -1
        and dt.role_family not in ("Out Of Scope", "Unmapped")

    join {{ ref('dim_location') }} dl
        on  f.location_key         = dl.location_key
        and dl.is_benchmark_market = true

    -- Join skills for avg_skills_per_posting calculation
    -- Left join so postings with zero skills still count
    -- in the denominator
    left join {{ ref('fct_job_posting_skills') }} sk
        on f.job_id = sk.job_id

    group by 1, 2

),
-- Aggregate skills: count distinct postings requiring each skill
-- per (skill, role_family, seniority, market) combination
skill_aggregated as (

    select
        skill_name,
        skill_category,
        role_family,
        market,
        country_clean,

        -- Number of postings requiring this skill
        count(distinct job_id)                   as postings_requiring_skill

    from skill_postings
    group by 1, 2, 3, 4, 5

),

-- Join aggregated skills to total postings for rate calculation
with_rates as (

    select
        s.skill_name,
        s.skill_category,
        s.role_family,
        --s.seniority_level,
        --s.seniority_rank,
        s.market,
        s.country_clean,
        s.postings_requiring_skill,
        t.total_postings,
        t.avg_skills_per_posting,

        -- Skill frequency rate: % of postings in this segment
        -- that require this skill.
        -- This is the primary benchmarking metric —
        -- "dbt required in 42% of UK Data Engineer postings
        --  vs 18% of Kenya Data Engineer postings"
        round(
            s.postings_requiring_skill * 100.0
            / nullif(t.total_postings, 0),
            1
        )                                               as skill_frequency_pct,

        -- Rank of this skill within its (role_family, market)
        -- segment by frequency — used for "top N skills" views
        -- without requiring consumers to write window functions
        row_number() over (
            partition by s.role_family, s.market
            order by s.postings_requiring_skill desc
        )                                               as skill_rank_in_segment
    from skill_aggregated s
    left join total_postings t
        on  s.role_family    = t.role_family
        and s.market         = t.market

)
select
    skill_name,
    skill_category,

    role_family,
    market,
    country_clean,
    postings_requiring_skill,

    total_postings,

    -- Primary metric: how commonly is this skill required
    -- in this role/market combination
    skill_frequency_pct,

    -- Rank within (role_family, market) — for top-N filtering
    skill_rank_in_segment,

    -- Average number of skills required per posting in this segment
    -- Higher = more demanding / broader requirements
    avg_skills_per_posting,

    -- ---------------------------------------------------------
    -- Skill prominence label
    -- Categorical signal for dashboard colour coding
    -- without requiring consumers to threshold on pct values
    -- ---------------------------------------------------------
    case
        when skill_frequency_pct >= 60                  then 'core'
        when skill_frequency_pct >= 30                  then 'common'
        when skill_frequency_pct >= 10                  then 'emerging'
        else                                                 'niche'
    end                                                 as skill_prominence,

    round(
        skill_frequency_pct / nullif(
            avg(skill_frequency_pct) over (
                partition by skill_name, role_family
            ),
            0
        ),
        2
    )                                                   as market_demand_index

from with_rates
order by
    role_family,
    market