-- models/intermediate/int_company_duplicates.sql
-- =============================================================
-- Data quality model: surfaces probable duplicate companies
-- using Levenshtein edit distance and website matching.
--
-- This model is purely informational — it does NOT auto-merge
-- duplicates or feed any dimension. Use it to:
--   1. Understand fragmentation in dim_company
--   2. Interpret "top companies" mart results correctly
--   3. Identify high-value candidates for manual review
--      if alias resolution becomes necessary later
--
-- Run periodically after new data loads, not on every run.
-- Materialise as table so it can be queried without
-- recomputing the expensive self-join each time.
-- =============================================================

{{ config(
    materialized='table',
    tags=['intermediate']
) }}

with companies as (

    select
        company_key,
        company_name_clean,
        company_website,
        hq_market,
        total_postings

    from {{ ref('dim_company') }}

    where company_key    != '-1'
      and company_name_clean != 'Unknown'
      -- Only compare companies with enough postings to matter
      -- Single-posting companies produce too much noise
      and total_postings >= 2

),

-- =============================================================
-- Self-join with blocking to keep the comparison space
-- manageable:
--   Block 1: first character must match
--   Block 2: name length within 6 characters
--   Block 3: levenshtein distance <= 4
-- Only keep one direction per pair (key_a < key_b)
-- =============================================================

candidate_pairs as (

    select

        a.company_key                                       as key_a,
        b.company_key                                       as key_b,
        a.company_name_clean                                as name_a,
        b.company_name_clean                                as name_b,
        a.total_postings                                    as postings_a,
        b.total_postings                                    as postings_b,
        a.company_website                                  as website_a,
        b.company_website                                  as website_b,
        a.hq_market                                         as market_a,
        b.hq_market                                         as market_b,
        --a.profile_completeness                              as completeness_a,
        --b.profile_completeness                              as completeness_b,

        levenshtein(
            a.company_name_clean,
            b.company_name_clean
        )                                                   as edit_distance,

        -- Similarity as % — more intuitive than raw distance
        round(
            (
                1 - (
                    levenshtein(a.company_name_clean, b.company_name_clean)
                    / cast(greatest(
                        length(a.company_name_clean),
                        length(b.company_name_clean)
                    ) as double)
                )
            ) * 100,
            1
        )                                                   as similarity_pct,

        -- Website match: strongest possible signal
        -- Same domain = almost certainly the same company
        case
            when a.company_website is not null
             and b.company_website is not null
             and lower(trim(a.company_website))
               = lower(trim(b.company_website))            then true
            else false
        end                                                 as same_website,

        -- Combined confidence score
        case
            -- Same website trumps everything
            when a.company_website is not null
             and b.company_website is not null
             and lower(trim(a.company_website))
               = lower(trim(b.company_website))            then 'high'
            -- Very close name + same market
            when levenshtein(
                    a.company_name_clean,
                    b.company_name_clean
                 ) <= 2
             and a.hq_market = b.hq_market                  then 'high'
            -- Close name, different or unknown market
            when levenshtein(
                    a.company_name_clean,
                    b.company_name_clean
                 ) <= 2                                      then 'medium'
            -- Moderate distance, same market

            -- Moderate distance, same market
            when levenshtein(
                    a.company_name_clean,
                    b.company_name_clean
                 ) <= 4
             and a.hq_market = b.hq_market                  then 'medium'
            else                                                 'low'
        end                                                 as match_confidence,

        -- Suggested canonical: prefer the name with more
        -- postings, using profile completeness as tiebreaker
    
    -- Suggested canonical: prefer the name with more
        -- postings, using profile completeness as tiebreaker
        case
            when a.total_postings > b.total_postings        then a.company_name_clean
            when b.total_postings > a.total_postings        then b.company_name_clean
            --when a.profile_completeness = 'high'            then a.company_name_clean
            --when b.profile_completeness = 'high'            then b.company_name_clean
            -- Final tiebreaker: longer name is usually more complete
            when length(a.company_name_clean)
               > length(b.company_name_clean)               then a.company_name_clean
            else                                                 b.company_name_clean
        end                                                 as suggested_canonical,

        case
            when a.total_postings > b.total_postings        then b.company_name_clean
            when b.total_postings > a.total_postings        then a.company_name_clean
            --when a.profile_completeness = 'high'            then b.company_name_clean
            --when b.profile_completeness = 'high'            then a.company_name_clean
            when length(a.company_name_clean)
               > length(b.company_name_clean)               then b.company_name_clean
            else                                                 a.company_name_clean
        end                                                 as suggested_alias,
     -- Combined posting count if merged
        a.total_postings + b.total_postings                 as combined_postings

    from companies a
    inner join companies b
        -- Only one direction per pair
        on a.company_key < b.company_key
        -- Block 1: same first character
        and left(lower(a.company_name_clean), 1)
          = left(lower(b.company_name_clean), 1)
        -- Block 2: similar name length
        and abs(
            length(a.company_name_clean)
          - length(b.company_name_clean)
        ) <= 6
        -- Block 3: edit distance threshold
        and levenshtein(
            a.company_name_clean,
            b.company_name_clean
        ) <= 4

)

select
    key_a,
    key_b,
    name_a,
    name_b,
    edit_distance,
    similarity_pct,
    same_website,
    match_confidence,
    suggested_canonical,
    suggested_alias,
    postings_a,
    postings_b,
    combined_postings,
    website_a,
    website_b,
    market_a,
    market_b

from candidate_pairs

-- Surface the most actionable pairs first:
-- high confidence, high combined posting volume
order by
    case match_confidence
        when 'high'   then 1
        when 'medium' then 2
        else               3
    end,
    same_website desc,
    combined_postings desc,
    similarity_pct desc
