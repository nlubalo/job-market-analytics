-- models/intermediate/int_job_titles_normalized.sql
-- =============================================================
-- Intermediate: clean and deduplicate raw job titles from
-- the JSearch job search staging model.
-- One row per distinct normalized title.
-- =============================================================

{{ config(
    materialized='view',
    tags=['intermediate']
) }}

with raw_titles as (

    select distinct
        title

    from {{ ref('stg_jobs') }}

    where title is not null
      and length(trim(title)) > 2
      and title not rlike '^[A-Z]{2,}-[0-9]+'

),

normalized as (

    select
        title,
        {{ normalize_job_title('title') }}              as title_normalized

    from raw_titles

),

deduplicated as (

    select
        title_normalized,
        first(title)                                    as title_raw_representative

    from normalized
    where title_normalized is not null
      and length(trim(title_normalized)) > 2

    group by title_normalized

)

select * from deduplicated