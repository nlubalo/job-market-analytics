with source as (
    select * from {{ source('raw', 'salary_history') }}
),

deduped as (
    select *
    from source
    qualify row_number() over (
        partition by role, month, _country
        order by _ingested_at desc
    ) = 1
)

select
    lower(trim(role))   as role,
    month,              -- "YYYY-MM"
    avg_salary,
    _country            as country_code,
    _ingested_at
from deduped
where avg_salary > 0
