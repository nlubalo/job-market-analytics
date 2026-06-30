with company_base as (
    select
        company_name,
        company_website,
        company_logo,
    from {{ ref('stg_jobs') }}
)
select
    company_name,
    {{location_normalization('company_name')}} as company_name
    company_name as raw_name,
    comapany_website,
    company_logo,
    min(ingestion_date)                                     as first_seen,
    max(ingestion_date)                                     as last_seen,
from company_base