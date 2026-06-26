with categories as (
    select
        category_tag,
        category_label,
        country_code,
        _ingested_at
    from {{ ref('stg_categories') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['category_tag', 'country_code']) }} as category_key,
    category_tag,
    category_label,
    country_code,
    _ingested_at
from categories
