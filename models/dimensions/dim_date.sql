-- dim_date: static date dimesnions covering 2020-01-01 to 2030-12-31
{{
    config(
        materialized='table',
        tags=['dimensions', 'static']
    )
}}
with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    )}}
),
enriched as (
   select
    cast(date_format(date_day, 'yyyyMMdd') as int)   as date_key,

    date_day as full_date,
    year(date_day) as year,
    quarter(date_day) as quarter_num,
    concat(cast(year(date_day) as string), '-Q', cast(quarter(date_day) as string)) as year_quarter,
    month(date_day) as month_num,
    date_format(date_day, 'MMMM') as month_name,
    date_format(date_day, 'MMM') as month_name_short,

    date_format(date_day, 'yyyy-MM') as year_month,
    weekofyear(date_day) as week_of_year,
    concat(
            cast(year(date_day) as string),
            '-W',
            lpad(cast(weekofyear(date_day) as string), 2, '0')
        )                                                as year_week,
    dayofmonth(date_day)                             as day_of_month,

    -- ISO day of week: Monday=1, Sunday=7
    -- Databricks dayofweek() is Sunday=1, so we adjust
    case dayofweek(date_day)
        when 1 then 7   -- Sunday
        else dayofweek(date_day) - 1
    end                                              as day_of_week_num,

    date_format(date_day, 'EEEE')                   as day_of_week_name,
    date_format(date_day, 'EEE')                    as day_of_week_name_short,

    -- is_weekend: Saturday (6) or Sunday (7) in ISO numbering
    case
        when dayofweek(date_day) in (1,7) then false
        else true
    end as is_weekday,

    -- First and last day of month — useful for month-boundary logic
    -- in incremental models
    case
        when date_day = last_day(date_day) then true
        else false
    end as is_last_day_of_month,

    case
        when dayofmonth(date_day) = 1 then true
        else false
    end as is_first_day_of_month

from date_spine
)
select * from enriched
order by full_date