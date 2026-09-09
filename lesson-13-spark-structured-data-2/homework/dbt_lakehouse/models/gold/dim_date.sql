-- Крок 7: gold.dim_date. Специфікація: ../../SPEC.md → «Крок 7».
-- Згенерований безперервний календар (БЕЗ seed): explode(sequence(min, max, interval 1 day)).
-- Межі min/max — підзапитом по фактичних датах з {{ ref('commits') }}, {{ ref('pull_requests') }},
-- {{ ref('issues') }} (pushed_at / opened_at / merged_at / closed_at). Не хардкодьте.
-- Колонки: date_id (int yyyyMMdd), date_day (date), day_of_week, is_weekend, iso_week, year.

{{ config(
    materialized='table'
) }}

with all_dates as (

    select cast(pushed_at as date) as date_day
    from {{ ref('commits') }}
    where pushed_at is not null

    union all

    select cast(opened_at as date) as date_day
    from {{ ref('pull_requests') }}
    where opened_at is not null

    union all

    select cast(merged_at as date) as date_day
    from {{ ref('pull_requests') }}
    where merged_at is not null

    union all

    select cast(closed_at as date) as date_day
    from {{ ref('pull_requests') }}
    where closed_at is not null

    union all

    select cast(opened_at as date) as date_day
    from {{ ref('issues') }}
    where opened_at is not null

    union all

    select cast(closed_at as date) as date_day
    from {{ ref('issues') }}
    where closed_at is not null

),

date_bounds as (

    select
        min(date_day) as min_date,
        max(date_day) as max_date

    from all_dates

),

calendar as (

    select
        explode(
            sequence(
                min_date,
                max_date,
                interval 1 day
            )
        ) as date_day

    from date_bounds

)

select
    cast(
        date_format(date_day, 'yyyyMMdd')
        as int
    ) as date_id,

    date_day,

    dayofweek(date_day) as day_of_week,

    dayofweek(date_day) in (1, 7) as is_weekend,

    weekofyear(date_day) as iso_week,

    year(date_day) as year

from calendar
