-- Крок 10: gold.fact_repo_activity_daily. Специфікація: ../../SPEC.md → «Крок 10».
-- Грануляція: (repo_id, date_id). Багатоджерельний rollup з {{ ref('commits') }},
-- {{ ref('pull_requests') }}, {{ ref('issues') }} та {{ ref('events') }} (WatchEvent/ForkEvent).
-- Патерн: денний агрегат на джерело (метрика + нулі для решти) → union all → group by.
-- Відсутні метрики → 0, не NULL. Порядок і типи колонок у всіх CTE мають збігатися.
-- Колонки: activity_id (md5(concat_ws('|', repo_id, date_id))), repo_id, date_id, commits,
--          distinct_committers, prs_opened, prs_merged, issues_opened, issues_closed, stars, forks.

{{ config(
    materialized='table'
) }}

with commit_activity as (

    select
        md5(repo_name) as repo_id,
        cast(date_format(pushed_at, 'yyyyMMdd') as int) as date_id,
        cast(count(*) as bigint) as commits,
        cast(count(distinct author_email) as bigint) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        cast(0 as bigint) as prs_merged,
        cast(0 as bigint) as issues_opened,
        cast(0 as bigint) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks

    from {{ ref('commits') }}

    where pushed_at is not null

    group by
        md5(repo_name),
        cast(date_format(pushed_at, 'yyyyMMdd') as int)

),

pull_request_activity as (

    select
        md5(repo_name) as repo_id,

        cast(
            date_format(opened_at, 'yyyyMMdd')
            as int
        ) as date_id,

        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,

        cast(count(*) as bigint) as prs_opened,

        cast(0 as bigint) as prs_merged,
        cast(0 as bigint) as issues_opened,
        cast(0 as bigint) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks

    from {{ ref('pull_requests') }}

    where opened_at is not null

    group by
        md5(repo_name),
        cast(date_format(opened_at, 'yyyyMMdd') as int)

    union all

    select
        md5(repo_name) as repo_id,

        cast(
            date_format(merged_at, 'yyyyMMdd')
            as int
        ) as date_id,

        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        cast(0 as bigint) as prs_opened,

        cast(count(*) as bigint) as prs_merged,

        cast(0 as bigint) as issues_opened,
        cast(0 as bigint) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks

    from {{ ref('pull_requests') }}

    where merged_at is not null

    group by
        md5(repo_name),
        cast(date_format(merged_at, 'yyyyMMdd') as int)

),

issue_activity as (

    select
        md5(repo_name) as repo_id,

        cast(
            date_format(opened_at, 'yyyyMMdd')
            as int
        ) as date_id,

        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        cast(0 as bigint) as prs_merged,

        cast(count(*) as bigint) as issues_opened,

        cast(0 as bigint) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks

    from {{ ref('issues') }}

    where opened_at is not null

    group by
        md5(repo_name),
        cast(date_format(opened_at, 'yyyyMMdd') as int)

    union all

    select
        md5(repo_name) as repo_id,

        cast(
            date_format(closed_at, 'yyyyMMdd')
            as int
        ) as date_id,

        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        cast(0 as bigint) as prs_merged,
        cast(0 as bigint) as issues_opened,

        cast(count(*) as bigint) as issues_closed,

        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks

    from {{ ref('issues') }}

    where closed_at is not null

    group by
        md5(repo_name),
        cast(date_format(closed_at, 'yyyyMMdd') as int)

),

event_activity as (

    select
        md5(repo_name) as repo_id,

        cast(
            date_format(created_at, 'yyyyMMdd')
            as int
        ) as date_id,

        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        cast(0 as bigint) as prs_merged,
        cast(0 as bigint) as issues_opened,
        cast(0 as bigint) as issues_closed,

        cast(
            sum(
                case
                    when event_type = 'WatchEvent'
                    then 1
                    else 0
                end
            ) as bigint
        ) as stars,

        cast(
            sum(
                case
                    when event_type = 'ForkEvent'
                    then 1
                    else 0
                end
            ) as bigint
        ) as forks

    from {{ ref('events') }}

    where event_type in (
        'WatchEvent',
        'ForkEvent'
    )

    group by
        md5(repo_name),
        cast(date_format(created_at, 'yyyyMMdd') as int)

),

all_activity as (

    select * from commit_activity

    union all

    select * from pull_request_activity

    union all

    select * from issue_activity

    union all

    select * from event_activity

),

daily_activity as (

    select
        repo_id,
        date_id,

        sum(commits) as commits,
        sum(distinct_committers) as distinct_committers,
        sum(prs_opened) as prs_opened,
        sum(prs_merged) as prs_merged,
        sum(issues_opened) as issues_opened,
        sum(issues_closed) as issues_closed,
        sum(stars) as stars,
        sum(forks) as forks

    from all_activity

    group by
        repo_id,
        date_id

)

select
    md5(
        concat_ws(
            '|',
            repo_id,
            cast(date_id as string)
        )
    ) as activity_id,

    repo_id,
    date_id,
    commits,
    distinct_committers,
    prs_opened,
    prs_merged,
    issues_opened,
    issues_closed,
    stars,
    forks

from daily_activity
