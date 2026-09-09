-- Крок 3: silver.pull_requests. Специфікація: ../../SPEC.md → «Крок 3».
-- Джерело: {{ ref('events') }}, лише PullRequestEvent. from_json(payload, PR_SCHEMA), PR_SCHEMA = var('pr_schema').
-- Грануляція: один рядок на (repo_name, pr_number) — стан з ОСТАННЬОЇ за часом події (row_number desc).
-- Колонки: repo_name, pr_number, title, author_login, state, is_merged, is_draft, opened_at,
--          closed_at, merged_at, additions, deletions, changed_files, commits_count, comments,
--          review_comments, author_association, label_names, last_action, last_event_at, churn, hours_open

{{ config(
    materialized='table'
) }}

with parsed_events as (

    select
        event_id,
        repo_name,
        created_at as event_at,

        from_json(
            payload,
            '{{ var("pr_schema") }}'
        ) as pr_data

    from {{ ref('events') }}

    where event_type = 'PullRequestEvent'

),

prepared as (

    select
        event_id,
        repo_name,
        event_at,

        pr_data.number as pr_number,
        pr_data.pull_request.title as title,
        pr_data.pull_request.user.login as author_login,
        pr_data.pull_request.state as state,
        pr_data.pull_request.merged as is_merged,
        pr_data.pull_request.draft as is_draft,

        to_timestamp(
            pr_data.pull_request.created_at
        ) as opened_at,

        to_timestamp(
            pr_data.pull_request.closed_at
        ) as closed_at,

        to_timestamp(
            pr_data.pull_request.merged_at
        ) as merged_at,

        pr_data.pull_request.additions as additions,
        pr_data.pull_request.deletions as deletions,
        pr_data.pull_request.changed_files as changed_files,
        pr_data.pull_request.commits as commits_count,
        pr_data.pull_request.comments as comments,
        pr_data.pull_request.review_comments as review_comments,
        pr_data.pull_request.author_association as author_association,

        transform(
            pr_data.pull_request.labels,
            x -> x.name
        ) as label_names,

        pr_data.action as last_action

    from parsed_events

),

ranked as (

    select
        *,
        row_number() over (
            partition by repo_name, pr_number
            order by event_at desc, event_id desc
        ) as rn

    from prepared

),

latest as (

    select
        repo_name,
        pr_number,
        title,
        author_login,
        state,
        is_merged,
        is_draft,
        opened_at,
        closed_at,
        merged_at,
        additions,
        deletions,
        changed_files,
        commits_count,
        comments,
        review_comments,
        author_association,
        label_names,
        last_action,
        event_at as last_event_at

    from ranked

    where rn = 1

)

select
    repo_name,
    pr_number,
    title,
    author_login,
    state,
    is_merged,
    is_draft,
    opened_at,
    closed_at,
    merged_at,
    additions,
    deletions,
    changed_files,
    commits_count,
    comments,
    review_comments,
    author_association,
    label_names,
    last_action,
    last_event_at,

    coalesce(additions, 0) + coalesce(deletions, 0) as churn,

    cast(
        (
            unix_timestamp(
                coalesce(closed_at, last_event_at)
            )
            - unix_timestamp(opened_at)
        ) / 3600.0
        as double
    ) as hours_open

from latest