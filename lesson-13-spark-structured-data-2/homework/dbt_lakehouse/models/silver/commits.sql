-- Крок 2: silver.commits. Специфікація: ../../SPEC.md → «Крок 2».
-- Джерело: {{ ref('events') }}, лише PushEvent.
-- from_json(payload, PUSH_SCHEMA) → explode масиву commits → commit grain. PUSH_SCHEMA = var('push_schema').
-- Дедуп: один рядок на commit_sha, найраніший pushed_at.
-- Колонки: commit_sha, repo_name, pushed_by, branch, author_name, author_email, message,
--          is_distinct, pushed_at, is_merge_commit, message_subject, message_length
-- Пастка: `distinct` — reserved word, у DDL-схемі та доступі до поля потрібні backticks.

{{ config(
    materialized='table'
) }}

with parsed_events as (

    select
        event_id,
        repo_name,
        created_at as pushed_at,
        actor_login as pushed_by,
        from_json(
            payload,
            '{{ var("push_schema") }}'
        ) as push_data

    from {{ ref('events') }}

    where event_type = 'PushEvent'

),

exploded_commits as (

    select
        event_id,
        repo_name,
        pushed_at,
        pushed_by,

        regexp_replace(
            push_data.ref,
            '^refs/heads/',
            ''
        ) as branch,

        commit_data.sha as commit_sha,
        commit_data.author.name as author_name,
        commit_data.author.email as author_email,
        commit_data.message as message,
        commit_data.`distinct` as is_distinct

    from parsed_events

    lateral view explode(push_data.commits) exploded as commit_data

),

prepared as (

    select
        commit_sha,
        repo_name,
        pushed_by,
        branch,
        author_name,
        author_email,
        message,
        is_distinct,
        pushed_at,

        instr(message, '\n') > 0 as is_merge_commit,

        case
            when instr(message, '\n') > 0
                then substring(message, 1, instr(message, '\n') - 1)
            else message
        end as message_subject,

        length(message) as message_length,

        event_id

    from exploded_commits

),

deduplicated as (

    select
        commit_sha,
        repo_name,
        pushed_by,
        branch,
        author_name,
        author_email,
        message,
        is_distinct,
        pushed_at,
        is_merge_commit,
        message_subject,
        message_length,

        row_number() over (
            partition by commit_sha
            order by pushed_at, event_id
        ) as rn

    from prepared

)

select
    commit_sha,
    repo_name,
    pushed_by,
    branch,
    author_name,
    author_email,
    message,
    is_distinct,
    pushed_at,
    is_merge_commit,
    message_subject,
    message_length

from deduplicated

where rn = 1