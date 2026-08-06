{{ config(materialized='view') }}
with events as (

    select
        id::varchar as id,
        event_type::varchar as event_type,
        created_at::timestamptz as created_at,

        event_date::date as event_date,


        actor_login::varchar as actor_login,
        repo_name::varchar as repo_name,

        coalesce(payload_commit_count, 0)::bigint
            as payload_commit_count,

        payload_action::varchar as payload_action,
        payload_ref::varchar as payload_ref

    from read_parquet(        
        '{{ var("events_path") }}',
        hive_partitioning = true
    )

)

select
    id,
    event_type,
    created_at,
    event_date,
    actor_login,
    repo_name,
    payload_commit_count,
    payload_action,
    payload_ref

from events

where event_type in (
    'PushEvent',
    'IssuesEvent',
    'PullRequestEvent',
    'WatchEvent',
    'IssueCommentEvent'
)

and actor_login not like '%[bot]'

and not (
    event_type = 'PushEvent'
    and payload_commit_count = 0
)