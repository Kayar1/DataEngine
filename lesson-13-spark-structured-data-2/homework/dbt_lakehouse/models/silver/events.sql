{{ config(materialized='incremental', incremental_strategy='append') }}

-- Крок 1: silver.events. Специфікація: ../../SPEC.md → «Крок 1».
-- Джерело: {{ source('bronze', 'raw_events') }}. payload несемо далі сирим рядком — from_json у кроках 2–4.
-- Колонки: event_id, event_type, actor_login, repo_name, repo_owner, created_at,
--          payload, _ingested_at, _source_file
-- Фільтри: 6 типів подій; public = true (NULL відкинути); event_id/repo_name/created_at не null; дедуп по event_id.
-- incremental (append): у is_incremental()-гілці брати лише рядки з _ingested_at > max(_ingested_at) у {{ this }}.

{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

with source_events as (

    select
        id as event_id,
        type as event_type,
        actor.login as actor_login,
        repo.name as repo_name,
        split(repo.name, '/')[0] as repo_owner,
        to_timestamp(created_at) as created_at,
        payload,
        _ingested_at,
        _source_file,
        public

    from {{ source('bronze', 'raw_events') }}

    where type in (
        'PushEvent',
        'PullRequestEvent',
        'IssuesEvent',
        'IssueCommentEvent',
        'WatchEvent',
        'ForkEvent'
    )
      and public = true
      and id is not null
      and repo.name is not null
      and created_at is not null

      {% if is_incremental() %}
      and _ingested_at > (
          select max(_ingested_at)
          from {{ this }}
      )
      {% endif %}

),

deduplicated as (

    select
        event_id,
        event_type,
        actor_login,
        repo_name,
        repo_owner,
        created_at,
        payload,
        _ingested_at,
        _source_file,

        row_number() over (
            partition by event_id
            order by _ingested_at, _source_file
        ) as rn

    from source_events

)

select
    event_id,
    event_type,
    actor_login,
    repo_name,
    repo_owner,
    created_at,
    payload,
    _ingested_at,
    _source_file

from deduplicated

where rn = 1
