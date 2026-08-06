{{ config(materialized='view') }}

with repo_counts as (

    select
        event_type,
        repo_name,
        count(*)::bigint as event_count

    from {{ ref('stg_events') }}

    group by
        event_type,
        repo_name

)

select
    event_type,
    repo_name,
    event_count,

    row_number() over (
        partition by event_type
        order by event_count desc, repo_name
    )::bigint as type_rank

from repo_counts

qualify type_rank <= 5