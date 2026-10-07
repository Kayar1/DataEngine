-- silver.events
-- Grain: одна подія (event_id)

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='event_id'
) }}

with source_events as (

    select
        event_id,
        event_type,
        ride_id,
        occurred_at,
        source,
        payload,
        _source_file,
        _ingested_at
    from {{ source('bronze', 'raw_events') }}

    where source <> 'loadtest'
      and occurred_at is not null
      and ride_id is not null

      {% if is_incremental() %}
      and _ingested_at > {{ high_watermark('_ingested_at') }}
      {% endif %}

),

deduplicated as (

    select
        event_id,
        event_type,
        ride_id,
        occurred_at,
        source,
        payload,
        _source_file,
        _ingested_at
    from (
        select
            *,
            row_number() over (
                partition by event_id
                order by _ingested_at, _source_file
            ) as rn
        from source_events
    ) ranked
    where rn = 1

)

select
    event_id,
    event_type,
    ride_id,
    occurred_at,
    source,
    {{ utc_date('occurred_at') }} as occurred_date,
    payload::jsonb as payload,
    _source_file,
    _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from deduplicated