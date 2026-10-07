{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='driver_key'
) }}

with new_driver_events as (

    select
        payload -> 'driver' ->> 'id' as driver_key
    from {{ ref('events') }}
    where event_type = 'ride_accepted'
      and payload -> 'driver' ->> 'id' is not null
      {% if is_incremental() %}
      and _ingested_at > {{ high_watermark('_ingested_at') }}
      {% endif %}
    group by payload -> 'driver' ->> 'id'

),

driver_history as (

    select
        e.event_id,
        e.occurred_at,
        e._ingested_at,
        e.payload -> 'driver' ->> 'id' as driver_key,
        e.payload -> 'driver' ->> 'rating' as rating,
        e.payload -> 'driver' -> 'vehicle' ->> 'type' as vehicle_type,
        e.payload -> 'driver' -> 'vehicle' ->> 'medallion' as medallion
    from {{ ref('events') }} e
    inner join new_driver_events d
        on e.payload -> 'driver' ->> 'id' = d.driver_key
    where e.event_type = 'ride_accepted'

),

ranked_history as (

    select
        *,
        row_number() over (
            partition by driver_key
            order by occurred_at desc, event_id desc
        ) as rn
    from driver_history

),

drivers as (

    select
        driver_key,
        vehicle_type,
        medallion,
        rating::numeric as latest_rating,
        first_seen_at,
        last_seen_at,
        max_ingested_at as _ingested_at
    from (
        select
            driver_key,
            vehicle_type,
            medallion,
            rating,
            rn,
            min(occurred_at) over (partition by driver_key) as first_seen_at,
            max(occurred_at) over (partition by driver_key) as last_seen_at,
            max(_ingested_at) over (partition by driver_key) as max_ingested_at
        from ranked_history
    ) h
    where rn = 1

),

unknown_driver as (

    select
        'unknown'::text as driver_key,
        'unknown'::text as vehicle_type,
        'unknown'::text as medallion,
        null::numeric as latest_rating,
        null::timestamptz as first_seen_at,
        null::timestamptz as last_seen_at,
        null::timestamptz as _ingested_at

    {% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }}
        where driver_key = 'unknown'
    )
    {% endif %}

)

select
    driver_key,
    vehicle_type,
    medallion,
    latest_rating,
    first_seen_at,
    last_seen_at,
    _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from drivers

union all

select
    driver_key,
    vehicle_type,
    medallion,
    latest_rating,
    first_seen_at,
    last_seen_at,
    _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from unknown_driver