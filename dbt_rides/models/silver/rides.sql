-- silver.rides
-- Grain: одна поїздка (ride_id)

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='ride_id'
) }}

with new_events as (

    select
        ride_id
    from {{ ref('events') }}

    {% if is_incremental() %}
    where _ingested_at > {{ high_watermark('_ingested_at') }}
    {% endif %}

    group by ride_id

),

ride_history as (

    select
        e.*
    from {{ ref('events') }} e

    inner join new_events n
        on e.ride_id = n.ride_id

),

requested as (

    select *
    from (
        select
            ride_id,
            payload -> 'rider' ->> 'id' as rider_id,
            payload -> 'rider' ->> 'platform' as rider_platform,
            payload -> 'rider' ->> 'app_version' as app_version,
            payload ->> 'requested_vehicle' as requested_vehicle,
            (payload -> 'pickup' ->> 'zone_id')::int as pickup_zone_id,
            (payload -> 'dropoff' ->> 'zone_id')::int as dropoff_zone_id,
            (payload ->> 'surge_estimate')::numeric as surge_estimate,
            occurred_at as requested_at,
            row_number() over (
                partition by ride_id
                order by occurred_at, event_id
            ) as rn
        from ride_history
        where event_type = 'ride_requested'
    ) x
    where rn = 1

),

accepted as (

    select *
    from (
        select
            ride_id,
            payload -> 'driver' ->> 'id' as driver_id,
            occurred_at as accepted_at,
            row_number() over (
                partition by ride_id
                order by occurred_at desc, event_id desc
            ) as rn
        from ride_history
        where event_type = 'ride_accepted'
    ) x
    where rn = 1

),

started as (

    select *
    from (
        select
            ride_id,
            payload -> 'driver' ->> 'id' as driver_id,
            (payload -> 'pickup' ->> 'zone_id')::int as pickup_zone_id,
            occurred_at as started_at,
            row_number() over (
                partition by ride_id
                order by occurred_at desc, event_id desc
            ) as rn
        from ride_history
        where event_type = 'ride_started'
    ) x
    where rn = 1

),

completed as (

    select *
    from (
        select
            ride_id,
            payload -> 'driver' ->> 'id' as driver_id,
            (payload -> 'dropoff' ->> 'zone_id')::int as dropoff_zone_id,
            (payload ->> 'distance_km')::numeric(8,2) as distance_km,
            (payload -> 'fare' ->> 'amount')::numeric(10,2) as fare_amount,
            (payload -> 'fare' ->> 'surge_multiplier')::numeric(4,2) as surge_multiplier,
            (payload -> 'fare' ->> 'tolls')::numeric(10,2) as tolls_amount,
            (payload -> 'fare' ->> 'tip')::numeric(10,2) as tip_amount,
            (payload -> 'fare' ->> 'total')::numeric(10,2) as total_amount,
            payload -> 'fare' ->> 'currency' as currency,
            occurred_at as completed_at,
            row_number() over (
                partition by ride_id
                order by occurred_at desc, event_id desc
            ) as rn
        from ride_history
        where event_type = 'ride_completed'
    ) x
    where rn = 1

),

cancelled as (

    select *
    from (
        select
            ride_id,
            payload ->> 'cancelled_by' as cancelled_by,
            payload ->> 'reason' as cancel_reason,
            payload ->> 'stage' as cancel_stage,
            occurred_at as cancelled_at,
            row_number() over (
                partition by ride_id
                order by occurred_at desc, event_id desc
            ) as rn
        from ride_history
        where event_type = 'ride_cancelled'
    ) x
    where rn = 1

),

payment as (

    select *
    from (
        select
            ride_id,
            payload -> 'payment' ->> 'method' as payment_method,
            payload -> 'payment' ->> 'psp_reference' as psp_reference,
            (payload -> 'payment' ->> 'amount')::numeric(10,2) as payment_amount,
            occurred_at as paid_at,
            row_number() over (
                partition by ride_id
                order by occurred_at desc, event_id desc
            ) as rn
        from ride_history
        where event_type = 'payment_captured'
    ) x
    where rn = 1

),

rides as (

    select
        r.ride_id,

        r.rider_id,
        r.rider_platform,
        r.app_version,
        r.requested_vehicle,

        coalesce(c.driver_id, s.driver_id, a.driver_id) as driver_id,

        case
            when c.ride_id is not null then 'completed'
            when x.ride_id is not null then 'cancelled'
            when s.ride_id is not null then 'in_progress'
            when a.ride_id is not null then 'accepted'
            else 'requested'
        end as status,

        r.requested_at,
        a.accepted_at,
        s.started_at,
        c.completed_at,
        x.cancelled_at,
        p.paid_at,

        coalesce(s.pickup_zone_id, r.pickup_zone_id) as pickup_zone_id,
        coalesce(c.dropoff_zone_id, r.dropoff_zone_id) as dropoff_zone_id,

        extract(epoch from (a.accepted_at - r.requested_at))::int
            as wait_seconds,

        extract(epoch from (c.completed_at - s.started_at))::int
            as trip_seconds,

        r.surge_estimate,

        c.distance_km,
        c.surge_multiplier,
        c.fare_amount,
        c.tolls_amount,
        c.tip_amount,
        c.total_amount,
        c.currency,

        x.cancelled_by,
        x.cancel_reason,
        x.cancel_stage,

        p.payment_method,
        p.psp_reference,
        p.payment_amount,

        (
            select max(e._ingested_at)
            from ride_history e
            where e.ride_id = r.ride_id
        ) as _ingested_at

    from requested r

    left join accepted a
        on r.ride_id = a.ride_id

    left join started s
        on r.ride_id = s.ride_id

    left join completed c
        on r.ride_id = c.ride_id

    left join cancelled x
        on r.ride_id = x.ride_id

    left join payment p
        on r.ride_id = p.ride_id

)

select
    ride_id,
    rider_id,
    rider_platform,
    app_version,
    requested_vehicle,
    driver_id,
    status,
    requested_at,
    accepted_at,
    started_at,
    completed_at,
    cancelled_at,
    paid_at,
    pickup_zone_id,
    dropoff_zone_id,
    wait_seconds,
    trip_seconds,
    surge_estimate,
    distance_km,
    surge_multiplier,
    fare_amount,
    tolls_amount,
    tip_amount,
    total_amount,
    currency,
    cancelled_by,
    cancel_reason,
    cancel_stage,
    payment_method,
    psp_reference,
    payment_amount,
    _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at

from rides