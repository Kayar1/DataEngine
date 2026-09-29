with expected as (
    select
        requested_hour,
        pickup_zone_key,
        max(_ingested_at) as expected_ingested_at
    from {{ ref('fact_ride') }}
    group by
        requested_hour,
        pickup_zone_key
)
select
    a.requested_hour,
    a.pickup_zone_key
from {{ ref('agg_zone_hourly') }} a
join expected e
    on e.requested_hour = a.requested_hour
   and e.pickup_zone_key = a.pickup_zone_key
where a._ingested_at <> e.expected_ingested_at
