select
    ride_id
from {{ ref('fact_ride') }}
where wait_seconds < 0
   or trip_seconds < 0
   or distance_km < 0
   or fare_amount < 0
   or surge_multiplier < 0
   or tolls_amount < 0
   or tip_amount < 0
   or total_amount < 0
   or payment_amount < 0
