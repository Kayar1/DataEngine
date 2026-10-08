{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['requested_hour', 'pickup_zone_key'],
    pre_hook=[
        """
        DO $$
        BEGIN
            IF to_regclass('{{ this }}') IS NOT NULL THEN
                DELETE FROM {{ this }}
                WHERE requested_hour IN (
                    SELECT DISTINCT requested_hour
                    FROM {{ ref('fact_ride') }}
                    WHERE _ingested_at >= {{ high_watermark('_ingested_at') }}
                );
            END IF;
        END
        $$;
        """
    ]
) }}

{% if is_incremental() %}

{% set affected_hours_query %}
    select distinct requested_hour
    from {{ ref('fact_ride') }}
    where _ingested_at >= {{ high_watermark('_ingested_at') }}
{% endset %}

{% set affected_hours_result = run_query(affected_hours_query) %}

{% if execute %}
    {% set affected_hours = affected_hours_result.columns[0].values() %}
{% else %}
    {% set affected_hours = [] %}
{% endif %}

{% else %}

{% set affected_hours = [] %}

{% endif %}

with source_data as (

    select
        f.requested_hour,
        f.pickup_zone_key,
        f.status,
        f.total_amount,
        f.tip_amount,
        f.wait_seconds,
        f._ingested_at
    from {{ ref('fact_ride') }} f

    {% if is_incremental() and affected_hours | length > 0 %}

    where f.requested_hour in (
        {% for hour in affected_hours %}
            '{{ hour }}'::timestamptz{% if not loop.last %},{% endif %}
        {% endfor %}
    )

    {% elif is_incremental() %}

    where 1 = 0

    {% endif %}

)

select
    requested_hour,
    pickup_zone_key,
    count(*) as rides_requested,
    count(*) filter (where status = 'completed') as rides_completed,
    count(*) filter (where status = 'cancelled') as rides_cancelled,
    coalesce(
        sum(total_amount) filter (where status = 'completed'),
        0
    )::numeric(12, 2) as gross_revenue,
    coalesce(
        sum(tip_amount) filter (where status = 'completed'),
        0
    )::numeric(12, 2) as tips,
    avg(wait_seconds) as avg_wait_seconds,
    max(_ingested_at) as _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from source_data
group by
    requested_hour,
    pickup_zone_key