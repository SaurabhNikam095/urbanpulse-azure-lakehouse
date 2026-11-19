-- Grain: one row per trip_id. Driver is joined point-in-time on the first event of the trip.
with ev as (
    select
        trip_id,
        event_type,
        rider_id,
        driver_id,
        pickup_lat,
        pickup_lng,
        drop_lat,
        drop_lng,
        fare,
        cast(event_ts as timestamp) as event_ts
    from {{ source('silver', 'trips') }}
),

trip as (
    select
        trip_id,
        max(rider_id) as rider_id,
        max(driver_id) as driver_id,
        min_by(pickup_lat, event_ts) as pickup_lat,
        min_by(pickup_lng, event_ts) as pickup_lng,
        min_by(drop_lat, event_ts) as drop_lat,
        min_by(drop_lng, event_ts) as drop_lng,
        min(event_ts) as first_event_at,
        min(case when event_type = 'started' then event_ts end) as started_at,
        min(case when event_type = 'completed' then event_ts end) as completed_at,
        min(case when event_type = 'cancelled' then event_ts end) as cancelled_at,
        max(case when event_type = 'completed' then fare end) as fare,
        max(case when event_type = 'completed' then 1 else 0 end) as is_completed,
        max(case when event_type = 'cancelled' then 1 else 0 end) as is_cancelled
    from ev
    group by trip_id
)

select
    t.trip_id,
    t.rider_id,
    d.driver_sk,
    t.driver_id,
    {{ zone_id('t.pickup_lat', 't.pickup_lng') }} as pickup_zone_id,
    {{ zone_id('t.drop_lat', 't.drop_lng') }} as drop_zone_id,
    cast(date_format(t.first_event_at, 'yyyyMMddHH') as int) as time_key,
    t.first_event_at,
    t.started_at,
    t.completed_at,
    t.cancelled_at,
    case
        when t.is_completed = 1 then 'completed'
        when t.is_cancelled = 1 then 'cancelled'
        else 'open'
    end as status,
    t.fare
from trip as t
left join {{ ref('dim_driver') }} as d
    on t.driver_id = d.driver_id
    and t.first_event_at >= d.valid_from
    and (d.valid_to is null or t.first_event_at < d.valid_to)
