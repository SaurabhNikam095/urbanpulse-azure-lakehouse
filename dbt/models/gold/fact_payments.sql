-- Grain: one row per payment_id.
select
    payment_id,
    trip_id,
    rider_id,
    upper(method) as method,
    upper(status) as status,
    cast(amount as double) as amount,
    cast(event_ts as timestamp) as paid_at,
    cast(date_format(cast(event_ts as timestamp), 'yyyyMMddHH') as int) as time_key
from {{ source('silver', 'payments') }}
