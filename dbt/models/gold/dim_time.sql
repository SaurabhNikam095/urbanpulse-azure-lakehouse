-- One row per hour between the first and last event in Silver trips and payments.
with bounds as (
    select min(ts) as lo, max(ts) as hi
    from (
        select cast(event_ts as timestamp) as ts from {{ source('silver', 'trips') }}
        union all
        select cast(event_ts as timestamp) as ts from {{ source('silver', 'payments') }}
    )
),

hours as (
    select explode(sequence(date_trunc('hour', lo), date_trunc('hour', hi), interval 1 hour)) as ts_hour
    from bounds
)

select
    cast(date_format(ts_hour, 'yyyyMMddHH') as int) as time_key,
    ts_hour,
    cast(ts_hour as date) as date_day,
    year(ts_hour) as year,
    month(ts_hour) as month,
    day(ts_hour) as day,
    hour(ts_hour) as hour,
    date_format(ts_hour, 'EEEE') as day_name,
    dayofweek(ts_hour) in (1, 7) as is_weekend
from hours
