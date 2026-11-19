-- SCD2 history from Silver. Hashed phone and email are not exposed in Gold.
select
    md5(concat(driver_id, '|', cast(valid_from as string))) as driver_sk,
    driver_id,
    name,
    vehicle_type,
    cast(rating as double) as rating,
    status,
    row_hash,
    valid_from_ts as valid_from,
    valid_to_ts as valid_to,
    is_current
from {{ source('silver', 'dim_driver') }}
