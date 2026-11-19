-- Current version of each rider only.
select
    rider_id,
    name,
    status,
    valid_from_ts as valid_from
from {{ source('silver', 'dim_rider') }}
where is_current
