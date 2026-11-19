-- Grid zones of 0.01 degrees (about 1 km), derived from pickup and drop points.
with points as (
    select pickup_lat as lat, pickup_lng as lng from {{ source('silver', 'trips') }}
    union all
    select drop_lat as lat, drop_lng as lng from {{ source('silver', 'trips') }}
)

select distinct
    {{ zone_id('lat', 'lng') }} as zone_id,
    round(lat, 2) as zone_lat,
    round(lng, 2) as zone_lng
from points
where lat is not null and lng is not null
