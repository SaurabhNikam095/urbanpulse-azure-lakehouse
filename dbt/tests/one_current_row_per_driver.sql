-- Fails if a driver has more than one current version.
select driver_id
from {{ ref('dim_driver') }}
group by driver_id
having sum(case when is_current then 1 else 0 end) > 1
