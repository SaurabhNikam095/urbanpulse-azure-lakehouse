-- Fails if two versions of the same driver have overlapping validity windows.
select a.driver_id
from {{ ref('dim_driver') }} as a
inner join {{ ref('dim_driver') }} as b
    on a.driver_id = b.driver_id
    and a.driver_sk <> b.driver_sk
    and a.valid_from < coalesce(b.valid_to, timestamp'9999-12-31')
    and b.valid_from < coalesce(a.valid_to, timestamp'9999-12-31')
