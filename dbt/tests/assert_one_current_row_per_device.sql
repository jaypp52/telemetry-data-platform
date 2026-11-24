-- No device may have two is_current = true rows in dim_device.
select device_id, count(*) as current_rows
from {{ ref('dim_device') }}
where is_current
group by device_id
having count(*) > 1
