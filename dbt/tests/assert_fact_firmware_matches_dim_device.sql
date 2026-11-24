-- The as-of join must pick the window whose firmware matches what the device reported.
select f.device_id, f.minute_ts, f.firmware_version as reported, d.firmware_version as dimension
from {{ ref('fct_device_minute') }} as f
inner join {{ ref('dim_device') }} as d on f.device_key = d.device_key
where f.has_heartbeat and f.firmware_version <> d.firmware_version
