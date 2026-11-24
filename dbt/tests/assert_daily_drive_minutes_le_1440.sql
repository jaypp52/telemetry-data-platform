-- A day has 1440 minutes; more drive minutes means double counting.
select device_id, event_date, drive_minutes
from {{ ref('fct_device_daily') }}
where drive_minutes > 1440
