-- Firmware x hardware model x day. A device that upgraded mid-day contributes one
-- device-day to each version it ran that day; crashes count against the version that
-- crashed (the event's own firmware_version).
with segments as (
    select
        device_id,
        event_date,
        firmware_version,
        hardware_model,
        count(*) as drive_minutes,
        count_if(lte_lost) as lte_loss_minutes
    from {{ ref('fct_device_minute') }}
    group by device_id, event_date, firmware_version, hardware_model
),

device_days as (
    select
        firmware_version,
        hardware_model,
        event_date,
        count(distinct device_id) as device_count,
        count(*) as device_days,
        sum(drive_minutes) as drive_minutes,
        cast(round(avg({{ pct('lte_loss_minutes', 'drive_minutes') }}), 2) as decimal(9, 2)) as mean_lte_loss_pct
    from segments
    group by firmware_version, hardware_model, event_date
),

crashes as (
    select firmware_version, hardware_model, event_date, count(*) as crash_count
    from {{ ref('fct_device_event') }}
    where event_code = 'CAM_CRASH'
    group by firmware_version, hardware_model, event_date
)

select
    d.firmware_version,
    d.hardware_model,
    d.event_date,
    d.device_count,
    d.device_days,
    d.drive_minutes,
    coalesce(c.crash_count, 0) as crash_count,
    round(100.0 * coalesce(c.crash_count, 0) / d.device_days, 3) as crash_rate_per_100_device_days,
    d.mean_lte_loss_pct
from device_days as d
left join crashes as c
    on d.firmware_version = c.firmware_version
    and d.hardware_model = c.hardware_model
    and d.event_date = c.event_date
