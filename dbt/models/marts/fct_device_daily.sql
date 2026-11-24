-- Device x day aggregate. A device-day exists when the device drove or reported an event.
with minutes as (
    select
        device_id,
        event_date,
        count(*) as drive_minutes,
        count_if(lte_lost) as lte_loss_minutes,
        count_if(fix_valid) as gps_valid_minutes,
        -- DECIMAL sums are exact, so the result does not depend on thread order
        cast(round(avg(cast(temp_c as decimal(6, 1))), 2) as decimal(6, 2)) as avg_temp_c,
        max(temp_c) as max_temp_c,
        cast(round(sum(cast(coalesce(speed_kmh, 0) as decimal(9, 1))) / 60, 2) as decimal(12, 2)) as gps_distance_km,
        arg_max(firmware_version, minute_ts) as firmware_version,
        arg_max(device_key, minute_ts) as device_key,
        count(distinct firmware_version) > 1 as firmware_changed
    from {{ ref('fct_device_minute') }}
    group by device_id, event_date
),

events as (
    select
        device_id,
        event_date,
        count(*) as device_event_count,
        count_if(event_code = 'CAM_CRASH') as crash_count,
        count_if(event_code = 'REBOOT') as reboot_count,
        count_if(event_code = 'OVERHEAT') as overheat_count,
        arg_max(firmware_version, event_ts) as firmware_version,
        arg_max(device_key, event_ts) as device_key
    from {{ ref('fct_device_event') }}
    group by device_id, event_date
),

trips as (
    select
        device_id,
        trip_date as event_date,
        count(*) as trip_count,
        sum(cast(distance_km as decimal(12, 2))) as trip_distance_km
    from {{ ref('fct_trip') }}
    where is_complete
    group by device_id, trip_date
),

keys as (
    select device_id, event_date from minutes
    union
    select device_id, event_date from events
)

select
    k.device_id,
    k.event_date,
    coalesce(m.device_key, e.device_key) as device_key,
    r.hardware_model,
    r.region_id,
    coalesce(m.firmware_version, e.firmware_version) as firmware_version,
    coalesce(m.firmware_changed, false) as firmware_changed,
    coalesce(m.drive_minutes, 0) as drive_minutes,
    coalesce(m.lte_loss_minutes, 0) as lte_loss_minutes,
    {{ pct('m.lte_loss_minutes', 'm.drive_minutes') }} as lte_loss_pct,
    {{ pct('m.gps_valid_minutes', 'm.drive_minutes') }} as gps_valid_pct,
    m.avg_temp_c,
    m.max_temp_c,
    coalesce(t.trip_count, 0) as trip_count,
    coalesce(t.trip_distance_km, 0) as trip_distance_km,
    coalesce(e.device_event_count, 0) as device_event_count,
    coalesce(e.crash_count, 0) as crash_count,
    coalesce(e.reboot_count, 0) as reboot_count,
    coalesce(e.overheat_count, 0) as overheat_count
from keys as k
left join minutes as m on k.device_id = m.device_id and k.event_date = m.event_date
left join events as e on k.device_id = e.device_id and k.event_date = e.event_date
left join trips as t on k.device_id = t.device_id and k.event_date = t.event_date
left join {{ ref('stg_fleet_registry') }} as r on k.device_id = r.device_id
