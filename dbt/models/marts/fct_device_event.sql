-- One row per device event, joined to the device version that was live at event time.
select
    e.event_id,
    e.device_id,
    d.device_key,
    e.event_ts,
    e.event_date,
    e.event_code,
    e.severity,
    e.message,
    e.firmware_version,
    d.hardware_model,
    d.region_id,
    e.is_late
from {{ ref('stg_device_event') }} as e
left join {{ ref('dim_device') }} as d
    on e.device_id = d.device_id
    and e.event_ts >= d.valid_from
    and e.event_ts < d.valid_to
where e.event_date <= {{ run_date() }}
