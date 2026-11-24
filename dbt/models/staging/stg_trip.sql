-- One row per trip_id, pairing ignition-on and ignition-off. A trip whose start or end
-- falls outside the loaded data is kept with is_complete = false.
with starts as (
    select trip_id, device_id, event_ts as started_at, odometer_km as start_odometer_km, firmware_version
    from {{ source('silver', 'trip_start') }}
),

ends as (
    select trip_id, device_id, event_ts as ended_at, odometer_km as end_odometer_km
    from {{ source('silver', 'trip_end') }}
)

select
    coalesce(s.trip_id, e.trip_id) as trip_id,
    coalesce(s.device_id, e.device_id) as device_id,
    s.started_at,
    e.ended_at,
    s.start_odometer_km,
    e.end_odometer_km,
    s.firmware_version,
    s.trip_id is not null and e.trip_id is not null as is_complete
from starts as s
full outer join ends as e on s.trip_id = e.trip_id
