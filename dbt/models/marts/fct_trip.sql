-- One row per trip. Duration, distance and average speed are null for incomplete trips.
with trips as (
    select
        *,
        coalesce(started_at, ended_at) as anchor_ts
    from {{ ref('stg_trip') }}
    where cast(coalesce(started_at, ended_at) as date) <= {{ run_date() }}
)

select
    t.trip_id,
    t.device_id,
    d.device_key,
    t.started_at,
    t.ended_at,
    cast(t.anchor_ts as date) as trip_date,
    case when t.is_complete then round(date_diff('second', t.started_at, t.ended_at) / 60.0, 2) end as duration_min,
    case when t.is_complete then round(t.end_odometer_km - t.start_odometer_km, 2) end as distance_km,
    case
        when t.is_complete and t.ended_at > t.started_at
            then round((t.end_odometer_km - t.start_odometer_km) / (date_diff('second', t.started_at, t.ended_at) / 3600.0), 2)
    end as avg_speed_kmh,
    coalesce(t.firmware_version, d.firmware_version) as firmware_version,
    d.hardware_model,
    d.region_id,
    t.is_complete
from trips as t
left join {{ ref('dim_device') }} as d
    on t.device_id = d.device_id
    and t.anchor_ts >= d.valid_from
    and t.anchor_ts < d.valid_to
