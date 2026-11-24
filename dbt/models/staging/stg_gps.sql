select
    event_id,
    device_id,
    event_type,
    event_ts,
    date_trunc('minute', event_ts) as minute_ts,
    event_date,
    firmware_version,
    lat,
    lon,
    speed_kmh,
    satellites,
    fix_valid,
    is_late,
    ingest_ts
from {{ source('silver', 'gps_fix') }}
