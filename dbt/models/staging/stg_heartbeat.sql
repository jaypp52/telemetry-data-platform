select
    event_id,
    device_id,
    event_type,
    event_ts,
    date_trunc('minute', event_ts) as minute_ts,
    event_date,
    firmware_version,
    cpu_pct,
    mem_free_mb,
    temp_c,
    storage_free_pct,
    is_late,
    ingest_ts
from {{ source('silver', 'heartbeat') }}
