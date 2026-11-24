select
    event_id,
    device_id,
    event_type,
    event_ts,
    event_date,
    firmware_version,
    event_code,
    severity,
    message,
    is_late,
    ingest_ts
from {{ source('silver', 'device_event') }}
