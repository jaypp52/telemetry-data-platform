select
    event_id,
    device_id,
    event_type,
    event_ts,
    date_trunc('minute', event_ts) as minute_ts,
    event_date,
    firmware_version,
    rsrp_dbm,
    sinr_db,
    registered,
    ip_assigned,
    is_late,
    ingest_ts
from {{ source('silver', 'lte_sample') }}
