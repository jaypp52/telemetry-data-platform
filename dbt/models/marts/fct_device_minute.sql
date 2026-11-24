-- Device x minute (ignition on). One heartbeat, LTE and GPS sample per minute, joined.
{%- set samples = {'hb': 'stg_heartbeat', 'lte': 'stg_lte', 'gps': 'stg_gps'} %}

with
{%- for alias, model in samples.items() %}
{{ alias }} as (
    select *
    from {{ ref(model) }}
    where event_date <= {{ run_date() }}
    qualify row_number() over (partition by device_id, minute_ts order by event_ts, event_id) = 1
),
{%- endfor %}

spine as (
    select device_id, minute_ts from hb
    union
    select device_id, minute_ts from lte
    union
    select device_id, minute_ts from gps
),

joined as (
    select
        s.device_id,
        s.minute_ts,
        coalesce(hb.event_ts, lte.event_ts, gps.event_ts) as sample_ts,
        coalesce(hb.firmware_version, lte.firmware_version, gps.firmware_version) as firmware_version,
        hb.event_id is not null as has_heartbeat,
        hb.cpu_pct,
        hb.mem_free_mb,
        hb.temp_c,
        hb.storage_free_pct,
        lte.rsrp_dbm,
        lte.sinr_db,
        lte.registered,
        lte.ip_assigned,
        gps.lat,
        gps.lon,
        gps.speed_kmh,
        gps.satellites,
        gps.fix_valid
    from spine as s
    left join hb on s.device_id = hb.device_id and s.minute_ts = hb.minute_ts
    left join lte on s.device_id = lte.device_id and s.minute_ts = lte.minute_ts
    left join gps on s.device_id = gps.device_id and s.minute_ts = gps.minute_ts
)

select
    {{ dbt_utils.generate_surrogate_key(['j.device_id', 'j.minute_ts']) }} as device_minute_id,
    j.device_id,
    j.minute_ts,
    cast(j.minute_ts as date) as event_date,
    d.device_key,
    d.hardware_model,
    d.region_id,
    j.firmware_version,
    j.has_heartbeat,
    j.cpu_pct,
    j.mem_free_mb,
    j.temp_c,
    j.storage_free_pct,
    j.rsrp_dbm,
    j.sinr_db,
    j.registered,
    j.ip_assigned,
    coalesce(j.registered = false or j.ip_assigned = false, false) as lte_lost,
    j.lat,
    j.lon,
    j.speed_kmh,
    j.satellites,
    j.fix_valid
from joined as j
left join {{ ref('dim_device') }} as d
    on j.device_id = d.device_id
    and j.sample_ts >= d.valid_from
    and j.sample_ts < d.valid_to
