{#-
  SCD Type 2 history of each device's firmware (plus its static attributes).

  The query returns the state of every registered device as of var('run_date'): the
  latest firmware any of its events carried (or the factory firmware from the registry if
  it has not reported yet) and the moment that version took over (firmware_since). The
  timestamp strategy makes dbt_valid_from = firmware_since, so validity windows follow the
  data, not the wall-clock time of the run, and re-running an older date changes nothing.

  Like any snapshot it records one state per run, so it must run once per date in order.
  The DAG's dbt_snapshot task snapshots "as of the day before" and then "as of ds": the
  first ever run therefore records the factory state before the first upgrade.

  It reads sources directly (not staging views) so `dbt snapshot` can run on its own.
-#}
{% snapshot dim_device_snapshot %}
{{
    config(
        unique_key='device_id',
        strategy='timestamp',
        updated_at='firmware_since',
    )
}}

with all_events as (
    {%- for t in ['heartbeat', 'lte_sample', 'gps_fix', 'trip_start', 'trip_end', 'device_event'] %}
    select device_id, firmware_version, event_ts from {{ source('silver', t) }} where event_date <= {{ run_date() }}
    {% if not loop.last %}union all{% endif %}
    {%- endfor %}
),

firmware_seen as (
    -- firmware only moves forward, so the first event on a version opens its window
    select device_id, firmware_version, min(event_ts) as first_seen_at
    from all_events
    group by device_id, firmware_version
),

latest as (
    select *
    from firmware_seen
    qualify row_number() over (partition by device_id order by first_seen_at desc) = 1
),

registry as (
    select
        cast(device_id as varchar) as device_id,
        cast(hardware_model as varchar) as hardware_model,
        cast(region_id as varchar) as region_id,
        cast(install_date as date) as install_date,
        cast(factory_firmware_version as varchar) as factory_firmware_version
    from {{ source('reference', 'fleet_registry') }}
)

select
    r.device_id,
    r.hardware_model,
    r.region_id,
    r.install_date,
    coalesce(l.firmware_version, r.factory_firmware_version) as firmware_version,
    case
        when l.firmware_version is null or l.firmware_version = r.factory_firmware_version
            then cast(r.install_date as timestamp)
        else l.first_seen_at
    end as firmware_since
from registry as r
left join latest as l on r.device_id = l.device_id

{% endsnapshot %}
