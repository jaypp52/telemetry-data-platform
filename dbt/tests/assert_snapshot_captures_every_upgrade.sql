-- Every device seen on more than one firmware version has one dim_device row per version.
with reported as (
    select device_id, count(distinct firmware_version) as versions_seen
    from {{ ref('stg_heartbeat') }}
    where event_date <= {{ run_date() }}
    group by device_id
),

captured as (
    select device_id, count(*) as versions_in_dim
    from {{ ref('dim_device') }}
    group by device_id
)

select r.device_id, r.versions_seen, c.versions_in_dim
from reported as r
left join captured as c on r.device_id = c.device_id
where r.versions_seen > 1 and coalesce(c.versions_in_dim, 0) < r.versions_seen
