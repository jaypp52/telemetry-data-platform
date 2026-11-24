-- SCD2 validity windows of a device must not overlap (and must be non-empty).
with ordered as (
    select
        device_id,
        device_key,
        valid_from,
        valid_to,
        lead(valid_from) over (partition by device_id order by valid_from) as next_valid_from
    from {{ ref('dim_device') }}
)

select *
from ordered
where valid_to <= valid_from
   or (next_valid_from is not null and valid_to > next_valid_from)
