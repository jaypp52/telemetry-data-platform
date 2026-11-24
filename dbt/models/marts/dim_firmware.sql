select
    firmware_version,
    release_date,
    cast(split_part(firmware_version, '.', 1) as integer) as major,
    cast(split_part(firmware_version, '.', 2) as integer) as minor,
    cast(split_part(firmware_version, '.', 3) as integer) as patch,
    release_date = max(release_date) over () as is_latest
from {{ ref('stg_firmware_releases') }}
