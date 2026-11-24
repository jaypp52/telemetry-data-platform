select
    cast(firmware_version as varchar) as firmware_version,
    cast(release_date as date) as release_date
from {{ source('reference', 'firmware_releases') }}
