select
    cast(device_id as varchar) as device_id,
    cast(hardware_model as varchar) as hardware_model,
    cast(region_id as varchar) as region_id,
    cast(install_date as date) as install_date,
    cast(factory_firmware_version as varchar) as factory_firmware_version
from {{ source('reference', 'fleet_registry') }}
