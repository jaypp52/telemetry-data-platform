-- SCD Type 2 device dimension: one row per device per firmware validity window.
select
    dbt_scd_id as device_key,
    device_id,
    hardware_model,
    region_id,
    install_date,
    firmware_version,
    dbt_valid_from as valid_from,
    coalesce(dbt_valid_to, cast('9999-12-31' as timestamp)) as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('dim_device_snapshot') }}
