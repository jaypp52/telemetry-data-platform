-- Data actually arrived: fct_device_daily has rows for the run's date. A daily Airflow run
-- for logical date D executes after D ends, so this is "yesterday" from the
-- scheduler's point of view. Without run_date (CI, ad-hoc), check the latest loaded day.
{% if run_date_is_set() %}
    {% set target_date = run_date() %}
{% else %}
    {% set target_date = "(select max(event_date) from " ~ ref('fct_device_daily') ~ ")" %}
{% endif %}

select 'no fct_device_daily rows for ' || cast({{ target_date }} as varchar) as failure
from (select count(*) as n from {{ ref('fct_device_daily') }} where event_date = {{ target_date }})
where n = 0
