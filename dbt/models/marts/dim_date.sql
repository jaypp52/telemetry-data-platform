with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ var('dim_date_start') ~ "' as date)",
        end_date="(cast('" ~ var('dim_date_end') ~ "' as date) + interval 1 day)"
    ) }}
)

select
    cast(date_day as date) as date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day_of_month,
    extract(isodow from date_day) as iso_day_of_week,
    strftime(date_day, '%A') as day_name,
    extract(isodow from date_day) in (6, 7) as is_weekend,
    extract(week from date_day) as iso_week
from spine
