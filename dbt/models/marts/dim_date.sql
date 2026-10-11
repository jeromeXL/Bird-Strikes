-- Calendar dimension: one row per day covering the flight dates (2000-2011).
-- Generated rather than taken from the data, so days with no strikes still exist.

with days as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2000-01-01' as date)",
        end_date="cast('2012-01-01' as date)"
    ) }}

)

select
    cast(date_day as date)                          as date_day,
    year(date_day)                                  as year,
    quarter(date_day)                               as quarter,
    month(date_day)                                 as month,
    monthname(date_day)                             as month_name,
    date_trunc('month', date_day)::date             as year_month,
    dayofweekiso(date_day)                          as day_of_week,   -- 1 = Monday
    dayname(date_day)                               as day_name,
    dayofweekiso(date_day) in (6, 7)                as is_weekend
from days
