-- Weather conditions dimension: one row per precipitation + sky combination.
-- "Conditions: Precipitation" can hold several values (e.g. 'Fog, Rain'), so it is
-- split into one flag per condition, which BI tools can filter on directly.

with conditions as (

    select distinct precipitation, sky_condition
    from {{ ref('int_bird_strikes_current') }}
    where not is_deleted

),

flags as (

    select
        precipitation,
        sky_condition,
        coalesce(contains(precipitation, 'Fog'), false)   as has_fog,
        coalesce(contains(precipitation, 'Rain'), false)  as has_rain,
        coalesce(contains(precipitation, 'Snow'), false)  as has_snow
    from conditions

)

select
    {{ dbt_utils.generate_surrogate_key(['precipitation', 'sky_condition']) }}  as conditions_key,
    precipitation,
    sky_condition,
    has_fog,
    has_rain,
    has_snow,
    has_fog or has_rain or has_snow                                            as has_precipitation,
    has_fog::int + has_rain::int + has_snow::int                               as precipitation_condition_count,
    case has_fog::int + has_rain::int + has_snow::int
        when 0 then 'None'
        when 1 then 'Single'
        else 'Mixed'
    end                                                                        as precipitation_category
from flags
