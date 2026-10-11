-- Airport dimension: one row per airport and state.
-- Incomplete records have no airport, so each state also gets an 'Unknown' airport row.

with airports as (

    select distinct airport_name, origin_state
    from {{ ref('int_bird_strikes_current') }}
    where not is_deleted

)

select
    {{ dbt_utils.generate_surrogate_key(['airport_name', 'origin_state']) }}  as airport_key,
    coalesce(airport_name, 'Unknown')                                        as airport_name,
    origin_state,
    case
        when origin_state in ('Alberta', 'British Columbia', 'Manitoba', 'New Brunswick',
                              'Newfoundland and Labrador', 'Nova Scotia', 'Ontario',
                              'Prince Edward Island', 'Quebec', 'Saskatchewan')
            then 'Canada'
        when origin_state = 'N/A' or origin_state is null
            then 'Other / unknown'
        else 'United States'   -- 50 states plus DC, Puerto Rico and the Virgin Islands
    end                                                                      as country
from airports
