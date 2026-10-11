-- Aircraft dimension: one row per aircraft make/model.
-- Number of engines is not here: it varies between records of the same model
-- (A-300 appears with 2 and 4), so it is kept on the fact.

select
    {{ dbt_utils.generate_surrogate_key(['aircraft_make_model']) }}  as aircraft_key,
    aircraft_make_model,
    max(aircraft_type)                                              as aircraft_type,
    boolor_agg(is_large_aircraft)                                   as is_large_aircraft
from {{ ref('int_bird_strikes_current') }}
where not is_deleted
group by aircraft_make_model
