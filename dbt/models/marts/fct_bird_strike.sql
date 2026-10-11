-- Fact table: one row per live bird strike (deleted records excluded).
-- Keys link to the dimensions; strike-level descriptive fields that do not belong
-- to any dimension are kept here (degenerate dimensions).

with strikes as (

    select * from {{ ref('int_bird_strikes_current') }}
    where not is_deleted

),

dq as (

    select record_id, count(*) as dq_issue_count
    from {{ ref('dq_record_issues') }}
    group by record_id

)

select
    -- keys
    strikes.record_id,
    strikes.flight_date,
    {{ dbt_utils.generate_surrogate_key(['strikes.airport_name', 'strikes.origin_state']) }}  as airport_key,
    {{ dbt_utils.generate_surrogate_key(['strikes.aircraft_make_model']) }}                  as aircraft_key,
    {{ dbt_utils.generate_surrogate_key(['strikes.airline_operator']) }}                     as operator_key,
    {{ dbt_utils.generate_surrogate_key(['strikes.wildlife_species']) }}                     as wildlife_key,
    {{ dbt_utils.generate_surrogate_key(['strikes.precipitation', 'strikes.sky_condition']) }} as conditions_key,

    -- strike-level descriptive fields
    strikes.phase_of_flight,
    strikes.impact_to_flight,
    strikes.altitude_bin,
    case strikes.altitude_bin
        when '< 1000 ft' then 1
        when '> 1000 ft' then 2
    end                                                     as altitude_bin_sort,
    strikes.number_struck_bin,
    case strikes.number_struck_bin
        when '1'         then 1
        when '2 to 10'   then 2
        when '11 to 100' then 3
        when 'Over 100'  then 4
    end                                                     as number_struck_bin_sort,
    strikes.wildlife_size,
    strikes.number_of_engines,
    strikes.pilot_warned,
    strikes.remains_collected,
    strikes.remains_sent_to_smithsonian,

    -- measures
    1                                                       as strike_count,
    strikes.caused_damage,
    iff(strikes.caused_damage, 1, 0)                        as damaged_strike_count,
    strikes.cost_total_usd,
    strikes.number_struck_actual,
    strikes.people_injured,
    strikes.feet_above_ground,

    -- data quality
    coalesce(dq.dq_issue_count, 0)                          as dq_issue_count,
    coalesce(dq.dq_issue_count, 0) > 0                      as has_dq_issue

from strikes
left join dq
    on strikes.record_id = dq.record_id
