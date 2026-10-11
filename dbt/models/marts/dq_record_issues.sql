-- Data quality: one row per live record per rule it fails.
-- Records are flagged, not removed. The rules are listed in the dq_rules seed.

with strikes as (

    select * from {{ ref('int_bird_strikes_current') }}
    where not is_deleted

),

issues as (

    -- exactly 1,000 ft counts as '< 1000 ft', following the supplier's practice
    select
        record_id,
        'altitude_bin_mismatch' as rule_id,
        'altitude_bin: ' || coalesce(altitude_bin, 'null')
            || ' | feet_above_ground: ' || coalesce(to_varchar(feet_above_ground), 'null') as failed_values
    from strikes
    where (altitude_bin = '< 1000 ft' and feet_above_ground > 1000)
       or (altitude_bin = '> 1000 ft' and feet_above_ground <= 1000)

    union all

    select
        record_id,
        'damage_without_cost',
        'caused_damage: true | cost_total_usd: 0'
    from strikes
    where caused_damage and cost_total_usd = 0

    union all

    select
        record_id,
        'cost_without_damage',
        'caused_damage: false | cost_total_usd: ' || to_varchar(cost_total_usd)
    from strikes
    where not caused_damage and cost_total_usd > 0

    union all

    select
        record_id,
        'number_struck_mismatch',
        'number_struck_bin: ' || coalesce(number_struck_bin, 'null')
            || ' | number_struck_actual: ' || coalesce(to_varchar(number_struck_actual), 'null')
    from strikes
    where not case number_struck_bin
                  when '1'         then number_struck_actual = 1
                  when '2 to 10'   then number_struck_actual between 2 and 10
                  when '11 to 100' then number_struck_actual between 11 and 100
                  when 'Over 100'  then number_struck_actual > 100
                  else true
              end

    union all

    select
        record_id,
        'incomplete_record',
        'flight_date: ' || coalesce(to_varchar(flight_date), 'null')
            || ' | airport_name: ' || coalesce(airport_name, 'null')
    from strikes
    where flight_date is null or airport_name is null

)

select
    issues.record_id,
    issues.rule_id,
    rules.rule_name,
    issues.failed_values
from issues
inner join {{ ref('dq_rules') }} as rules
    on issues.rule_id = rules.rule_id
