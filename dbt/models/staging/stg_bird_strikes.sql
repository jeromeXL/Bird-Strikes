-- Staging: cleaned and typed copy of RAW.BIRD_STRIKES.
-- One row per non-empty RAW row: every version of every record is kept.
-- No business logic here (no dedup, deletes or derived fields) - that is the
-- job of the intermediate layer.

with source as (

    select * from {{ source('raw', 'bird_strikes') }}
    where "Record ID" is not null          -- drop the empty rows in the daily files

),

batches as (

    select * from {{ ref('batches') }}

),

cleaned as (

    select
        -- keys and batch information
        to_number(source."Record ID")                                   as record_id,
        batches.batch_seq,
        batches.batch_name,
        batches.load_type,
        source._file_name                                                as source_file,
        source._loaded_at                                                as loaded_at,

        -- when and where
        try_to_date(source."FlightDate", 'DD/MM/YYYY HH24:MI')           as flight_date,
        trim(source."Airport: Name")                                     as airport_name,
        trim(source."Origin State")                                      as origin_state,
        trim(source."When: Phase of flight")                             as phase_of_flight,
        trim(source."Altitude bin")                                      as altitude_bin,
        try_to_number(replace(source."Feet above ground", ',', ''))      as feet_above_ground,

        -- aircraft
        trim(source."Aircraft: Type")                                    as aircraft_type,
        trim(source."Aircraft: Make/Model")                              as aircraft_make_model,
        trim(source."Aircraft: Airline/Operator")                        as airline_operator,
        try_to_number(source."Aircraft: Number of engines?")             as number_of_engines,
        try_to_boolean(trim(source."Is Aircraft Large?"))                as is_large_aircraft,

        -- conditions
        trim(source."Conditions: Precipitation")                         as precipitation,
        trim(source."Conditions: Sky")                                   as sky_condition,

        -- wildlife
        trim(source."Wildlife: Species")                                 as wildlife_species,
        trim(source."Wildlife: Size")                                    as wildlife_size,
        trim(source."Wildlife: Number struck")                           as number_struck_bin,
        try_to_number(replace(source."Wildlife: Number Struck Actual", ',', ''))
                                                                         as number_struck_actual,
        try_to_boolean(trim(source."Pilot warned of birds or wildlife?")) as pilot_warned,
        try_to_boolean(trim(source."Remains of wildlife collected?"))    as remains_collected,
        try_to_boolean(trim(source."Remains of wildlife sent to Smithsonian"))
                                                                         as remains_sent_to_smithsonian,

        -- effect
        trim(source."Effect: Impact to flight")                          as impact_to_flight,
        source."Effect: Indicated Damage" = 'Caused damage'              as caused_damage,
        try_to_number(replace(source."Cost: Total $", ',', ''))          as cost_total_usd,
        try_to_number(replace(source."Number of people injured", ',', ''))
                                                                         as people_injured,

        -- free text: 'None' / 'NONE' / 'None.' mean no remark
        iff(upper(trim(source."Remarks")) in ('NONE', 'NONE.'), null, trim(source."Remarks"))
                                                                         as remarks

    from source
    left join batches
        on split_part(source._file_name, '/', 1) = batches.stage_folder

)

select * from cleaned
