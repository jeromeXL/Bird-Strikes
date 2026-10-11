-- Intermediate: current state of each bird strike record.
-- One row per record, holding its most recent delivered version. Supplier
-- corrections are applied as updates (Type 1). Records missing from the latest
-- full snapshot are kept but flagged is_deleted.

with batches as (

    select * from {{ ref('batches') }}

),

staged as (

    select
        stg.*,
        batches.delivery_date,
        batches.has_pilot_warned,
        batches.has_number_of_engines
    from {{ ref('stg_bird_strikes') }} as stg
    inner join batches
        on stg.batch_seq = batches.batch_seq

),

latest_full_batch as (

    -- the most recent complete snapshot: anything not in it has been deleted
    select batch_seq, delivery_date
    from batches
    where load_type = 'full'
    qualify row_number() over (order by batch_seq desc) = 1

),

with_history as (

    select
        staged.*,

        -- Columns a file didn't contain must not blank out a value: take the value
        -- from the latest batch that actually supplied the column.
        last_value(iff(has_pilot_warned, pilot_warned, null)) ignore nulls over (
            partition by record_id order by batch_seq
            rows between unbounded preceding and unbounded following
        )                                                        as pilot_warned_latest,
        last_value(iff(has_number_of_engines, number_of_engines, null)) ignore nulls over (
            partition by record_id order by batch_seq
            rows between unbounded preceding and unbounded following
        )                                                        as number_of_engines_latest,

        min(batch_seq)     over (partition by record_id)         as first_seen_batch,
        min(delivery_date) over (partition by record_id)         as first_seen_date,
        max(batch_seq)     over (partition by record_id)         as last_seen_batch,
        max(delivery_date) over (partition by record_id)         as last_seen_date,
        count(*)           over (partition by record_id)         as times_delivered

    from staged

),

latest as (

    -- keep each record's most recent delivery
    select * from with_history
    qualify row_number() over (partition by record_id order by batch_seq desc) = 1

)

select
    latest.record_id,

    -- when and where
    latest.flight_date,
    latest.airport_name,
    latest.origin_state,
    latest.phase_of_flight,
    latest.altitude_bin,
    latest.feet_above_ground,

    -- aircraft
    latest.aircraft_type,
    latest.aircraft_make_model,
    latest.airline_operator,
    latest.number_of_engines_latest                              as number_of_engines,
    latest.is_large_aircraft,

    -- conditions
    latest.precipitation,
    latest.sky_condition,

    -- wildlife
    latest.wildlife_species,
    latest.wildlife_size,
    latest.number_struck_bin,
    latest.number_struck_actual,
    latest.pilot_warned_latest                                   as pilot_warned,
    latest.remains_collected,
    latest.remains_sent_to_smithsonian,

    -- effect
    latest.impact_to_flight,
    latest.caused_damage,
    latest.cost_total_usd,
    latest.people_injured,
    latest.remarks,

    -- delivery history
    latest.first_seen_batch,
    latest.first_seen_date,
    latest.last_seen_batch,
    latest.last_seen_date,
    latest.times_delivered,

    -- deletes: not in the latest full snapshot
    coalesce(latest.last_seen_batch < latest_full_batch.batch_seq, false)
                                                                 as is_deleted,
    iff(latest.last_seen_batch < latest_full_batch.batch_seq,
        latest_full_batch.delivery_date, null)                  as deleted_date

from latest
left join latest_full_batch
    on true
