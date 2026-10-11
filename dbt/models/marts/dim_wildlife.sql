-- Wildlife dimension: one row per species.
-- Size is not here: the same species is recorded with different sizes, so it is
-- kept on the fact.

with species as (

    select distinct wildlife_species
    from {{ ref('int_bird_strikes_current') }}
    where not is_deleted

)

select
    {{ dbt_utils.generate_surrogate_key(['wildlife_species']) }}  as wildlife_key,
    wildlife_species,
    not coalesce(wildlife_species ilike 'Unknown%', true)         as is_species_identified
from species
