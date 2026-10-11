-- Operator dimension: one row per airline/operator.
-- The source mixes named operators with generic categories (BUSINESS, MILITARY, ...),
-- so operator_category groups them for reporting.

with operators as (

    select distinct airline_operator
    from {{ ref('int_bird_strikes_current') }}
    where not is_deleted

)

select
    {{ dbt_utils.generate_surrogate_key(['airline_operator']) }}  as operator_key,
    coalesce(trim(replace(airline_operator, '*', '')), 'Unknown')  as operator_name,   -- 'US AIRWAYS*' -> 'US AIRWAYS'
    case upper(trim(airline_operator))
        when 'BUSINESS'        then 'Business'
        when 'PRIVATELY OWNED' then 'Private'
        when 'GOVERNMENT'      then 'Government'
        when 'MILITARY'        then 'Military'
        when 'UNKNOWN'         then 'Unknown'
        else iff(airline_operator is null, 'Unknown', 'Airline / named operator')
    end                                                           as operator_category,
    airline_operator                                              as source_operator_name
from operators
