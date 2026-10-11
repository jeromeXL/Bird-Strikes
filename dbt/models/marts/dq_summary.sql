-- Data quality summary: one row per rule, with how many live records fail it.
-- Rebuilt on every run, so it shows the current state of the data.

with rules as (

    select * from {{ ref('dq_rules') }}

),

checked as (

    select count(*) as records_checked
    from {{ ref('int_bird_strikes_current') }}
    where not is_deleted

),

failures as (

    select rule_id, count(distinct record_id) as records_failed
    from {{ ref('dq_record_issues') }}
    group by rule_id

)

select
    rules.rule_id,
    rules.rule_name,
    rules.description,
    rules.why_it_matters,
    checked.records_checked,
    coalesce(failures.records_failed, 0)                                      as records_failed,
    round(100 * coalesce(failures.records_failed, 0) / checked.records_checked, 2) as pct_failed,
    current_timestamp()                                                       as checked_at
from rules
cross join checked
left join failures
    on rules.rule_id = failures.rule_id
