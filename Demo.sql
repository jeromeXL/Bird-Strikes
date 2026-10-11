-- Part 1: Loading to Raw 
-- 1.1) Bird Strike Files Loaded to Snowflake Landing_Stage
LIST @BIRD_STRIKES.RAW.LANDING_STAGE;

-- 1.2) Bird Strike rows loaded to Database in Raw Schema
select * from BIRD_STRIKES.RAW.BIRD_STRIKES
select * from BIRD_STRIKES.RAW.BIRD_STRIKES where "Record ID" is NULL

-- 1.3) Removing Files form Landing_Stage and Removing Rows from Raw Database to show ReRun
-- rerun command: C:\Users\Jerome\.venvs\bird-strikes\Scripts\python.exe load_bird_strikes.py
TRUNCATE TABLE BIRD_STRIKES.RAW.BIRD_STRIKES;
REMOVE @BIRD_STRIKES.RAW.LANDING_STAGE;

-- 1.4) Load history for the table
SELECT FILE_NAME, STATUS, ROW_COUNT, ROW_PARSED, FIRST_ERROR_MESSAGE, LAST_LOAD_TIME
FROM TABLE(BIRD_STRIKES.INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'BIRD_STRIKES.RAW.BIRD_STRIKES',
    START_TIME => DATEADD(day, -14, CURRENT_TIMESTAMP())))
ORDER BY LAST_LOAD_TIME;

-- Part 2: Cleaned data in Staging layer
-- 2.1) rerun command: C:\Users\Jerome\.venvs\bird-strikes\Scripts\python.exe transform.py
select * from BIRD_STRIKES.STAGING.STG_BIRD_STRIKES
select * from BIRD_STRIKES.STAGING.Batches 

-- Part 3: Current Data in Intermediate layer 
-- 3.1) Latest Data INT_BIRD_STRIKES_CURRENT table 
select * from BIRD_STRIKES.INTERMEDIATE.INT_BIRD_STRIKES_CURRENT 

-- 3.2) Rows that were soft deleted (not in final)
select * from BIRD_STRIKES.INTERMEDIATE.INT_BIRD_STRIKES_CURRENT where is_deleted = TRUE

-- Part 4: MARTS Layer 
-- 4.1 Fact Table for Bird Strikes
select * from BIRD_STRIKES.MARTS.FCT_BIRD_STRIKE

-- 4.2 Dimensions e.g Date, Aircraft, Airport etc
select * from BIRD_STRIKES.MARTS.DIM_AIRCRAFT
select * from BIRD_STRIKES.MARTS.DIM_AIRPORT
select * from BIRD_STRIKES.MARTS.DIM_CONDITIONS
select * from BIRD_STRIKES.MARTS.DIM_DATE
select * from BIRD_STRIKES.MARTS.DIM_OPERATOR
select * from BIRD_STRIKES.MARTS.DIM_WILDLIFE

-- 4.3 Data Quality checks 
select * from BIRD_STRIKES.MARTS.DQ_RECORD_ISSUES
select * from BIRD_STRIKES.MARTS.DQ_SUMMARY

-- Part 5: Data Analysis 
-- 5.1) Which departure airport has most bird strikes
-- select distinct(phase_of_flight), count(record_id) from BIRD_STRIKES.MARTS.FCT_BIRD_STRIKE group by phase_of_flight
select 
    ap.airport_name, 
    count(ap.airport_key) as bird_strike_count
from 
BIRD_STRIKES.MARTS.FCT_BIRD_STRIKE f
left join BIRD_STRIKES.MARTS.DIM_AIRPORT ap 
    on f.airport_key = ap.airport_key 
where f.phase_of_flight 
    in ('Take-off run', 'Climb')
group by ap.airport_key, ap.airport_name
order by 2 desc

-- 5.2) What is the overall trend in reported bird strikes over time? 
-- Reported bird strikes per year
SELECT
    d.year,
    SUM(f.strike_count)                                                    AS strikes,
    ROUND(100 * (SUM(f.strike_count)
          / LAG(SUM(f.strike_count)) OVER (ORDER BY d.year) - 1), 1)       AS change_vs_prior_year_pct,
    SUM(f.damaged_strike_count)                                            AS damaging_strikes,
    ROUND(100 * SUM(f.damaged_strike_count) / SUM(f.strike_count), 1)      AS damage_rate_pct,
    COUNT(DISTINCT f.airport_key)                                          AS airports_reporting
FROM BIRD_STRIKES.MARTS.FCT_BIRD_STRIKE AS f
JOIN BIRD_STRIKES.MARTS.DIM_DATE        AS d
    ON f.flight_date = d.date_day          -- the 129 records with no date are excluded
GROUP BY d.year
ORDER BY d.year;

-- 5.3) Wildlife Size vs Number of birds struck
-- Damage and cost by wildlife size
SELECT
    f.wildlife_size,
    SUM(f.strike_count)                                                AS strikes,
    SUM(f.damaged_strike_count)                                        AS damaging_strikes,
    ROUND(100 * SUM(f.damaged_strike_count) / SUM(f.strike_count), 1)  AS damage_rate_pct,
    ROUND(SUM(f.cost_total_usd) / 1e6, 1)                              AS reported_cost_musd
FROM BIRD_STRIKES.MARTS.FCT_BIRD_STRIKE AS f
WHERE f.wildlife_size IS NOT NULL
GROUP BY f.wildlife_size
ORDER BY damage_rate_pct DESC;

-- Damage and cost by number of birds struck
SELECT
    f.number_struck_bin,
    SUM(f.strike_count)                                                AS strikes,
    ROUND(100 * SUM(f.damaged_strike_count) / SUM(f.strike_count), 1)  AS damage_rate_pct,
    ROUND(AVG(NULLIF(f.cost_total_usd, 0)))                            AS avg_cost_when_reported_usd
FROM BIRD_STRIKES.MARTS.FCT_BIRD_STRIKE AS f
WHERE f.number_struck_bin IS NOT NULL
GROUP BY f.number_struck_bin, f.number_struck_bin_sort
ORDER BY f.number_struck_bin_sort;

-- 5.4) Which types of operator are most exposed to damaging strikes?
SELECT
    o.operator_category,
    SUM(f.strike_count)                                                AS strikes,
    SUM(f.damaged_strike_count)                                        AS damaging_strikes,
    ROUND(100 * SUM(f.damaged_strike_count) / SUM(f.strike_count), 1)  AS damage_rate_pct,
    ROUND(SUM(f.cost_total_usd) / 1e6, 1)                              AS reported_cost_musd
FROM BIRD_STRIKES.MARTS.FCT_BIRD_STRIKE AS f
JOIN BIRD_STRIKES.MARTS.DIM_OPERATOR    AS o
    ON f.operator_key = o.operator_key
GROUP BY o.operator_category
ORDER BY damage_rate_pct DESC;
