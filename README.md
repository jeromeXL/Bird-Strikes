# Bird Strikes Data Pipeline

Ingests U.S. bird strike reports delivered by a supplier as CSV files (a base load,
three daily delta files and a weekly full snapshot), checks their quality, and models
them into a star schema in Snowflake for BI reporting.

**Stack:** Snowflake · dbt Core · Prefect · Python

## Architecture

```
Supplier CSV files
      │  load_bird_strikes.py (Prefect)        PUT + COPY INTO, in delivery order
      ▼
RAW           BIRD_STRIKES          all VARCHAR, exactly as received, every delivery kept
      │  transform.py (Prefect) → dbt build
      ▼
STAGING       stg_bird_strikes      cleaned and typed, empty rows removed (view)
      ▼
INTERMEDIATE  int_bird_strikes_current   latest version of each record, deletes flagged (table)
      ▼
MARTS         fct_bird_strike + 6 dimensions    star schema for the BI tool (tables)
              dq_record_issues, dq_summary      data quality results
```

| Layer | Rows | Purpose |
|---|---|---|
| RAW | 60,833 | Audit trail of every delivery, including empty rows and superseded versions |
| STAGING | 48,448 | One row per record per delivery, cleaned and typed. No business logic |
| INTERMEDIATE | 25,558 | One row per record, holding its current state. The 40 records missing from the weekly snapshot are soft-deleted |
| MARTS | 25,518 strikes | Fact table of live strikes, plus dimensions and data quality tables |

## Repository layout

```
load_bird_strikes.py   Prefect flow: files → Snowflake stage → RAW
transform.py           Prefect flow: runs dbt build (optionally one layer)
requirements.txt
dbt/
  dbt_project.yml      layer → schema / materialization
  seeds/               batches.csv (delivery order, load type, schema per file)
                       dq_rules.csv (data quality rule definitions)
  models/staging/      stg_bird_strikes
  models/intermediate/ int_bird_strikes_current
  models/marts/        fct_bird_strike, dim_*, dq_record_issues, dq_summary
  macros/              generate_schema_name (schemas named exactly STAGING, MARTS, ...)
```

## Setup

1. **Python 3.11** (dbt does not support 3.14 yet):
   ```powershell
   py -3.11 -m venv .venv
   .venv\Scripts\pip install -r requirements.txt
   ```
   On Windows, if the project sits in a deep folder (e.g. OneDrive), create the venv in a short path such as `C:\Users\<you>\.venvs\bird-strikes`. Otherwise package installs can fail on the 260-character path limit.
2. **Snowflake key-pair login.** The account enforces MFA, which a script cannot complete, so the pipeline logs in with a key pair. Generate an RSA key and register the public key against your user:
   ```sql
   ALTER USER <user> SET RSA_PUBLIC_KEY = '<public key>';
   ```
3. **`.env`** in the project root, used by the Prefect flows (it's git-ignored):
   ```
   SNOWFLAKE_ACCOUNT=<orgname-accountname>
   SNOWFLAKE_USER=<user>
   SNOWFLAKE_ROLE=ACCOUNTADMIN
   SNOWFLAKE_WAREHOUSE=COMPUTE_WH
   SNOWFLAKE_PRIVATE_KEY_PATH=<path to rsa_key.p8>
   ```
4. **`~/.dbt/profiles.yml`**, used by dbt:
   ```yaml
   bird_strikes:
     target: dev
     outputs:
       dev:
         type: snowflake
         account: <orgname-accountname>
         user: <user>
         private_key_path: <path to rsa_key.p8>
         role: ACCOUNTADMIN
         warehouse: COMPUTE_WH
         database: BIRD_STRIKES
         schema: STAGING
         threads: 4
   ```
5. **Install the dbt packages:**
   ```powershell
   .venv\Scripts\dbt deps --project-dir dbt
   ```
6. **Source files:** place the supplier CSVs in `../Assignment Files/` (next to this folder).

## Running

Activate the venv first (`.venv\Scripts\Activate.ps1`), then from the project root:

```powershell
python load_bird_strikes.py               # 1. load the CSV files into RAW
python transform.py                       # 2. build staging → intermediate → marts, run all tests
python transform.py --select marts        #    or build a single layer
```

- **Creating objects:** the load flow creates the database, schema, file format, stage and RAW table if they don't exist. dbt creates the other schemas.
- **Re-running:** both flows are safe to re-run. `COPY INTO` skips files already loaded, and dbt rebuilds its models from RAW.
- **Replaying from scratch:**
  ```sql
  TRUNCATE TABLE BIRD_STRIKES.RAW.BIRD_STRIKES;   -- also clears COPY's load history
  REMOVE @BIRD_STRIKES.RAW.LANDING_STAGE;
  ```
  Then run both flows again.

## Data model (MARTS)

| Table | Grain | Notes |
|---|---|---|
| `fct_bird_strike` | One live strike | Measures: strike_count, damaged_strike_count, cost_total_usd, number_struck_actual, people_injured. Also holds strike-level fields (phase of flight, altitude bin, wildlife size …) and `has_dq_issue` |
| `dim_date` | Day, 2000–2011 | Generated calendar |
| `dim_airport` | Airport + state | Derived `country` (United States / Canada / Other) |
| `dim_aircraft` | Make/model | Type, large-aircraft flag |
| `dim_operator` | Operator | Derived `operator_category` (Airline, Business, Private, Government, Military, Unknown) |
| `dim_wildlife` | Species | Derived `is_species_identified` |
| `dim_conditions` | Precipitation + sky | The multi-value precipitation field split into `has_fog`, `has_rain`, `has_snow` and `precipitation_category` |

Dimension keys are deterministic hashes (`dbt_utils.generate_surrogate_key`). They stay stable across full rebuilds and need no lookup join.

## Data quality

Data quality is checked at three levels:

1. **Load:** the Prefect flow reports rows loaded and rejected per file, and fails if a file fails to load.
2. **Structure:** dbt tests on every build. Unique and not-null keys, accepted values, and fact-to-dimension relationships.
3. **Business rules:** the rules are defined in `dq_rules.csv`. Failing records are listed in `dq_record_issues` and counted in `dq_summary`. Records are flagged, not removed.

| Rule | Failing records |
|---|---|
| Damage reported with $0 cost | 1,635 |
| Cost reported with no damage | 393 |
| Incomplete record (no date or airport) | 129 |
| Altitude bin vs feet above ground | 2 |
| Number struck bin vs actual | 0 |

## Key design decisions

- **Append-only RAW, loaded by column name.** Day 2 is missing a column and Final adds one mid-file. Matching by header name avoids data shifting into the wrong columns.
- **Transform once, ordered by batch.** All deliveries are loaded, then dbt runs once. Logic is ordered by delivery batch (from the `batches` seed), so the result is the same as processing each day as it arrives.
- **Deletes come from the weekly full snapshot.** Records absent from the latest full load are soft-deleted (`is_deleted`), not removed.
- **Missing columns don't erase values.** When a file didn't supply a column, the value comes from the latest delivery that did.
- **No change history (Type 1).** History wasn't required, and RAW keeps every delivery, so it could be derived later.

## Assumptions

- `Record ID` is the unique business key across all files.
- Delivery dates in `batches.csv` are assumed (the files carry none).
- Exactly 1,000 ft belongs to the `< 1000 ft` bin, following the supplier's practice (825 of the 826 such records).
- `Airport: Name` is used as the airport for the strike. The data has no separate departure and arrival airports.

## Production next steps

- Land supplier files in S3 (external stage), with event-driven loading.
- Run Prefect deployments on a schedule or file trigger, with failure alerts.
- Use a service user and role in place of ACCOUNTADMIN, and separate dev and prod targets.
- Add CI that runs `dbt build` on every pull request.
- Track `dq_summary` over time to show whether data quality is improving.
