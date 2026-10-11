"""Prefect flow: load the Bird Strikes CSV files into Snowflake.

Each file is uploaded (PUT) to an internal stage, then loaded (COPY INTO) into
BIRD_STRIKES.RAW.BIRD_STRIKES. Files are loaded in delivery order.

Run:  .venv\\Scripts\\python.exe load_bird_strikes.py
"""

import os
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE

PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR.parent / "Assignment Files"

# Delivery order matters: later files update records from earlier ones.
FILES = [
    ("Bird Strikes - Base.csv", "01_base"),
    ("Bird Strikes - Day 1.csv", "02_day1"),
    ("Bird Strikes - Day 2.csv", "03_day2"),
    ("Bird Strikes - Day 3.csv", "04_day3"),
    ("Bird Strikes - Final.csv", "05_final"),
]

SETUP_SQL = [
    "CREATE DATABASE IF NOT EXISTS BIRD_STRIKES",
    "CREATE SCHEMA IF NOT EXISTS BIRD_STRIKES.RAW",
    "USE SCHEMA BIRD_STRIKES.RAW",
    # PARSE_HEADER: match columns by name (Day 2 is missing one, Final adds one mid-row)
    # FIELD_OPTIONALLY_ENCLOSED_BY: Remarks contain commas and line breaks
    # ENCODING: files are Windows/Excel exports, not UTF-8
    # ERROR_ON_COLUMN_COUNT_MISMATCH: files have fewer columns than the table
    #   (no metadata columns, some files lack a column) - matched by name instead
    """CREATE FILE FORMAT IF NOT EXISTS CSV_FORMAT
        TYPE = CSV
        PARSE_HEADER = TRUE
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        ENCODING = 'WINDOWS1252'
        EMPTY_FIELD_AS_NULL = TRUE
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE""",
    "CREATE STAGE IF NOT EXISTS LANDING_STAGE FILE_FORMAT = CSV_FORMAT",
    # All VARCHAR, exact source column names; typing/cleansing happens later in dbt
    """CREATE TABLE IF NOT EXISTS BIRD_STRIKES (
        "Record ID" VARCHAR,
        "Aircraft: Type" VARCHAR,
        "Airport: Name" VARCHAR,
        "Altitude bin" VARCHAR,
        "Aircraft: Make/Model" VARCHAR,
        "Wildlife: Number struck" VARCHAR,
        "Wildlife: Number Struck Actual" VARCHAR,
        "Effect: Impact to flight" VARCHAR,
        "FlightDate" VARCHAR,
        "Effect: Indicated Damage" VARCHAR,
        "Aircraft: Number of engines?" VARCHAR,
        "Aircraft: Airline/Operator" VARCHAR,
        "Origin State" VARCHAR,
        "When: Phase of flight" VARCHAR,
        "Conditions: Precipitation" VARCHAR,
        "Remains of wildlife collected?" VARCHAR,
        "Remains of wildlife sent to Smithsonian" VARCHAR,
        "Remarks" VARCHAR,
        "Wildlife: Size" VARCHAR,
        "Conditions: Sky" VARCHAR,
        "Wildlife: Species" VARCHAR,
        "Pilot warned of birds or wildlife?" VARCHAR,
        "Cost: Total $" VARCHAR,
        "Feet above ground" VARCHAR,
        "Number of people injured" VARCHAR,
        "Is Aircraft Large?" VARCHAR,
        _FILE_NAME VARCHAR,
        _LOADED_AT TIMESTAMP_LTZ
    )""",
]


def connect():
    load_dotenv(PROJECT_DIR / ".env")
    params = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    }
    # Key pair is preferred: works for unattended runs on MFA-enforced accounts
    if os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"):
        params["private_key_file"] = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
    elif os.getenv("SNOWFLAKE_AUTHENTICATOR"):
        params["authenticator"] = os.environ["SNOWFLAKE_AUTHENTICATOR"]
    else:
        params["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    return snowflake.connector.connect(**params)


@task(cache_policy=NO_CACHE)
def setup_snowflake(conn) -> None:
    cur = conn.cursor()
    for sql in SETUP_SQL:
        cur.execute(sql)
    get_run_logger().info("Database, schema, file format, stage and table are ready")


@task(cache_policy=NO_CACHE)
def load_file(conn, file_name: str, stage_folder: str) -> int:
    logger = get_run_logger()
    cur = conn.cursor()
    local_path = (DATA_DIR / file_name).as_posix()

    cur.execute(f"PUT 'file://{local_path}' @LANDING_STAGE/{stage_folder}/ AUTO_COMPRESS = TRUE")
    logger.info(f"Uploaded {file_name} to @LANDING_STAGE/{stage_folder}/")

    cur.execute(f"""
        COPY INTO BIRD_STRIKES
        FROM @LANDING_STAGE/{stage_folder}/
        MATCH_BY_COLUMN_NAME = CASE_SENSITIVE
        INCLUDE_METADATA = (_FILE_NAME = METADATA$FILENAME, _LOADED_AT = METADATA$START_SCAN_TIME)
        ON_ERROR = CONTINUE
    """)
    cols = [c[0].lower() for c in cur.description]
    results = [dict(zip(cols, row)) for row in cur.fetchall()]

    # COPY returns only a 'status' column when the file was already loaded before
    if "rows_loaded" not in cols:
        logger.info(f"{file_name}: already loaded, skipped")
        return 0

    rows_loaded = sum(r["rows_loaded"] for r in results)
    errors = sum(r["errors_seen"] for r in results)
    logger.info(f"{file_name}: loaded {rows_loaded} rows, {errors} errors")
    if errors:
        logger.warning(f"{file_name}: first error: {results[0]['first_error']}")
    # Fail the task (and stop later files, since order matters) if the file didn't load
    if any(r["status"] == "LOAD_FAILED" for r in results):
        raise RuntimeError(f"{file_name} failed to load: {results[0]['first_error']}")
    return rows_loaded


@flow(name="load-bird-strikes")
def load_bird_strikes() -> None:
    conn = connect()
    try:
        setup_snowflake(conn)
        for file_name, stage_folder in FILES:
            load_file(conn, file_name, stage_folder)
    finally:
        conn.close()


if __name__ == "__main__":
    load_bird_strikes()
