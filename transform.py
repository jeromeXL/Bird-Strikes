"""Prefect flow: transform the Bird Strikes data with dbt.

Runs `dbt build` on the dbt project in ./dbt, which builds every model
(staging -> intermediate -> marts, as they exist) in dependency order and runs
their tests. Pass --select to build a single layer.

Run:  python transform.py                    # all layers
      python transform.py --select staging   # one layer
"""

import argparse
import subprocess
import sys
from pathlib import Path

from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE

DBT_DIR = Path(__file__).resolve().parent / "dbt"
DBT_EXE = Path(sys.executable).parent / "dbt.exe"  # dbt from the same venv as Python


@task(cache_policy=NO_CACHE)
def run_dbt(select: str | None = None) -> None:
    logger = get_run_logger()
    command = [str(DBT_EXE), "--no-use-colors", "build", "--project-dir", str(DBT_DIR)]
    if select:
        command += ["--select", select]
    logger.info(f"Running: dbt build{' --select ' + select if select else ''}")

    result = subprocess.run(command, capture_output=True, text=True, encoding="utf-8", errors="replace")
    for line in (result.stdout + result.stderr).splitlines():
        if line.strip():
            logger.info(line)

    # dbt exits non-zero on a model error or an error-severity test failure;
    # warn-severity tests (e.g. missing flight dates) still exit 0
    if result.returncode != 0:
        raise RuntimeError(f"dbt build failed (exit code {result.returncode})")


@flow(name="transform")
def transform(select: str | None = None) -> None:
    run_dbt(select)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the dbt transformations")
    parser.add_argument("--select", help="build one layer only, e.g. staging")
    args = parser.parse_args()
    transform(args.select)
