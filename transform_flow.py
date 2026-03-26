"""
Prefect + dbt transformation flow using the prefect-dbt package
Starts from a local Parquet file → DuckDB raw table → dbt staging → dbt marts

Install dependencies:
    pip install prefect prefect-dbt dbt-core dbt-duckdb duckdb pandas pyarrow
"""

from pathlib import Path
from typing import Optional
from datetime import timedelta

import duckdb
import pandas as pd
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings


# ---------------------------------------------------------------------------
# Task 1: Load Parquet → DuckDB raw table
# ---------------------------------------------------------------------------

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def load_parquet_to_duckdb(
    parquet_path: str,
    db_path: str,
    table_name: str = "raw_customers",
) -> None:
    logger = get_run_logger()
    parquet_path = Path(parquet_path)

    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

    df = pd.read_parquet(parquet_path)
    logger.info(f"Loaded {len(df):,} rows from {parquet_path}")

    conn = duckdb.connect(db_path)
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")

    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    logger.info(f"Written {count:,} rows → {db_path} :: {table_name}")
    conn.close()


# ---------------------------------------------------------------------------
# Task 2: Run dbt commands via PrefectDbtRunner
# ---------------------------------------------------------------------------

@task(retries=2, retry_delay_seconds=5, log_prints=True)
def run_dbt_command(
    command: str,
    project_dir: str,
    profiles_dir: str,
    select: Optional[str] = None,
) -> None:
    """
    Execute a dbt command using PrefectDbtRunner for native Prefect integration.
    .invoke() mirrors dbt Core's DbtRunner API — each node surfaces as its own
    task in the Prefect UI with per-node logs and state.
    """
    logger = get_run_logger()

    settings = PrefectDbtSettings(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
    )

    runner = PrefectDbtRunner(settings=settings)

    args = [command]
    if select:
        args += ["--select", select]

    logger.info(f"Running: dbt {' '.join(args)}")
    runner.invoke(args)  # .invoke() not .run()


# ---------------------------------------------------------------------------
# Flow: wire tasks together in dependency order
# ---------------------------------------------------------------------------

@flow(name="customer-transform-flow", log_prints=True)
def transform_flow(
    parquet_path: str = "./synth_file_generation/customers_raw.parquet",
    db_path: str = "./pipeline.duckdb",
    raw_table: str = "raw_customers",
    project_dir: str = "./dob_holiday",
    profiles_dir: str = "../../.dbt",
) -> None:
    # Step 1: land raw Parquet data into DuckDB
    load_parquet_to_duckdb(
        parquet_path=parquet_path,
        db_path=db_path,
        table_name=raw_table,
    )

    # Step 2: staging layer — clean, type-cast, dedupe
    run_dbt_command("run", project_dir, profiles_dir, select="staging")
    run_dbt_command("test", project_dir, profiles_dir, select="staging")

    # Step 3: marts layer — segments and computed attributes for Braze
    run_dbt_command("run", project_dir, profiles_dir, select="marts")
    run_dbt_command("test", project_dir, profiles_dir, select="marts")


# ---------------------------------------------------------------------------
# Optional: serve with a nightly schedule
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # One-off run
    transform_flow()

    # Uncomment to serve on a schedule instead:
    # transform_flow.serve(
    #     name="nightly-transform",
    #     interval=timedelta(hours=24),
    # )