"""
Prefect + dbt transformation flow using the prefect-dbt package
Starts from a local Parquet file → DuckDB raw table → dbt staging → dbt marts

Install dependencies:
    pip install prefect prefect-dbt dbt-core dbt-duckdb duckdb pandas pyarrow requests
"""

import os
from pathlib import Path
from typing import Optional
from datetime import timedelta

import duckdb
import pandas as pd
import requests
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings


_REPO_ROOT = Path(__file__).parent.resolve()


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
# Task 3: Push mart_customers__braze_attributes to Braze /users/track
# ---------------------------------------------------------------------------

BRAZE_BATCH_SIZE = 75  # Braze /users/track hard limit per request

@task(retries=2, retry_delay_seconds=10, log_prints=True)
def push_to_braze(db_path: str) -> None:
    logger = get_run_logger()


# Don't have real credentials so this is only scaffolding 
    api_key = os.environ.get("BRAZE_API_KEY")
    rest_endpoint = os.environ.get("BRAZE_REST_ENDPOINT")

    if not api_key:
        raise EnvironmentError("BRAZE_API_KEY environment variable is not set")
    if not rest_endpoint:
        raise EnvironmentError("BRAZE_REST_ENDPOINT environment variable is not set")

    url = f"{rest_endpoint.rstrip('/')}/users/track"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    conn = duckdb.connect(db_path)
    rows = conn.execute("""
        SELECT
            external_id,
            email,
            first_name,
            city,
            state,
            country,
            child_age
        FROM mart_customers__braze_attributes
    """).fetchall()
    conn.close()

    columns = ["external_id", "email", "first_name", "city", "state", "country", "child_age"]
    total_rows = len(rows)
    logger.info(f"Fetched {total_rows:,} rows from mart_customers__braze_attributes")

    def row_to_attribute(row: tuple) -> dict:
        record = dict(zip(columns, row))
        attr: dict = {
            "external_id": record["external_id"],
            "email":        record["email"],
            "first_name":   record["first_name"],
            "home_city":    record["city"],
            "state":        record["state"],
            "country":      record["country"],
        }
        # child_age is the only custom attribute — omit if null (~10% of records)
        if record["child_age"] is not None:
            attr["child_age"] = int(record["child_age"])
        return attr

    success_count = 0
    failure_count = 0

    for batch_start in range(0, total_rows, BRAZE_BATCH_SIZE):
        batch_rows = rows[batch_start : batch_start + BRAZE_BATCH_SIZE]
        payload = {"attributes": [row_to_attribute(r) for r in batch_rows]}

        response = requests.post(url, json=payload, headers=headers, timeout=30)

        if 200 <= response.status_code < 300:
            success_count += len(batch_rows)
            logger.info(
                f"Batch {batch_start // BRAZE_BATCH_SIZE + 1}: "
                f"{len(batch_rows)} records accepted (HTTP {response.status_code})"
            )
        else:
            failure_count += len(batch_rows)
            logger.error(
                f"Batch {batch_start // BRAZE_BATCH_SIZE + 1} failed: "
                f"HTTP {response.status_code} — {response.text}"
            )
            response.raise_for_status()

    logger.info(f"Braze sync complete: {success_count:,} succeeded, {failure_count:,} failed")


# ---------------------------------------------------------------------------
# Flow: wire tasks together in dependency order
# ---------------------------------------------------------------------------

@flow(name="customer-transform-flow", log_prints=True)
def transform_flow(
    parquet_path: str = str(_REPO_ROOT / "synth_file_generation" / "customers_raw.parquet"),
    db_path: str = os.environ.get("DBT_DEV_DB_PATH", str(_REPO_ROOT / "dev.duckdb")),
    raw_table: str = "raw_customers",
    project_dir: str = str(_REPO_ROOT / "dob_holiday"),
    profiles_dir: str = str(Path.home() / ".dbt"),
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

    # Step 3: intermediate layer — enriched customer attributes
    run_dbt_command("run", project_dir, profiles_dir, select="intermediate")
    run_dbt_command("test", project_dir, profiles_dir, select="intermediate")

    # Step 4: marts layer — shaped for Braze
    run_dbt_command("run", project_dir, profiles_dir, select="marts")
    run_dbt_command("test", project_dir, profiles_dir, select="marts")

    # Step 5: push marts table to Braze /users/track
    push_to_braze(db_path=db_path)


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
