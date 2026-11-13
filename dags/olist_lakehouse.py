"""
### Olist e-commerce lakehouse

Daily pipeline:

1. **warmup**: start Spark once so the Delta/JDBC jars are downloaded before the parallel tasks.
2. **bronze**: one task per source table; incremental extract from Postgres into Delta.
3. **silver**: one task per table; clean, de-duplicate, quality-check, MERGE.
4. **gold**: rebuild the star schema (dims + fact_order_items).
5. **dbt_build**: build the analytics marts in DuckDB and run all dbt tests.

Each silver task waits only for its own bronze task, so tables move through
the layers independently.
"""

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, TaskGroup

from lakehouse.config import SOURCE_TABLES

DBT_BIN = os.getenv("DBT_BIN", "dbt")
PROJECT_DIR = "/opt/project"

default_args = {
    "owner": "mihir",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "cwd": PROJECT_DIR,
}

with DAG(
    dag_id="olist_lakehouse",
    description="Postgres -> bronze -> silver -> gold (Delta Lake) -> dbt marts",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    # Each task starts its own Spark driver; 2 at a time is comfortable on a laptop.
    max_active_tasks=2,
    default_args=default_args,
    doc_md=__doc__,
    tags=["lakehouse", "olist", "spark", "delta"],
) as dag:
    warmup = BashOperator(task_id="warmup", bash_command="python -m lakehouse.spark")

    with TaskGroup("bronze") as bronze:
        bronze_tasks = {
            t: BashOperator(task_id=t, bash_command=f"python -m lakehouse.jobs.bronze --table {t}")
            for t in SOURCE_TABLES
        }

    with TaskGroup("silver") as silver:
        silver_tasks = {
            t: BashOperator(task_id=t, bash_command=f"python -m lakehouse.jobs.silver --table {t}")
            for t in SOURCE_TABLES
        }

    for t in SOURCE_TABLES:
        bronze_tasks[t] >> silver_tasks[t]

    gold = BashOperator(task_id="gold", bash_command="python -m lakehouse.jobs.gold")

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT_BIN} build --profiles-dir . --project-dir .",
        cwd=f"{PROJECT_DIR}/dbt/olist",
    )

    warmup >> bronze
    silver >> gold >> dbt_build
