from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="fintech_transaction_pipeline",
    description="Synthetic transaction generation -> contract validation -> Silver Parquet -> analytics mart",
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["fintech", "data-contract", "parquet", "dbt"],
) as dag:
    generate = BashOperator(
        task_id="generate_synthetic_events",
        bash_command="python -m src.generate_transactions --count 10000 --output data/incoming/transactions.jsonl",
    )

    validate_and_materialize = BashOperator(
        task_id="contract_to_silver",
        bash_command="python -m src.pipeline data/incoming/transactions.jsonl",
    )

    build_local_mart = BashOperator(
        task_id="build_local_mart",
        bash_command="python -m src.build_mart",
    )

    dbt_quality = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "mkdir -p .dbt && cp dbt/profiles.yml.example .dbt/profiles.yml && "
            "cd dbt && dbt build --profiles-dir ../.dbt"
        ),
    )

    generate >> validate_and_materialize >> build_local_mart >> dbt_quality
