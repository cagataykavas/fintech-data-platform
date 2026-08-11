from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="fintech_daily_features",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["fintech", "features", "dbt"],
) as dag:
    validate_raw = BashOperator(
        task_id="validate_raw_events",
        bash_command="python -m src.validate_event",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd dbt && dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd dbt && dbt test --profiles-dir .",
    )

    validate_raw >> dbt_run >> dbt_test
