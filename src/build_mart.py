from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


MART_SQL = """
CREATE OR REPLACE TABLE fact_transactions AS
SELECT
    event_id,
    customer_id,
    CAST(event_time AS TIMESTAMPTZ) AS event_time,
    amount,
    currency,
    merchant_id,
    country,
    channel,
    CAST(event_date AS DATE) AS event_date,
    ingestion_run_id
FROM read_parquet(?, hive_partitioning = true);

CREATE OR REPLACE TABLE customer_daily_activity AS
SELECT
    customer_id,
    event_date,
    COUNT(*) AS transaction_count,
    ROUND(SUM(amount), 2) AS gross_amount,
    ROUND(AVG(amount), 2) AS average_amount,
    COUNT(DISTINCT merchant_id) AS merchant_count,
    COUNT(DISTINCT country) AS country_count,
    SUM(CASE WHEN channel = 'bank_transfer' THEN 1 ELSE 0 END) AS transfer_count
FROM fact_transactions
GROUP BY 1, 2;

CREATE OR REPLACE VIEW high_velocity_customers AS
SELECT *
FROM customer_daily_activity
WHERE transaction_count >= 10 OR country_count >= 3 OR gross_amount >= 5000
ORDER BY gross_amount DESC;
"""


def build_mart(silver_root: Path, database: Path) -> dict[str, int]:
    parquet_glob = str(silver_root / "**" / "*.parquet")
    database.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(database))
    try:
        statements = [part.strip() for part in MART_SQL.split(";") if part.strip()]
        for index, statement in enumerate(statements):
            if index == 0:
                connection.execute(statement, [parquet_glob])
            else:
                connection.execute(statement)
        counts = {
            "fact_transactions": connection.execute("SELECT COUNT(*) FROM fact_transactions").fetchone()[0],
            "customer_daily_activity": connection.execute("SELECT COUNT(*) FROM customer_daily_activity").fetchone()[0],
            "high_velocity_customers": connection.execute("SELECT COUNT(*) FROM high_velocity_customers").fetchone()[0],
        }
    finally:
        connection.close()
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Build local analytics marts from Silver Parquet.")
    parser.add_argument("--silver-root", type=Path, default=Path("data/silver/transactions"))
    parser.add_argument("--database", type=Path, default=Path("data/warehouse/fintech.duckdb"))
    args = parser.parse_args()
    print(json.dumps(build_mart(args.silver_root, args.database), indent=2))


if __name__ == "__main__":
    main()
