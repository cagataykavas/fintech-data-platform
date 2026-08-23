from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb
import psycopg

DDL = """
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE TABLE IF NOT EXISTS analytics.customer_daily_activity (
    customer_id TEXT NOT NULL,
    event_date DATE NOT NULL,
    transaction_count BIGINT NOT NULL,
    gross_amount DOUBLE PRECISION NOT NULL,
    average_amount DOUBLE PRECISION NOT NULL,
    merchant_count BIGINT NOT NULL,
    country_count BIGINT NOT NULL,
    transfer_count BIGINT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (customer_id, event_date)
);
"""


def rows_from_duckdb(database: Path) -> list[tuple]:
    connection = duckdb.connect(str(database), read_only=True)
    try:
        return connection.execute(
            """
            SELECT customer_id, event_date, transaction_count, gross_amount,
                   average_amount, merchant_count, country_count, transfer_count
            FROM customer_daily_activity
            ORDER BY event_date, customer_id
            """
        ).fetchall()
    finally:
        connection.close()


def load(database: Path, dsn: str) -> int:
    rows = rows_from_duckdb(database)
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute("TRUNCATE analytics.customer_daily_activity")
            with cur.copy(
                """
                COPY analytics.customer_daily_activity (
                    customer_id, event_date, transaction_count, gross_amount,
                    average_amount, merchant_count, country_count, transfer_count
                ) FROM STDIN
                """
            ) as copy:
                for row in rows:
                    copy.write_row(row)
        conn.commit()
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load the customer daily mart into PostgreSQL.")
    parser.add_argument("--database", type=Path, default=Path("data/warehouse/fintech.duckdb"))
    parser.add_argument(
        "--dsn",
        default=os.getenv("POSTGRES_DSN", "postgresql://fintech:fintech@localhost:5432/fintech"),
    )
    args = parser.parse_args()
    print(f"loaded {load(args.database, args.dsn)} rows into PostgreSQL")


if __name__ == "__main__":
    main()
