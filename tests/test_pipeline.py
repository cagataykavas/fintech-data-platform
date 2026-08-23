from dataclasses import asdict
from pathlib import Path

import duckdb

from src.build_mart import build_mart
from src.generate_transactions import generate_events, write_jsonl
from src.pipeline import ingest
from src.validate_event import validate_event


def test_generated_events_satisfy_contract() -> None:
    events = generate_events(25, seed=7)
    assert all(validate_event(asdict(event)) == [] for event in events)


def test_pipeline_deduplicates_and_builds_mart(tmp_path: Path) -> None:
    incoming = tmp_path / "incoming.jsonl"
    events = generate_events(100, seed=11)
    write_jsonl(events, incoming)
    with incoming.open("a", encoding="utf-8") as handle:
        import json
        handle.write(json.dumps(asdict(events[0])) + "\n")

    stats = ingest(incoming, tmp_path / "data")
    assert stats.received == 101
    assert stats.valid == 100
    assert stats.duplicates == 1
    assert stats.invalid == 0

    database = tmp_path / "warehouse.duckdb"
    counts = build_mart(tmp_path / "data" / "silver" / "transactions", database)
    assert counts["fact_transactions"] == 100

    connection = duckdb.connect(str(database), read_only=True)
    try:
        total = connection.execute("SELECT COUNT(*) FROM customer_daily_activity").fetchone()[0]
        assert total > 0
    finally:
        connection.close()
