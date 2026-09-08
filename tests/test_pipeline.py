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


def test_exact_file_replay_does_not_append_parquet_rows(tmp_path: Path) -> None:
    incoming = tmp_path / "incoming.jsonl"
    events = generate_events(12, seed=31)
    write_jsonl(events, incoming)
    data_root = tmp_path / "data"

    first = ingest(incoming, data_root)
    parquet_before = sorted((data_root / "silver").rglob("*.parquet"))
    replay = ingest(incoming, data_root)
    parquet_after = sorted((data_root / "silver").rglob("*.parquet"))

    assert first.written == 12
    assert replay.replayed_run is True
    assert replay.attempt == 2
    assert replay.written == 0
    assert replay.duplicates == 12
    assert parquet_after == parquet_before
    assert len(list((data_root / "manifests").rglob("*.json"))) == 2
    assert build_mart(data_root / "silver" / "transactions", tmp_path / "mart.db")[
        "fact_transactions"
    ] == 12


def test_overlapping_batch_deduplicates_by_business_event_id(tmp_path: Path) -> None:
    first_path = tmp_path / "first.jsonl"
    second_path = tmp_path / "second.jsonl"
    events = generate_events(8, seed=44)
    write_jsonl(events[:5], first_path)
    write_jsonl(events[3:], second_path)
    data_root = tmp_path / "data"

    first = ingest(first_path, data_root)
    second = ingest(second_path, data_root)

    assert first.written == 5
    assert second.written == 3
    assert second.duplicates == 2
    counts = build_mart(data_root / "silver" / "transactions", tmp_path / "mart.db")
    assert counts["fact_transactions"] == 8


def test_mutated_payload_for_existing_event_id_is_quarantined(tmp_path: Path) -> None:
    import json

    first_path = tmp_path / "first.jsonl"
    conflict_path = tmp_path / "conflict.jsonl"
    event = asdict(generate_events(1, seed=55)[0])
    first_path.write_text(json.dumps(event) + "\n", encoding="utf-8")
    ingest(first_path, tmp_path / "data")
    event["amount"] += 100
    conflict_path.write_text(json.dumps(event) + "\n", encoding="utf-8")

    result = ingest(conflict_path, tmp_path / "data")

    assert result.written == 0
    assert result.conflicts == 1
    quarantine = list((tmp_path / "data" / "quarantine").rglob("*.jsonl"))
    assert len(quarantine) == 1
    assert "event_id_payload_conflict" in quarantine[0].read_text(encoding="utf-8")


def test_same_batch_payload_collision_is_not_counted_as_duplicate(tmp_path: Path) -> None:
    import json

    incoming = tmp_path / "collision.jsonl"
    event = asdict(generate_events(1, seed=66)[0])
    mutated = event | {"amount": event["amount"] + 1}
    incoming.write_text(
        json.dumps(event) + "\n" + json.dumps(mutated) + "\n", encoding="utf-8"
    )

    result = ingest(incoming, tmp_path / "data")

    assert result.received == 2
    assert result.written == 1
    assert result.duplicates == 0
    assert result.conflicts == 1
    quarantine = next((tmp_path / "data" / "quarantine").rglob("*.jsonl"))
    assert "in_batch_event_id_payload_conflict" in quarantine.read_text(encoding="utf-8")
