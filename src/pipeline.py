from __future__ import annotations

import argparse
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from src.ledger import CandidateEvent, IngestionLedger
from src.validate_event import idempotency_key, validate_event


@dataclass(frozen=True)
class PipelineStats:
    run_id: str
    attempt: int
    received: int
    valid: int
    invalid: int
    duplicates: int
    conflicts: int
    written: int
    replayed_run: bool
    bronze_path: str
    silver_path: str
    manifest_path: str


def read_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                parsed = json.loads(stripped)
                if not isinstance(parsed, dict):
                    yield {"_parse_error": f"line {line_number}: event must be an object"}
                else:
                    yield parsed
            except json.JSONDecodeError as exc:
                yield {"_parse_error": f"line {line_number}: {exc.msg}"}


def _partition_day(event_time: str) -> str:
    parsed = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
    return parsed.astimezone(UTC).date().isoformat()


def _atomic_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)


def _source_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value for key, value in row.items() if key not in {"event_date", "ingestion_run_id"}
    }


def _write_partitioned(rows: list[dict[str, Any]], root: Path, run_id: str) -> list[Path]:
    by_date: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_date.setdefault(str(row["event_date"]), []).append(row)

    published: list[Path] = []
    for event_date, partition_rows in sorted(by_date.items()):
        partition = root / f"event_date={event_date}"
        partition.mkdir(parents=True, exist_ok=True)
        destination = partition / f"run={run_id}.parquet"
        temporary = destination.with_suffix(".parquet.tmp")
        pq.write_table(pa.Table.from_pylist(partition_rows), temporary, compression="zstd")
        os.replace(temporary, destination)
        published.append(destination)
    return published


def ingest(input_path: Path, data_root: Path = Path("data")) -> PipelineStats:
    input_bytes = input_path.read_bytes()
    input_sha256 = hashlib.sha256(input_bytes).hexdigest()
    run_id = input_sha256[:20]
    bronze_root = data_root / "bronze" / "transactions"
    silver_root = data_root / "silver" / "transactions"
    quarantine_root = data_root / "quarantine" / "transactions"
    manifest_root = data_root / "manifests" / "transactions"
    ledger = IngestionLedger(data_root / "metadata" / "ingestion.db")

    bronze_path = bronze_root / f"run={run_id}.jsonl"
    _atomic_text(bronze_path, input_bytes.decode("utf-8"))

    received = invalid = in_batch_duplicates = in_batch_conflicts = 0
    valid_rows_by_id: dict[str, dict[str, Any]] = {}
    invalid_rows: list[dict[str, Any]] = []
    for event in read_jsonl(input_path):
        received += 1
        if "_parse_error" in event:
            invalid += 1
            invalid_rows.append({"event": event, "errors": [event["_parse_error"]]})
            continue
        errors = validate_event(event)
        if errors:
            invalid += 1
            invalid_rows.append({"event": event, "errors": errors})
            continue
        event_id = str(event["event_id"])
        if event_id in valid_rows_by_id:
            existing = _source_payload(valid_rows_by_id[event_id])
            if idempotency_key(existing) == idempotency_key(event):
                in_batch_duplicates += 1
            else:
                in_batch_conflicts += 1
                invalid_rows.append(
                    {"event": event, "errors": ["in_batch_event_id_payload_conflict"]}
                )
            continue
        row = dict(event)
        row["event_date"] = _partition_day(str(event["event_time"]))
        row["ingestion_run_id"] = run_id
        valid_rows_by_id[event_id] = row

    candidates = [
        CandidateEvent(
            event_id=event_id,
            payload_sha256=idempotency_key(_source_payload(row)),
            event_date=str(row["event_date"]),
        )
        for event_id, row in valid_rows_by_id.items()
    ]
    reservation = ledger.reserve(
        run_id=run_id,
        input_sha256=input_sha256,
        received=received,
        candidates=candidates,
    )
    accepted_rows = [
        row for event_id, row in valid_rows_by_id.items() if event_id in reservation.accepted_ids
    ]
    published: list[Path] = []
    try:
        if accepted_rows:
            published = _write_partitioned(accepted_rows, silver_root, run_id)
        if not reservation.replayed_run:
            ledger.commit(run_id)
    except Exception as exc:
        for path in published:
            path.unlink(missing_ok=True)
        ledger.abort(run_id, str(exc))
        raise

    if invalid_rows or reservation.conflicts or in_batch_conflicts:
        for event_id in sorted(reservation.conflicting_ids):
            if event_id in valid_rows_by_id:
                invalid_rows.append(
                    {
                        "event": _source_payload(valid_rows_by_id[event_id]),
                        "errors": ["event_id_payload_conflict"],
                    }
                )
        quarantine_path = quarantine_root / f"run={run_id}.jsonl"
        _atomic_text(
            quarantine_path,
            "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in invalid_rows),
        )

    stats = PipelineStats(
        run_id=run_id,
        attempt=reservation.attempt,
        received=received,
        valid=len(valid_rows_by_id),
        invalid=invalid,
        duplicates=in_batch_duplicates + reservation.duplicates,
        conflicts=in_batch_conflicts + reservation.conflicts,
        written=len(accepted_rows),
        replayed_run=reservation.replayed_run,
        bronze_path=str(bronze_path),
        silver_path=str(silver_root),
        manifest_path=str(
            manifest_root / f"run={run_id}" / f"attempt={reservation.attempt:04d}.json"
        ),
    )
    _atomic_text(
        Path(stats.manifest_path), json.dumps(asdict(stats), indent=2, sort_keys=True) + "\n"
    )
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay-safe transaction validation and lakehouse materialization."
    )
    parser.add_argument(
        "input", type=Path, nargs="?", default=Path("data/incoming/transactions.jsonl")
    )
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    args = parser.parse_args()
    stats = ingest(args.input, args.data_root)
    print(json.dumps(asdict(stats), indent=2))
    if stats.invalid or stats.conflicts:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
