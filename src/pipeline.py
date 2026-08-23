from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq

from src.validate_event import idempotency_key, validate_event


@dataclass(frozen=True)
class PipelineStats:
    received: int
    valid: int
    invalid: int
    duplicates: int
    written: int
    bronze_path: str
    silver_path: str


def read_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                yield json.loads(stripped)
            except json.JSONDecodeError as exc:
                yield {"_parse_error": f"line {line_number}: {exc.msg}"}


def _partition_day(event_time: str) -> str:
    parsed = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
    return parsed.astimezone(timezone.utc).date().isoformat()


def ingest(input_path: Path, data_root: Path = Path("data")) -> PipelineStats:
    bronze_root = data_root / "bronze" / "transactions"
    silver_root = data_root / "silver" / "transactions"
    quarantine_root = data_root / "quarantine" / "transactions"
    bronze_root.mkdir(parents=True, exist_ok=True)
    silver_root.mkdir(parents=True, exist_ok=True)
    quarantine_root.mkdir(parents=True, exist_ok=True)

    received = valid = invalid = duplicates = 0
    seen: set[str] = set()
    valid_rows: list[dict[str, Any]] = []
    invalid_rows: list[dict[str, Any]] = []

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    bronze_path = bronze_root / f"run={run_id}.jsonl"

    with bronze_path.open("w", encoding="utf-8") as bronze:
        for event in read_jsonl(input_path):
            received += 1
            bronze.write(json.dumps(event, separators=(",", ":"), ensure_ascii=False) + "\n")
            if "_parse_error" in event:
                invalid += 1
                invalid_rows.append({"event": event, "errors": [event["_parse_error"]]})
                continue

            errors = validate_event(event)
            if errors:
                invalid += 1
                invalid_rows.append({"event": event, "errors": errors})
                continue

            key = idempotency_key(event)
            if key in seen:
                duplicates += 1
                continue
            seen.add(key)
            valid += 1
            row = dict(event)
            row["event_date"] = _partition_day(str(event["event_time"]))
            row["ingestion_run_id"] = run_id
            valid_rows.append(row)

    if invalid_rows:
        quarantine_path = quarantine_root / f"run={run_id}.jsonl"
        with quarantine_path.open("w", encoding="utf-8") as handle:
            for row in invalid_rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    if valid_rows:
        table = pa.Table.from_pylist(valid_rows)
        pq.write_to_dataset(
            table,
            root_path=str(silver_root),
            partition_cols=["event_date"],
            existing_data_behavior="overwrite_or_ignore",
        )

    return PipelineStats(
        received=received,
        valid=valid,
        invalid=invalid,
        duplicates=duplicates,
        written=len(valid_rows),
        bronze_path=str(bronze_path),
        silver_path=str(silver_root),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate, quarantine and materialize fintech transaction events.")
    parser.add_argument("input", type=Path, nargs="?", default=Path("data/incoming/transactions.jsonl"))
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    args = parser.parse_args()
    stats = ingest(args.input, args.data_root)
    print(json.dumps(stats.__dict__, indent=2))
    if stats.invalid:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
