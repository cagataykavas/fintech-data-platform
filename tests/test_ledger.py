from pathlib import Path

import pytest

from src.ledger import CandidateEvent, ConcurrentIngestion, IngestionLedger


def candidate(event_id: str, payload: str = "hash-a") -> CandidateEvent:
    return CandidateEvent(event_id, payload, "2026-01-01")


def test_abort_releases_reservations_for_safe_retry(tmp_path: Path) -> None:
    ledger = IngestionLedger(tmp_path / "ledger.db")
    first = ledger.reserve(
        run_id="run-a",
        input_sha256="input-a",
        received=1,
        candidates=[candidate("evt-1")],
    )
    assert first.accepted_ids == {"evt-1"}
    ledger.abort("run-a", "synthetic write failure")

    retry = ledger.reserve(
        run_id="run-a",
        input_sha256="input-a",
        received=1,
        candidates=[candidate("evt-1")],
    )
    assert retry.accepted_ids == {"evt-1"}
    assert ledger.run_status("run-a")["status"] == "staging"


def test_other_run_cannot_steal_unfinished_reservation(tmp_path: Path) -> None:
    ledger = IngestionLedger(tmp_path / "ledger.db")
    ledger.reserve(
        run_id="run-a",
        input_sha256="input-a",
        received=1,
        candidates=[candidate("evt-1")],
    )

    with pytest.raises(ConcurrentIngestion):
        ledger.reserve(
            run_id="run-b",
            input_sha256="input-b",
            received=1,
            candidates=[candidate("evt-1")],
        )

    assert ledger.run_status("run-b") is None


def test_committed_event_distinguishes_replay_from_payload_conflict(tmp_path: Path) -> None:
    ledger = IngestionLedger(tmp_path / "ledger.db")
    ledger.reserve(
        run_id="run-a",
        input_sha256="input-a",
        received=1,
        candidates=[candidate("evt-1")],
    )
    ledger.commit("run-a")

    replay = ledger.reserve(
        run_id="run-b",
        input_sha256="input-b",
        received=1,
        candidates=[candidate("evt-1")],
    )
    conflict = ledger.reserve(
        run_id="run-c",
        input_sha256="input-c",
        received=1,
        candidates=[candidate("evt-1", "hash-b")],
    )

    assert replay.duplicates == 1 and replay.conflicts == 0
    assert conflict.conflicts == 1 and conflict.conflicting_ids == {"evt-1"}
