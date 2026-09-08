from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3
import threading


SCHEMA = """
CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id TEXT PRIMARY KEY,
    input_sha256 TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('staging', 'committed', 'failed')),
    received INTEGER NOT NULL DEFAULT 0,
    accepted INTEGER NOT NULL DEFAULT 0,
    duplicates INTEGER NOT NULL DEFAULT 0,
    conflicts INTEGER NOT NULL DEFAULT 0,
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    committed_at TEXT,
    failure_reason TEXT,
    attempts INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS event_ledger (
    event_id TEXT PRIMARY KEY,
    payload_sha256 TEXT NOT NULL,
    run_id TEXT NOT NULL REFERENCES ingestion_runs(run_id),
    event_date TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('reserved', 'committed')),
    committed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_event_run ON event_ledger(run_id, state);
CREATE TABLE IF NOT EXISTS event_conflicts (
    conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    existing_sha256 TEXT NOT NULL,
    incoming_sha256 TEXT NOT NULL,
    incoming_run_id TEXT NOT NULL,
    observed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


class ConcurrentIngestion(RuntimeError):
    """An event is currently reserved by another unfinished ingestion run."""


class RunIdentityConflict(RuntimeError):
    """A deterministic run ID was reused for different input bytes."""


@dataclass(frozen=True)
class CandidateEvent:
    event_id: str
    payload_sha256: str
    event_date: str


@dataclass(frozen=True)
class Reservation:
    accepted_ids: frozenset[str]
    conflicting_ids: frozenset[str]
    duplicates: int
    conflicts: int
    replayed_run: bool
    attempt: int


class IngestionLedger:
    """Persistent deduplication boundary for at-least-once file delivery."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        with self._connect() as connection:
            connection.executescript(SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA busy_timeout = 10000")
        return connection

    def reserve(
        self,
        *,
        run_id: str,
        input_sha256: str,
        received: int,
        candidates: list[CandidateEvent],
    ) -> Reservation:
        with self._lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            run = connection.execute(
                "SELECT input_sha256, status FROM ingestion_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is not None and run["input_sha256"] != input_sha256:
                connection.rollback()
                raise RunIdentityConflict(f"run {run_id} has a different input digest")
            if run is not None and run["status"] == "committed":
                connection.execute(
                    "UPDATE ingestion_runs SET attempts = attempts + 1 WHERE run_id = ?",
                    (run_id,),
                )
                attempt = connection.execute(
                    "SELECT attempts FROM ingestion_runs WHERE run_id = ?", (run_id,)
                ).fetchone()["attempts"]
                connection.commit()
                return Reservation(
                    frozenset(), frozenset(), len(candidates), 0, True, int(attempt)
                )

            connection.execute(
                """
                INSERT INTO ingestion_runs(run_id, input_sha256, status, received)
                VALUES (?, ?, 'staging', ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    status = 'staging', received = excluded.received, failure_reason = NULL,
                    attempts = ingestion_runs.attempts + 1
                """,
                (run_id, input_sha256, received),
            )
            accepted: set[str] = set()
            conflicting: set[str] = set()
            duplicates = conflicts = 0
            for candidate in candidates:
                row = connection.execute(
                    "SELECT payload_sha256, run_id, state FROM event_ledger WHERE event_id = ?",
                    (candidate.event_id,),
                ).fetchone()
                if row is None:
                    connection.execute(
                        """
                        INSERT INTO event_ledger(
                            event_id, payload_sha256, run_id, event_date, state
                        ) VALUES (?, ?, ?, ?, 'reserved')
                        """,
                        (
                            candidate.event_id,
                            candidate.payload_sha256,
                            run_id,
                            candidate.event_date,
                        ),
                    )
                    accepted.add(candidate.event_id)
                    continue
                if row["payload_sha256"] != candidate.payload_sha256:
                    conflicts += 1
                    conflicting.add(candidate.event_id)
                    connection.execute(
                        """
                        INSERT INTO event_conflicts(
                            event_id, existing_sha256, incoming_sha256, incoming_run_id
                        ) VALUES (?, ?, ?, ?)
                        """,
                        (
                            candidate.event_id,
                            row["payload_sha256"],
                            candidate.payload_sha256,
                            run_id,
                        ),
                    )
                    continue
                if row["state"] == "reserved" and row["run_id"] != run_id:
                    connection.rollback()
                    raise ConcurrentIngestion(
                        f"event {candidate.event_id} is reserved by run {row['run_id']}"
                    )
                if row["state"] == "reserved":
                    accepted.add(candidate.event_id)
                else:
                    duplicates += 1

            connection.execute(
                """
                UPDATE ingestion_runs SET accepted = ?, duplicates = ?, conflicts = ?
                WHERE run_id = ?
                """,
                (len(accepted), duplicates, conflicts, run_id),
            )
            attempt = connection.execute(
                "SELECT attempts FROM ingestion_runs WHERE run_id = ?", (run_id,)
            ).fetchone()["attempts"]
            connection.commit()
        return Reservation(
            frozenset(accepted),
            frozenset(conflicting),
            duplicates,
            conflicts,
            False,
            int(attempt),
        )

    def commit(self, run_id: str) -> None:
        with self._lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                UPDATE event_ledger SET state = 'committed', committed_at = CURRENT_TIMESTAMP
                WHERE run_id = ? AND state = 'reserved'
                """,
                (run_id,),
            )
            cursor = connection.execute(
                """
                UPDATE ingestion_runs SET status = 'committed', committed_at = CURRENT_TIMESTAMP
                WHERE run_id = ? AND status = 'staging'
                """,
                (run_id,),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise RuntimeError(f"run {run_id} is not staging")
            connection.commit()

    def abort(self, run_id: str, reason: str) -> None:
        with self._lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "DELETE FROM event_ledger WHERE run_id = ? AND state = 'reserved'", (run_id,)
            )
            connection.execute(
                """
                UPDATE ingestion_runs SET status = 'failed', failure_reason = ?
                WHERE run_id = ? AND status = 'staging'
                """,
                (reason[:1000], run_id),
            )
            connection.commit()

    def run_status(self, run_id: str) -> dict[str, object] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM ingestion_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        return dict(row) if row is not None else None
