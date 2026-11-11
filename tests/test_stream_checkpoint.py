from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest

from src.stream_checkpoint import (
    CheckpointContractError,
    CheckpointContractMismatch,
    StreamCheckpointContract,
    admit_local_checkpoint,
)


def contract(**changes) -> StreamCheckpointContract:
    values = {
        "query_name": "transaction_risk_features",
        "bootstrap_servers": "kafka-b:9092,kafka-a:9092",
        "topic": "transactions",
        "output_path": "s3a://features/customer-risk",
        "event_schema": {
            "type": "struct",
            "fields": [{"name": "event_id", "type": "string", "nullable": False}],
        },
        "watermark": "15 minutes",
        "window_duration": "5 minutes",
        "slide_duration": "1 minute",
        "deduplication_keys": ("event_id",),
        "partition_keys": ("window_start",),
    }
    values.update(changes)
    return StreamCheckpointContract.build(**values)


def test_contract_is_stable_and_omits_raw_infrastructure_names() -> None:
    first = contract()
    reordered = contract(bootstrap_servers="kafka-a:9092,kafka-b:9092")

    assert first == reordered
    assert first.contract_sha256 == reordered.contract_sha256
    serialized = first.canonical_bytes().decode()
    assert "kafka-a" not in serialized
    assert "transactions" not in serialized
    assert "s3a://" not in serialized
    assert len(first.contract_sha256) == 64


@pytest.mark.parametrize(
    "change",
    [
        {"topic": "transactions-v2"},
        {"output_path": "s3a://features-v2/customer-risk"},
        {"event_schema": {"type": "struct", "fields": []}},
        {"watermark": "20 minutes"},
        {"window_duration": "10 minutes"},
        {"deduplication_keys": ("customer_id",)},
    ],
)
def test_stateful_semantic_changes_produce_a_new_identity(change) -> None:
    assert contract(**change).contract_sha256 != contract().contract_sha256


def test_first_start_creates_contract_and_exact_restart_matches(tmp_path) -> None:
    checkpoint = tmp_path / "transaction-risk"
    expected = contract()

    created = admit_local_checkpoint(checkpoint, expected)
    matched = admit_local_checkpoint(checkpoint, expected)

    assert created.status == "created"
    assert matched.status == "matched"
    assert created.contract_sha256 == expected.contract_sha256
    persisted = checkpoint.with_name("transaction-risk.contract.json")
    assert StreamCheckpointContract.from_dict(json.loads(persisted.read_text())) == expected
    assert persisted.stat().st_mode & 0o077 == 0


def test_drift_fails_closed_without_replacing_persisted_contract(tmp_path) -> None:
    checkpoint = tmp_path / "transaction-risk"
    expected = contract()
    admit_local_checkpoint(checkpoint, expected)
    persisted = checkpoint.with_name("transaction-risk.contract.json")
    original = persisted.read_bytes()

    changed = replace(expected, watermark="30 minutes")
    with pytest.raises(CheckpointContractMismatch) as error:
        admit_local_checkpoint(checkpoint, changed)

    assert error.value.changed_fields == ("watermark",)
    assert persisted.read_bytes() == original


def test_existing_unbound_checkpoint_state_is_not_relabelled(tmp_path) -> None:
    checkpoint = tmp_path / "transaction-risk"
    checkpoint.mkdir()
    (checkpoint / "offsets").mkdir()

    with pytest.raises(CheckpointContractError, match="has no contract"):
        admit_local_checkpoint(checkpoint, contract())

    assert not checkpoint.with_name("transaction-risk.contract.json").exists()


def test_concurrent_first_start_has_one_creator_and_matching_restarts(tmp_path) -> None:
    checkpoint = tmp_path / "transaction-risk"
    expected = contract()

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(
            executor.map(lambda _: admit_local_checkpoint(checkpoint, expected), range(16))
        )

    statuses = [result.status for result in results]
    assert statuses.count("created") == 1
    assert statuses.count("matched") == 15


def test_duplicate_or_extended_contract_fields_are_rejected(tmp_path) -> None:
    checkpoint = tmp_path / "transaction-risk"
    persisted = checkpoint.with_name("transaction-risk.contract.json")
    persisted.write_text('{"schema_version":"x","schema_version":"y"}', encoding="utf-8")
    with pytest.raises(CheckpointContractError, match="duplicate"):
        admit_local_checkpoint(checkpoint, contract())

    value = contract().as_dict()
    value["unexpected"] = True
    persisted.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(CheckpointContractError, match="fields do not match"):
        admit_local_checkpoint(checkpoint, contract())


def test_symlink_and_remote_checkpoint_locations_fail_closed(tmp_path) -> None:
    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    checkpoint = tmp_path / "transaction-risk"
    persisted = checkpoint.with_name("transaction-risk.contract.json")
    persisted.symlink_to(target)

    with pytest.raises(CheckpointContractError, match="regular file"):
        admit_local_checkpoint(checkpoint, contract())
    with pytest.raises(CheckpointContractError, match="contract-store adapter"):
        admit_local_checkpoint("s3a://checkpoints/transaction-risk", contract())


def test_hardlinked_contract_is_rejected_after_publication_window(tmp_path) -> None:
    checkpoint = tmp_path / "transaction-risk"
    persisted = checkpoint.with_name("transaction-risk.contract.json")
    persisted.write_bytes(contract().canonical_bytes())
    os.link(persisted, tmp_path / "unexpected-hard-link.json")

    with pytest.raises(CheckpointContractError, match="regular file"):
        admit_local_checkpoint(checkpoint, contract())


@pytest.mark.parametrize(
    "change,match",
    [
        ({"bootstrap_servers": "kafka:9092,kafka:9092"}, "unique"),
        ({"watermark": "forever"}, "watermark"),
        ({"deduplication_keys": ()}, "deduplication_keys"),
        ({"event_schema": {"threshold": float("nan")}}, "canonical JSON"),
    ],
)
def test_invalid_contract_inputs_are_rejected(change, match) -> None:
    with pytest.raises(CheckpointContractError, match=match):
        contract(**change)
