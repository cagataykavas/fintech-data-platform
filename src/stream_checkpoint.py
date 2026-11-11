"""Bind stateful stream checkpoints to the semantics that produced them."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal


MAX_CONTRACT_BYTES = 16 * 1024
_IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,127}$")
_DURATION = re.compile(r"^[1-9][0-9]{0,5} (?:millisecond|second|minute|hour|day)s?$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class CheckpointContractError(ValueError):
    """Raised when checkpoint evidence is malformed or unsafe to process."""


class CheckpointContractMismatch(CheckpointContractError):
    """Raised when persisted state belongs to different stream semantics."""

    def __init__(self, changed_fields: tuple[str, ...]) -> None:
        self.changed_fields = changed_fields
        super().__init__(f"checkpoint contract mismatch: {', '.join(changed_fields)}")


def _canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise CheckpointContractError("contract value is not canonical JSON") from exc


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _bounded_text(value: Any, field: str, *, maximum: int = 1024) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise CheckpointContractError(f"invalid {field}")
    if value != value.strip() or any(ord(character) < 32 for character in value):
        raise CheckpointContractError(f"invalid {field}")
    return value


def _identifier(value: Any, field: str) -> str:
    text = _bounded_text(value, field, maximum=128)
    if not _IDENTIFIER.fullmatch(text):
        raise CheckpointContractError(f"invalid {field}")
    return text


def _duration(value: Any, field: str) -> str:
    text = _bounded_text(value, field, maximum=32).lower()
    if value != text or not _DURATION.fullmatch(text):
        raise CheckpointContractError(f"invalid {field}")
    return text


def _hash(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise CheckpointContractError(f"invalid {field}")
    return value


def _identifier_tuple(value: Any, field: str) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or not value or len(value) > 32:
        raise CheckpointContractError(f"invalid {field}")
    result = tuple(_identifier(item, field) for item in value)
    if len(result) != len(set(result)):
        raise CheckpointContractError(f"duplicate {field}")
    return result


@dataclass(frozen=True, slots=True)
class StreamCheckpointContract:
    """State-affecting stream semantics stored alongside a checkpoint."""

    schema_version: str
    query_name: str
    source_identity_sha256: str
    output_identity_sha256: str
    event_schema_sha256: str
    starting_offsets: str
    fail_on_data_loss: bool
    watermark: str
    window_duration: str
    slide_duration: str
    deduplication_keys: tuple[str, ...]
    output_mode: str
    partition_keys: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.schema_version != "stream-checkpoint-contract/v1":
            raise CheckpointContractError("unsupported checkpoint contract schema")
        _identifier(self.query_name, "query_name")
        _hash(self.source_identity_sha256, "source_identity_sha256")
        _hash(self.output_identity_sha256, "output_identity_sha256")
        _hash(self.event_schema_sha256, "event_schema_sha256")
        if self.starting_offsets not in {"earliest", "latest"}:
            raise CheckpointContractError("invalid starting_offsets")
        if not isinstance(self.fail_on_data_loss, bool):
            raise CheckpointContractError("fail_on_data_loss must be boolean")
        _duration(self.watermark, "watermark")
        _duration(self.window_duration, "window_duration")
        _duration(self.slide_duration, "slide_duration")
        deduplication_keys = _identifier_tuple(
            self.deduplication_keys, "deduplication_keys"
        )
        if deduplication_keys != self.deduplication_keys:
            raise CheckpointContractError("deduplication_keys must be canonical")
        if self.output_mode not in {"append", "complete", "update"}:
            raise CheckpointContractError("invalid output_mode")
        if _identifier_tuple(self.partition_keys, "partition_keys") != self.partition_keys:
            raise CheckpointContractError("partition_keys must be canonical")

    @classmethod
    def build(
        cls,
        *,
        query_name: str,
        bootstrap_servers: str,
        topic: str,
        output_path: str,
        event_schema: Any,
        watermark: str,
        window_duration: str,
        slide_duration: str,
        deduplication_keys: tuple[str, ...],
        partition_keys: tuple[str, ...],
        starting_offsets: str = "latest",
        fail_on_data_loss: bool = False,
        output_mode: str = "append",
    ) -> StreamCheckpointContract:
        bootstrap_text = _bounded_text(
            bootstrap_servers, "bootstrap_servers", maximum=8192
        )
        servers = [
            _bounded_text(item.strip(), "bootstrap server", maximum=255)
            for item in bootstrap_text.split(",")
        ]
        normalized_servers = sorted(server.lower() for server in servers)
        if len(servers) > 32 or len(servers) != len(set(normalized_servers)):
            raise CheckpointContractError("bootstrap servers must be unique and bounded")
        topic_name = _bounded_text(topic, "topic", maximum=249)
        output_location = _bounded_text(output_path, "output_path", maximum=2048)
        source_identity = {
            "bootstrap_servers": normalized_servers,
            "topic": topic_name,
        }
        return cls(
            schema_version="stream-checkpoint-contract/v1",
            query_name=_identifier(query_name, "query_name"),
            source_identity_sha256=_digest(source_identity),
            output_identity_sha256=_digest({"output_path": output_location}),
            event_schema_sha256=_digest(event_schema),
            starting_offsets=starting_offsets,
            fail_on_data_loss=fail_on_data_loss,
            watermark=_duration(watermark, "watermark"),
            window_duration=_duration(window_duration, "window_duration"),
            slide_duration=_duration(slide_duration, "slide_duration"),
            deduplication_keys=_identifier_tuple(
                deduplication_keys, "deduplication_keys"
            ),
            output_mode=output_mode,
            partition_keys=_identifier_tuple(partition_keys, "partition_keys"),
        )

    @classmethod
    def from_dict(cls, value: Any) -> StreamCheckpointContract:
        if not isinstance(value, dict):
            raise CheckpointContractError("checkpoint contract must be an object")
        expected = set(cls.__dataclass_fields__)
        if set(value) != expected:
            raise CheckpointContractError("checkpoint contract fields do not match schema")
        return cls(
            schema_version=value["schema_version"],
            query_name=value["query_name"],
            source_identity_sha256=value["source_identity_sha256"],
            output_identity_sha256=value["output_identity_sha256"],
            event_schema_sha256=value["event_schema_sha256"],
            starting_offsets=value["starting_offsets"],
            fail_on_data_loss=value["fail_on_data_loss"],
            watermark=value["watermark"],
            window_duration=value["window_duration"],
            slide_duration=value["slide_duration"],
            deduplication_keys=_identifier_tuple(
                value["deduplication_keys"], "deduplication_keys"
            ),
            output_mode=value["output_mode"],
            partition_keys=_identifier_tuple(value["partition_keys"], "partition_keys"),
        )

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["deduplication_keys"] = list(self.deduplication_keys)
        value["partition_keys"] = list(self.partition_keys)
        return value

    def canonical_bytes(self) -> bytes:
        return _canonical_json(self.as_dict())

    @property
    def contract_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()


@dataclass(frozen=True, slots=True)
class CheckpointContractEvidence:
    schema_version: str
    status: Literal["created", "matched"]
    contract_sha256: str

    def as_dict(self) -> dict[str, str]:
        return asdict(self)


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise CheckpointContractError("checkpoint contract contains duplicate fields")
        result[key] = value
    return result


def _reject_non_finite_constant(token: str) -> None:
    raise CheckpointContractError(f"non-finite JSON constant: {token}")


def _read_regular_file(path: Path) -> bytes:
    for attempt in range(20):
        try:
            path_stat = path.lstat()
        except OSError as exc:
            raise CheckpointContractError("checkpoint contract cannot be inspected") from exc
        if stat.S_ISREG(path_stat.st_mode) and path_stat.st_nlink == 1:
            break
        if stat.S_ISREG(path_stat.st_mode) and path_stat.st_nlink == 2 and attempt < 19:
            time.sleep(0.001)
            continue
        raise CheckpointContractError("checkpoint contract must be one regular file")
    if path_stat.st_size <= 0 or path_stat.st_size > MAX_CONTRACT_BYTES:
        raise CheckpointContractError("checkpoint contract violates byte budget")

    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise CheckpointContractError("checkpoint contract cannot be opened safely") from exc
    try:
        before = os.fstat(descriptor)
        if (before.st_dev, before.st_ino) != (path_stat.st_dev, path_stat.st_ino):
            raise CheckpointContractError("checkpoint contract changed before read")
        chunks: list[bytes] = []
        size = 0
        while True:
            chunk = os.read(descriptor, min(4096, MAX_CONTRACT_BYTES + 1 - size))
            if not chunk:
                break
            chunks.append(chunk)
            size += len(chunk)
            if size > MAX_CONTRACT_BYTES:
                raise CheckpointContractError("checkpoint contract violates byte budget")
        after = os.fstat(descriptor)
        identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        if identity_before != identity_after or size != after.st_size:
            raise CheckpointContractError("checkpoint contract changed during read")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _load_contract(path: Path) -> StreamCheckpointContract:
    raw = _read_regular_file(path)
    try:
        value = json.loads(
            raw,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_non_finite_constant,
        )
    except (CheckpointContractError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        if isinstance(exc, CheckpointContractError):
            raise
        raise CheckpointContractError("checkpoint contract is invalid JSON") from exc
    return StreamCheckpointContract.from_dict(value)


def _publish_once(path: Path, payload: bytes) -> bool:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError:
            return False
        temporary.unlink()
        directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        directory = os.open(path.parent, directory_flags)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return True
    finally:
        temporary.unlink(missing_ok=True)


def admit_local_checkpoint(
    checkpoint_location: str | Path,
    expected: StreamCheckpointContract,
) -> CheckpointContractEvidence:
    """Create or reconcile a local checkpoint contract without overwriting evidence."""

    location = _bounded_text(str(checkpoint_location), "checkpoint_location", maximum=2048)
    if "://" in location:
        raise CheckpointContractError(
            "non-local checkpoint locations require an atomic contract-store adapter"
        )
    checkpoint_path = Path(location)
    if not checkpoint_path.name:
        raise CheckpointContractError("checkpoint location must name one directory")
    contract_path = checkpoint_path.with_name(f"{checkpoint_path.name}.contract.json")
    contract_path.parent.mkdir(parents=True, exist_ok=True)
    if contract_path.parent.is_symlink():
        raise CheckpointContractError("checkpoint contract directory must not be a symlink")

    if not contract_path.exists() and checkpoint_path.exists():
        try:
            checkpoint_stat = checkpoint_path.lstat()
            has_state = next(checkpoint_path.iterdir(), None) is not None
        except OSError as exc:
            raise CheckpointContractError("existing checkpoint cannot be inspected") from exc
        if not stat.S_ISDIR(checkpoint_stat.st_mode) or checkpoint_path.is_symlink():
            raise CheckpointContractError("checkpoint location must be one regular directory")
        if has_state:
            raise CheckpointContractError("existing checkpoint state has no contract")

    created = _publish_once(contract_path, expected.canonical_bytes() + b"\n")
    if created:
        status: Literal["created", "matched"] = "created"
    else:
        observed = _load_contract(contract_path)
        if observed != expected:
            observed_values = observed.as_dict()
            expected_values = expected.as_dict()
            changed = tuple(
                sorted(
                    field
                    for field in expected_values
                    if observed_values[field] != expected_values[field]
                )
            )
            raise CheckpointContractMismatch(changed)
        status = "matched"
    return CheckpointContractEvidence(
        schema_version="checkpoint-contract-evidence/v1",
        status=status,
        contract_sha256=expected.contract_sha256,
    )
