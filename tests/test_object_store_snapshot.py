from __future__ import annotations

import base64
from dataclasses import replace
import hashlib
import io
import json
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError
import pytest

import src.object_store as object_store
from src.object_store import (
    SnapshotPolicy,
    SnapshotPublicationError,
    publish_snapshot,
    sync_directory,
)


def client_error(code: str, status: int, operation: str) -> ClientError:
    return ClientError(
        {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}},
        operation,
    )


class FakeS3:
    def __init__(self, *, fail_after: int | None = None) -> None:
        self.objects: dict[tuple[str, str], dict[str, Any]] = {}
        self.operations: list[tuple[str, str]] = []
        self.fail_after = fail_after
        self.put_count = 0
        self.buckets = {"fintech-lakehouse"}

    def list_buckets(self) -> dict[str, Any]:
        return {"Buckets": [{"Name": name} for name in sorted(self.buckets)]}

    def create_bucket(self, *, Bucket: str) -> dict[str, Any]:
        self.buckets.add(Bucket)
        return {}

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        self.operations.append(("head", Key))
        try:
            item = self.objects[(Bucket, Key)]
        except KeyError as exc:
            raise client_error("NoSuchKey", 404, "HeadObject") from exc
        return {"ContentLength": len(item["body"]), "Metadata": dict(item["metadata"])}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        self.operations.append(("get", Key))
        try:
            body = self.objects[(Bucket, Key)]["body"]
        except KeyError as exc:
            raise client_error("NoSuchKey", 404, "GetObject") from exc
        return {"Body": io.BytesIO(body)}

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        key = (kwargs["Bucket"], kwargs["Key"])
        self.operations.append(("put", kwargs["Key"]))
        self.put_count += 1
        if self.fail_after is not None and self.put_count > self.fail_after:
            raise client_error("ServiceUnavailable", 503, "PutObject")
        if key in self.objects and kwargs.get("IfNoneMatch") == "*":
            raise client_error("PreconditionFailed", 412, "PutObject")
        body = kwargs["Body"]
        data = body.read() if hasattr(body, "read") else bytes(body)
        assert len(data) == kwargs["ContentLength"]
        assert kwargs["ChecksumSHA256"] == base64.b64encode(hashlib.sha256(data).digest()).decode(
            "ascii"
        )
        self.objects[key] = {
            "body": data,
            "metadata": dict(kwargs["Metadata"]),
            "content_type": kwargs["ContentType"],
        }
        return {}


class RacingS3(FakeS3):
    def __init__(self) -> None:
        super().__init__()
        self.raced = False

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        if not self.raced and "/objects/sha256/" in kwargs["Key"]:
            self.raced = True
            body = kwargs["Body"]
            data = body.read() if hasattr(body, "read") else bytes(body)
            assert kwargs["ChecksumSHA256"] == base64.b64encode(
                hashlib.sha256(data).digest()
            ).decode("ascii")
            self.objects[(kwargs["Bucket"], kwargs["Key"])] = {
                "body": data,
                "metadata": dict(kwargs["Metadata"]),
                "content_type": kwargs["ContentType"],
            }
            raise client_error("PreconditionFailed", 412, "PutObject")
        return super().put_object(**kwargs)


def write_snapshot(root: Path, entries: dict[str, bytes]) -> None:
    for relative, content in entries.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)


def manifest(store: FakeS3, publication) -> dict[str, Any]:
    raw = store.objects[(publication.bucket, publication.manifest_key)]["body"]
    return json.loads(raw)


def test_manifest_is_published_after_every_content_object(tmp_path: Path) -> None:
    root = tmp_path / "silver"
    write_snapshot(root, {"event_date=2026-01-01/a.parquet": b"a", "b.parquet": b"bb"})
    store = FakeS3()

    result = publish_snapshot(
        root,
        "fintech-lakehouse",
        source_revision="git:abc1234",
        s3=store,
    )

    puts = [key for operation, key in store.operations if operation == "put"]
    assert puts[-1] == result.manifest_key
    assert all("/objects/sha256/" in key for key in puts[:-1])
    assert result.uploaded_objects == 2
    assert result.reused_objects == 0


def test_manifest_binds_paths_sizes_digests_policy_and_revision(tmp_path: Path) -> None:
    root = tmp_path / "silver"
    write_snapshot(root, {"z.parquet": b"z", "a.parquet": b"aaa"})
    store = FakeS3()

    result = publish_snapshot(
        root,
        "fintech-lakehouse",
        source_revision="sha256:build-20260930",
        dataset="risk_features",
        s3=store,
    )
    document = manifest(store, result)

    assert document["source_revision"] == "sha256:build-20260930"
    assert document["dataset"] == "risk_features"
    assert document["file_count"] == 2
    assert document["total_bytes"] == 4
    assert [item["logical_path"] for item in document["files"]] == ["a.parquet", "z.parquet"]
    assert document["snapshot_sha256"] == result.snapshot_sha256
    assert document["policy"] == SnapshotPolicy().as_dict()


def test_exact_replay_reuses_objects_and_manifest(tmp_path: Path) -> None:
    root = tmp_path / "silver"
    write_snapshot(root, {"a.parquet": b"same"})
    store = FakeS3()
    first = publish_snapshot(root, "fintech-lakehouse", source_revision="git:abc", s3=store)
    puts_after_first = store.put_count

    replay = publish_snapshot(root, "fintech-lakehouse", source_revision="git:abc", s3=store)

    assert replay == replace(first, uploaded_objects=0, reused_objects=1)
    assert store.put_count == puts_after_first


def test_duplicate_content_uploads_one_object_but_keeps_two_logical_files(tmp_path: Path) -> None:
    root = tmp_path / "silver"
    write_snapshot(root, {"day=1/a.parquet": b"same", "day=2/b.parquet": b"same"})
    store = FakeS3()

    result = publish_snapshot(root, "fintech-lakehouse", source_revision="git:abc", s3=store)

    assert result.file_count == 2
    assert result.unique_object_count == 1
    assert result.uploaded_objects == 1
    document = manifest(store, result)
    assert document["files"][0]["object_key"] == document["files"][1]["object_key"]


def test_conditional_write_race_reuses_verified_winner(tmp_path: Path) -> None:
    root = tmp_path / "silver"
    write_snapshot(root, {"a.parquet": b"same"})

    result = publish_snapshot(root, "fintech-lakehouse", source_revision="git:abc", s3=RacingS3())

    assert result.uploaded_objects == 0
    assert result.reused_objects == 1


def test_partial_failure_never_writes_commit_manifest(tmp_path: Path) -> None:
    root = tmp_path / "silver"
    write_snapshot(root, {"a.parquet": b"a", "b.parquet": b"b"})
    store = FakeS3(fail_after=1)

    with pytest.raises(SnapshotPublicationError, match="OBJECT_STORE_PUT_FAILED"):
        publish_snapshot(root, "fintech-lakehouse", source_revision="git:abc", s3=store)

    assert all("/manifests/" not in key for _, key in store.objects)
    assert len(store.objects) == 1


def test_existing_object_with_wrong_integrity_metadata_fails_closed(tmp_path: Path) -> None:
    root = tmp_path / "silver"
    write_snapshot(root, {"a.parquet": b"payload"})
    store = FakeS3()
    first = publish_snapshot(root, "fintech-lakehouse", source_revision="git:abc", s3=store)
    document = manifest(store, first)
    key = document["files"][0]["object_key"]
    store.objects[("fintech-lakehouse", key)]["metadata"]["sha256"] = "0" * 64

    with pytest.raises(SnapshotPublicationError, match="IMMUTABLE_OBJECT_COLLISION"):
        publish_snapshot(root, "fintech-lakehouse", source_revision="git:abc", s3=store)


def test_existing_manifest_body_is_verified_on_replay(tmp_path: Path) -> None:
    root = tmp_path / "silver"
    write_snapshot(root, {"a.parquet": b"payload"})
    store = FakeS3()
    result = publish_snapshot(root, "fintech-lakehouse", source_revision="git:abc", s3=store)
    item = store.objects[(result.bucket, result.manifest_key)]
    item["body"] = b"x" * len(item["body"])

    with pytest.raises(SnapshotPublicationError, match="IMMUTABLE_MANIFEST_COLLISION"):
        publish_snapshot(root, "fintech-lakehouse", source_revision="git:abc", s3=store)


def test_creation_order_does_not_change_snapshot_identity(tmp_path: Path) -> None:
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    write_snapshot(first_root, {"z.parquet": b"z", "a.parquet": b"a"})
    write_snapshot(second_root, {"a.parquet": b"a", "z.parquet": b"z"})

    first = publish_snapshot(
        first_root, "fintech-lakehouse", source_revision="git:abc", s3=FakeS3()
    )
    second = publish_snapshot(
        second_root, "fintech-lakehouse", source_revision="git:abc", s3=FakeS3()
    )

    assert first.snapshot_sha256 == second.snapshot_sha256
    assert first.manifest_sha256 == second.manifest_sha256


def test_file_mutation_after_scan_is_rejected_before_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "silver"
    path = root / "a.parquet"
    write_snapshot(root, {"a.parquet": b"before"})
    original_scan = object_store._scan_snapshot

    def scan_then_mutate(*args, **kwargs):
        scanned = original_scan(*args, **kwargs)
        path.write_bytes(b"after!")
        return scanned

    monkeypatch.setattr(object_store, "_scan_snapshot", scan_then_mutate)
    store = FakeS3()

    with pytest.raises(SnapshotPublicationError, match="FILE_CHANGED_DURING_PUBLICATION"):
        publish_snapshot(root, "fintech-lakehouse", source_revision="git:abc", s3=store)

    assert store.put_count == 0


@pytest.mark.parametrize("revision", ["main", "HEAD", "latest", "bad revision", ""])
def test_mutable_or_malformed_source_revision_is_rejected(tmp_path: Path, revision: str) -> None:
    root = tmp_path / "silver"
    write_snapshot(root, {"a.parquet": b"a"})

    with pytest.raises(SnapshotPublicationError, match="INVALID_SOURCE_REVISION"):
        publish_snapshot(root, "fintech-lakehouse", source_revision=revision, s3=FakeS3())


@pytest.mark.parametrize(
    ("bucket", "prefix", "dataset", "code"),
    [
        ("BAD_BUCKET", "silver/transactions", "transactions", "INVALID_BUCKET"),
        ("fintech-lakehouse", "../silver", "transactions", "INVALID_PREFIX"),
        ("fintech-lakehouse", "silver//transactions", "transactions", "INVALID_PREFIX"),
        ("fintech-lakehouse", "silver/transactions", "bad dataset", "INVALID_DATASET"),
    ],
)
def test_namespace_inputs_are_validated(
    tmp_path: Path, bucket: str, prefix: str, dataset: str, code: str
) -> None:
    root = tmp_path / "silver"
    write_snapshot(root, {"a.parquet": b"a"})

    with pytest.raises(SnapshotPublicationError, match=code):
        publish_snapshot(
            root,
            bucket,
            prefix=prefix,
            dataset=dataset,
            source_revision="git:abc",
            s3=FakeS3(),
        )


def test_empty_or_missing_snapshot_fails_closed(tmp_path: Path) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(SnapshotPublicationError, match="EMPTY_SNAPSHOT"):
        publish_snapshot(empty, "fintech-lakehouse", source_revision="git:abc", s3=FakeS3())
    with pytest.raises(SnapshotPublicationError, match="INVALID_SNAPSHOT_ROOT"):
        publish_snapshot(
            tmp_path / "missing",
            "fintech-lakehouse",
            source_revision="git:abc",
            s3=FakeS3(),
        )


def test_symlinked_parquet_is_rejected(tmp_path: Path) -> None:
    root = tmp_path / "silver"
    root.mkdir()
    outside = tmp_path / "outside.parquet"
    outside.write_bytes(b"outside")
    (root / "linked.parquet").symlink_to(outside)

    with pytest.raises(SnapshotPublicationError, match="UNSAFE_SNAPSHOT_ENTRY"):
        publish_snapshot(root, "fintech-lakehouse", source_revision="git:abc", s3=FakeS3())


def test_file_count_file_size_and_total_size_budgets(tmp_path: Path) -> None:
    root = tmp_path / "silver"
    write_snapshot(root, {"a.parquet": b"aa", "b.parquet": b"bb"})

    with pytest.raises(SnapshotPublicationError, match="FILE_COUNT_BUDGET_EXCEEDED"):
        publish_snapshot(
            root,
            "fintech-lakehouse",
            source_revision="git:abc",
            s3=FakeS3(),
            policy=SnapshotPolicy(max_files=1),
        )
    with pytest.raises(SnapshotPublicationError, match="FILE_BYTE_BUDGET_EXCEEDED"):
        publish_snapshot(
            root,
            "fintech-lakehouse",
            source_revision="git:abc",
            s3=FakeS3(),
            policy=SnapshotPolicy(max_file_bytes=1, max_total_bytes=4),
        )
    with pytest.raises(SnapshotPublicationError, match="TOTAL_BYTE_BUDGET_EXCEEDED"):
        publish_snapshot(
            root,
            "fintech-lakehouse",
            source_revision="git:abc",
            s3=FakeS3(),
            policy=SnapshotPolicy(max_file_bytes=2, max_total_bytes=3),
        )


def test_manifest_budget_is_enforced_before_any_upload(tmp_path: Path) -> None:
    root = tmp_path / "silver"
    write_snapshot(root, {f"partition-{index:03d}.parquet": b"x" for index in range(10)})
    store = FakeS3()

    with pytest.raises(SnapshotPublicationError, match="MANIFEST_BYTE_BUDGET_EXCEEDED"):
        publish_snapshot(
            root,
            "fintech-lakehouse",
            source_revision="git:abc",
            s3=store,
            policy=SnapshotPolicy(max_manifest_bytes=256),
        )

    assert store.put_count == 0


def test_create_bucket_is_explicit(tmp_path: Path) -> None:
    root = tmp_path / "silver"
    write_snapshot(root, {"a.parquet": b"a"})
    store = FakeS3()
    store.buckets.clear()

    publish_snapshot(
        root,
        "fintech-lakehouse",
        source_revision="git:abc",
        s3=store,
        create_bucket=True,
    )

    assert "fintech-lakehouse" in store.buckets


def test_compatibility_wrapper_requires_explicit_revision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SOURCE_REVISION", raising=False)

    with pytest.raises(SnapshotPublicationError, match="SOURCE_REVISION_REQUIRED"):
        sync_directory(tmp_path, "fintech-lakehouse")
