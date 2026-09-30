from __future__ import annotations

import argparse
import base64
from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any, BinaryIO, Protocol

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError


SHA256 = re.compile(r"^[0-9a-f]{64}$")
BUCKET = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
REVISION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/+@-]{0,127}$")
MUTABLE_REVISIONS = {"head", "latest", "main", "master", "stable"}


class SnapshotPublicationError(RuntimeError):
    """A snapshot cannot be published without weakening its integrity contract."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class ObjectStore(Protocol):
    def head_object(self, **kwargs: Any) -> dict[str, Any]: ...

    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...

    def put_object(self, **kwargs: Any) -> dict[str, Any]: ...

    def list_buckets(self) -> dict[str, Any]: ...

    def create_bucket(self, **kwargs: Any) -> dict[str, Any]: ...


@dataclass(frozen=True)
class SnapshotPolicy:
    max_files: int = 10_000
    max_file_bytes: int = 128 * 1024 * 1024
    max_total_bytes: int = 2 * 1024 * 1024 * 1024
    max_manifest_bytes: int = 4 * 1024 * 1024

    def __post_init__(self) -> None:
        for name, value, lower, upper in (
            ("max_files", self.max_files, 1, 1_000_000),
            ("max_file_bytes", self.max_file_bytes, 1, 1024**4),
            ("max_total_bytes", self.max_total_bytes, 1, 1024**5),
            ("max_manifest_bytes", self.max_manifest_bytes, 256, 64 * 1024 * 1024),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or not lower <= value <= upper:
                raise ValueError(f"{name} must be between {lower} and {upper}")
        if self.max_file_bytes > self.max_total_bytes:
            raise ValueError("max_file_bytes cannot exceed max_total_bytes")

    def as_dict(self) -> dict[str, int]:
        return asdict(self)


@dataclass(frozen=True)
class SnapshotFile:
    logical_path: str
    object_key: str
    size_bytes: int
    sha256: str

    def as_dict(self) -> dict[str, str | int]:
        return asdict(self)


@dataclass(frozen=True)
class SnapshotPublication:
    bucket: str
    manifest_key: str
    snapshot_sha256: str
    manifest_sha256: str
    file_count: int
    unique_object_count: int
    total_bytes: int
    uploaded_objects: int
    reused_objects: int

    def as_dict(self) -> dict[str, str | int]:
        return asdict(self)


def client() -> ObjectStore:
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minio"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minio123"),
        region_name=os.getenv("AWS_REGION", "eu-central-1"),
        config=Config(signature_version="s3v4"),
    )


def ensure_bucket(s3: ObjectStore, bucket: str) -> None:
    _validate_bucket(bucket)
    existing = {item["Name"] for item in s3.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)


def _validate_bucket(bucket: str) -> None:
    if not isinstance(bucket, str) or not BUCKET.fullmatch(bucket):
        raise SnapshotPublicationError("INVALID_BUCKET")


def _validate_prefix(prefix: str) -> str:
    if not isinstance(prefix, str):
        raise SnapshotPublicationError("INVALID_PREFIX")
    normalized = prefix.strip("/")
    parts = normalized.split("/")
    if (
        not normalized
        or len(normalized) > 512
        or any(part in {"", ".", ".."} for part in parts)
        or any(any(ord(character) < 32 for character in part) for part in parts)
    ):
        raise SnapshotPublicationError("INVALID_PREFIX")
    return normalized


def _validate_revision(revision: str) -> None:
    if (
        not isinstance(revision, str)
        or not REVISION.fullmatch(revision)
        or revision.casefold() in MUTABLE_REVISIONS
    ):
        raise SnapshotPublicationError("INVALID_SOURCE_REVISION")


def _canonical_json(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError) as exc:
        raise SnapshotPublicationError("NON_CANONICAL_MANIFEST") from exc


def _sha256_file(path: Path, maximum: int) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            size += len(chunk)
            if size > maximum:
                raise SnapshotPublicationError("FILE_BYTE_BUDGET_EXCEEDED")
            digest.update(chunk)
    return digest.hexdigest(), size


def _verify_open_file(
    handle: BinaryIO, *, expected_digest: str, expected_size: int, maximum: int
) -> os.stat_result:
    before = os.fstat(handle.fileno())
    digest = hashlib.sha256()
    size = 0
    while chunk := handle.read(1024 * 1024):
        size += len(chunk)
        if size > maximum:
            raise SnapshotPublicationError("FILE_BYTE_BUDGET_EXCEEDED")
        digest.update(chunk)
    if size != expected_size or digest.hexdigest() != expected_digest:
        raise SnapshotPublicationError("FILE_CHANGED_DURING_PUBLICATION")
    handle.seek(0)
    return before


def _scan_snapshot(root: Path, prefix: str, policy: SnapshotPolicy) -> list[SnapshotFile]:
    if not isinstance(root, Path) or not root.is_dir() or root.is_symlink():
        raise SnapshotPublicationError("INVALID_SNAPSHOT_ROOT")
    resolved_root = root.resolve(strict=True)
    paths = sorted(root.rglob("*.parquet"), key=lambda value: value.relative_to(root).as_posix())
    if not paths:
        raise SnapshotPublicationError("EMPTY_SNAPSHOT")
    if len(paths) > policy.max_files:
        raise SnapshotPublicationError("FILE_COUNT_BUDGET_EXCEEDED")

    files: list[SnapshotFile] = []
    total = 0
    for path in paths:
        if path.is_symlink() or not path.is_file():
            raise SnapshotPublicationError("UNSAFE_SNAPSHOT_ENTRY")
        try:
            path.resolve(strict=True).relative_to(resolved_root)
        except (OSError, ValueError) as exc:
            raise SnapshotPublicationError("UNSAFE_SNAPSHOT_ENTRY") from exc
        relative = path.relative_to(root).as_posix()
        digest, size = _sha256_file(path, policy.max_file_bytes)
        total += size
        if total > policy.max_total_bytes:
            raise SnapshotPublicationError("TOTAL_BYTE_BUDGET_EXCEEDED")
        files.append(
            SnapshotFile(
                logical_path=relative,
                object_key=f"{prefix}/objects/sha256/{digest}.parquet",
                size_bytes=size,
                sha256=digest,
            )
        )
    return files


def _head(s3: ObjectStore, bucket: str, key: str) -> dict[str, Any] | None:
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        response = exc.response
        code = str(response.get("Error", {}).get("Code", ""))
        status = int(response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))
        if code in {"404", "NoSuchKey", "NotFound"} or status == 404:
            return None
        raise SnapshotPublicationError("OBJECT_STORE_HEAD_FAILED") from exc


def _metadata_matches(head: dict[str, Any], digest: str, size: int) -> bool:
    metadata = {str(key).casefold(): str(value) for key, value in head.get("Metadata", {}).items()}
    return (
        metadata.get("sha256") == digest
        and head.get("ContentLength") == size
        and SHA256.fullmatch(digest) is not None
    )


def _put_immutable(
    s3: ObjectStore,
    *,
    bucket: str,
    key: str,
    body: bytes | BinaryIO,
    digest: str,
    size: int,
    content_type: str,
    metadata: dict[str, str],
) -> bool:
    existing = _head(s3, bucket, key)
    if existing is not None:
        if not _metadata_matches(existing, digest, size):
            raise SnapshotPublicationError("IMMUTABLE_OBJECT_COLLISION")
        return False
    created = True
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentLength=size,
            ContentType=content_type,
            ChecksumSHA256=base64.b64encode(bytes.fromhex(digest)).decode("ascii"),
            IfNoneMatch="*",
            Metadata={"sha256": digest, **metadata},
        )
    except ClientError as exc:
        response = exc.response
        code = str(response.get("Error", {}).get("Code", ""))
        status = int(response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))
        if code not in {"412", "PreconditionFailed"} and status != 412:
            raise SnapshotPublicationError("OBJECT_STORE_PUT_FAILED") from exc
        created = False
    verified = _head(s3, bucket, key)
    if verified is None or not _metadata_matches(verified, digest, size):
        raise SnapshotPublicationError("OBJECT_VERIFICATION_FAILED")
    return created


def _manifest_bytes(
    *,
    dataset: str,
    source_revision: str,
    policy: SnapshotPolicy,
    files: list[SnapshotFile],
) -> tuple[str, bytes, str]:
    body = {
        "schema_version": 1,
        "dataset": dataset,
        "source_revision": source_revision,
        "policy": policy.as_dict(),
        "file_count": len(files),
        "unique_object_count": len({item.sha256 for item in files}),
        "total_bytes": sum(item.size_bytes for item in files),
        "files": [item.as_dict() for item in files],
    }
    snapshot_sha256 = hashlib.sha256(_canonical_json(body)).hexdigest()
    manifest = _canonical_json({**body, "snapshot_sha256": snapshot_sha256})
    return snapshot_sha256, manifest, hashlib.sha256(manifest).hexdigest()


def publish_snapshot(
    root: Path,
    bucket: str,
    *,
    source_revision: str,
    dataset: str = "transactions",
    prefix: str = "silver/transactions",
    s3: ObjectStore | None = None,
    policy: SnapshotPolicy | None = None,
    create_bucket: bool = False,
) -> SnapshotPublication:
    """Publish immutable objects, then commit the snapshot by writing its manifest last."""

    active_policy = policy or SnapshotPolicy()
    _validate_bucket(bucket)
    normalized_prefix = _validate_prefix(prefix)
    _validate_revision(source_revision)
    if not isinstance(dataset, str) or not NAME.fullmatch(dataset):
        raise SnapshotPublicationError("INVALID_DATASET")
    store = s3 or client()
    files = _scan_snapshot(root, normalized_prefix, active_policy)
    snapshot_sha256, manifest, manifest_sha256 = _manifest_bytes(
        dataset=dataset,
        source_revision=source_revision,
        policy=active_policy,
        files=files,
    )
    if len(manifest) > active_policy.max_manifest_bytes:
        raise SnapshotPublicationError("MANIFEST_BYTE_BUDGET_EXCEEDED")
    if create_bucket:
        ensure_bucket(store, bucket)

    uploaded = reused = 0
    sources_by_digest: dict[str, tuple[Path, SnapshotFile]] = {}
    for item in files:
        sources_by_digest.setdefault(item.sha256, (root / item.logical_path, item))
    for digest, (path, item) in sorted(sources_by_digest.items()):
        with path.open("rb") as handle:
            before = _verify_open_file(
                handle,
                expected_digest=digest,
                expected_size=item.size_bytes,
                maximum=active_policy.max_file_bytes,
            )
            created = _put_immutable(
                store,
                bucket=bucket,
                key=item.object_key,
                body=handle,
                digest=digest,
                size=item.size_bytes,
                content_type="application/vnd.apache.parquet",
                metadata={"content-addressed": "true"},
            )
            after = os.fstat(handle.fileno())
            if (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_mtime_ns,
            ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
                raise SnapshotPublicationError("FILE_CHANGED_DURING_PUBLICATION")
        uploaded += int(created)
        reused += int(not created)

    manifest_key = f"{normalized_prefix}/manifests/{snapshot_sha256}.json"
    manifest_created = _put_immutable(
        store,
        bucket=bucket,
        key=manifest_key,
        body=manifest,
        digest=manifest_sha256,
        size=len(manifest),
        content_type="application/json",
        metadata={"snapshot-sha256": snapshot_sha256},
    )
    if not manifest_created:
        try:
            persisted = store.get_object(Bucket=bucket, Key=manifest_key)["Body"].read()
        except (ClientError, KeyError, OSError, AttributeError) as exc:
            raise SnapshotPublicationError("MANIFEST_READ_FAILED") from exc
        if persisted != manifest:
            raise SnapshotPublicationError("IMMUTABLE_MANIFEST_COLLISION")

    return SnapshotPublication(
        bucket=bucket,
        manifest_key=manifest_key,
        snapshot_sha256=snapshot_sha256,
        manifest_sha256=manifest_sha256,
        file_count=len(files),
        unique_object_count=len(sources_by_digest),
        total_bytes=sum(item.size_bytes for item in files),
        uploaded_objects=uploaded,
        reused_objects=reused,
    )


def sync_directory(
    root: Path, bucket: str, prefix: str = "silver/transactions"
) -> dict[str, object]:
    """Compatibility entrypoint using an explicit immutable source revision."""

    revision = os.getenv("SOURCE_REVISION")
    if revision is None:
        raise SnapshotPublicationError("SOURCE_REVISION_REQUIRED")
    return publish_snapshot(
        root,
        bucket,
        prefix=prefix,
        source_revision=revision,
        create_bucket=True,
    ).as_dict()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Publish an immutable, manifest-committed Silver Parquet snapshot."
    )
    parser.add_argument("--root", type=Path, default=Path("data/silver/transactions"))
    parser.add_argument("--bucket", default="fintech-lakehouse")
    parser.add_argument("--prefix", default="silver/transactions")
    parser.add_argument("--dataset", default="transactions")
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--create-bucket", action="store_true")
    args = parser.parse_args()
    publication = publish_snapshot(
        args.root,
        args.bucket,
        prefix=args.prefix,
        dataset=args.dataset,
        source_revision=args.source_revision,
        create_bucket=args.create_bucket,
    )
    print(json.dumps(publication.as_dict(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
