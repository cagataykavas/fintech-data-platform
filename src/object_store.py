from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import boto3
from botocore.client import Config


def client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minio"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minio123"),
        region_name=os.getenv("AWS_REGION", "eu-central-1"),
        config=Config(signature_version="s3v4"),
    )


def ensure_bucket(s3, bucket: str) -> None:
    existing = {item["Name"] for item in s3.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)


def sync_directory(root: Path, bucket: str, prefix: str = "silver/transactions") -> dict[str, object]:
    s3 = client()
    ensure_bucket(s3, bucket)
    uploaded: list[str] = []
    for path in sorted(root.rglob("*.parquet")):
        relative = path.relative_to(root).as_posix()
        key = f"{prefix.rstrip('/')}/{relative}"
        s3.upload_file(str(path), bucket, key)
        uploaded.append(key)
    return {"bucket": bucket, "objects_uploaded": len(uploaded), "sample_keys": uploaded[:10]}


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync Silver Parquet to an S3-compatible object store.")
    parser.add_argument("--root", type=Path, default=Path("data/silver/transactions"))
    parser.add_argument("--bucket", default="fintech-lakehouse")
    parser.add_argument("--prefix", default="silver/transactions")
    args = parser.parse_args()
    print(json.dumps(sync_directory(args.root, args.bucket, args.prefix), indent=2))


if __name__ == "__main__":
    main()
