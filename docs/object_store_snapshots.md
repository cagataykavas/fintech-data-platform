# Immutable object-store snapshots

The local pipeline publishes partition files atomically and commits their event identities in
SQLite. Copying those files to S3-compatible storage with mutable keys would weaken that contract:
a retry could overwrite bytes, consumers could observe only part of a run, and a directory listing
would not identify a complete reproducible dataset.

`src.object_store.publish_snapshot` uses a manifest-last protocol:

1. enumerate a bounded, symlink-free Parquet tree;
2. hash every file and map it to `objects/sha256/<digest>.parquet`;
3. create each object with `If-None-Match: *`, a server-validated SHA-256 checksum, and then verify
   size plus SHA-256 metadata;
4. build a canonical manifest containing logical paths, object keys, byte sizes, policy and the
   immutable producer revision;
5. publish `manifests/<snapshot_sha256>.json` only after all objects are present and verified.

The manifest key is the commit marker. Consumers must receive an exact manifest key or snapshot
digest; they must not infer completeness from object listings or select a lexicographic "latest"
object.

## Usage

```bash
python -m src.object_store \
  --root data/silver/transactions \
  --bucket fintech-lakehouse \
  --prefix silver/transactions \
  --source-revision git:0123456789abcdef \
  --create-bucket
```

The command returns the snapshot and manifest SHA-256 values, counts and byte totals. It never
returns credentials or Parquet contents.

## Failure and retry behavior

- An interrupted upload leaves unreferenced content-addressed objects, not a committed partial
  snapshot. A later retry safely reuses objects whose stored digest and size match.
- Concurrent publishers race on conditional creates. The loser accepts the object only after the
  stored integrity metadata matches its expected bytes.
- Reusing an object or manifest key with different integrity evidence fails closed.
- The scanner bounds file count, individual size, aggregate size and canonical manifest size.
- Mutable producer revisions such as `main`, `latest` and `HEAD` are rejected.

Content-addressed orphans can be garbage-collected only after a retention period and a complete
reachability scan of retained manifests. Immediate deletion is unsafe while publishers or consumers
may still be using an object.

## Trust boundary and limitations

S3 metadata and conditional-write semantics are trusted. SHA-256 detects accidental or adversarial
byte substitution but does not authenticate the producer; production manifests should be signed
and retained with object-lock/versioning policy. Bucket creation is a local-development convenience,
not infrastructure provisioning. The publisher does not encrypt, configure IAM, replicate, promote
a mutable alias, or provide distributed garbage collection. A snapshot proves byte completeness,
not business correctness or successful downstream consumption.

The next step is to sign the canonical manifest, require a bucket policy that denies non-TLS and
unencrypted writes, and bind an accepted snapshot digest to the Airflow/dbt run evidence.
