from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
import json
import math
from pathlib import Path
from typing import Any, Iterable, Mapping


@dataclass(frozen=True)
class ReconciliationPolicy:
    maturation_lag: timedelta = timedelta(minutes=20)
    min_reference_windows: int = 20
    max_missing_rate: float = 0.01
    max_unexpected_rate: float = 0.01
    max_mismatch_rate: float = 0.01
    amount_abs_tolerance: float = 0.01
    amount_rel_tolerance: float = 0.001
    max_evidence_keys: int = 20

    def __post_init__(self) -> None:
        if self.maturation_lag < timedelta(0):
            raise ValueError("maturation_lag must be non-negative")
        if self.min_reference_windows < 1:
            raise ValueError("min_reference_windows must be positive")
        for name in ("max_missing_rate", "max_unexpected_rate", "max_mismatch_rate"):
            value = getattr(self, name)
            if not math.isfinite(value) or not 0 <= value <= 1:
                raise ValueError(f"{name} must be finite and between 0 and 1")
        for name in ("amount_abs_tolerance", "amount_rel_tolerance"):
            value = getattr(self, name)
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and non-negative")
        if self.max_evidence_keys < 1:
            raise ValueError("max_evidence_keys must be positive")


@dataclass(frozen=True)
class FeatureSnapshot:
    customer_id: str
    window_start: datetime
    window_end: datetime
    txn_count: int
    amount_sum: float

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> FeatureSnapshot:
        required = {
            "customer_id",
            "window_start",
            "window_end",
            "txn_count",
            "amount_sum",
        }
        missing = sorted(required - value.keys())
        if missing:
            raise ValueError(f"snapshot is missing fields: {', '.join(missing)}")
        return cls(
            customer_id=str(value["customer_id"]),
            window_start=_parse_timestamp(value["window_start"], "window_start"),
            window_end=_parse_timestamp(value["window_end"], "window_end"),
            txn_count=value["txn_count"],
            amount_sum=value["amount_sum"],
        )

    def __post_init__(self) -> None:
        if not self.customer_id.strip():
            raise ValueError("customer_id must not be empty")
        _require_aware(self.window_start, "window_start")
        _require_aware(self.window_end, "window_end")
        if self.window_start >= self.window_end:
            raise ValueError("window_start must be before window_end")
        if isinstance(self.txn_count, bool) or not isinstance(self.txn_count, int):
            raise ValueError("txn_count must be an integer")
        if self.txn_count < 0:
            raise ValueError("txn_count must be non-negative")
        if isinstance(self.amount_sum, bool) or not isinstance(self.amount_sum, (int, float)):
            raise ValueError("amount_sum must be numeric")
        if not math.isfinite(self.amount_sum) or self.amount_sum < 0:
            raise ValueError("amount_sum must be finite and non-negative")

    @property
    def key(self) -> str:
        start = self.window_start.astimezone(UTC).isoformat().replace("+00:00", "Z")
        end = self.window_end.astimezone(UTC).isoformat().replace("+00:00", "Z")
        return f"{self.customer_id}|{start}|{end}"


@dataclass(frozen=True)
class FeatureMismatch:
    key: str
    batch_txn_count: int
    stream_txn_count: int
    batch_amount_sum: float
    stream_amount_sum: float
    amount_abs_delta: float
    amount_allowed_delta: float
    differing_metrics: tuple[str, ...]


@dataclass(frozen=True)
class ReconciliationReport:
    passed: bool
    as_of: str
    maturity_cutoff: str
    reference_windows: int
    stream_windows: int
    matched_windows: int
    pending_batch_windows: int
    pending_stream_windows: int
    missing_windows: int
    unexpected_windows: int
    mismatched_windows: int
    missing_rate: float
    unexpected_rate: float
    mismatch_rate: float
    reasons: tuple[str, ...]
    missing_keys: tuple[str, ...]
    unexpected_keys: tuple[str, ...]
    mismatches: tuple[FeatureMismatch, ...]
    evidence_truncated: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def reconcile_feature_snapshots(
    batch: Iterable[FeatureSnapshot | Mapping[str, Any]],
    stream: Iterable[FeatureSnapshot | Mapping[str, Any]],
    *,
    as_of: datetime,
    policy: ReconciliationPolicy | None = None,
) -> ReconciliationReport:
    policy = policy or ReconciliationPolicy()
    _require_aware(as_of, "as_of")
    as_of_utc = as_of.astimezone(UTC)
    cutoff = as_of_utc - policy.maturation_lag

    batch_rows = _coerce_snapshots(batch, "batch")
    stream_rows = _coerce_snapshots(stream, "stream")
    batch_mature = {row.key: row for row in batch_rows if row.window_end <= cutoff}
    stream_mature = {row.key: row for row in stream_rows if row.window_end <= cutoff}

    batch_keys = set(batch_mature)
    stream_keys = set(stream_mature)
    missing = sorted(batch_keys - stream_keys)
    unexpected = sorted(stream_keys - batch_keys)
    common = sorted(batch_keys & stream_keys)

    all_mismatches: list[FeatureMismatch] = []
    for key in common:
        mismatch = _compare(batch_mature[key], stream_mature[key], policy)
        if mismatch is not None:
            all_mismatches.append(mismatch)
    missing_rate = _rate(len(missing), len(batch_mature))
    unexpected_rate = _rate(len(unexpected), len(stream_mature))
    mismatch_rate = _rate(len(all_mismatches), len(common))

    reasons: list[str] = []
    if len(batch_mature) < policy.min_reference_windows:
        reasons.append("insufficient_reference_windows")
    if missing_rate > policy.max_missing_rate:
        reasons.append("missing_stream_windows")
    if unexpected_rate > policy.max_unexpected_rate:
        reasons.append("unexpected_stream_windows")
    if mismatch_rate > policy.max_mismatch_rate:
        reasons.append("feature_value_mismatch")

    limit = policy.max_evidence_keys
    return ReconciliationReport(
        passed=not reasons,
        as_of=_format_timestamp(as_of_utc),
        maturity_cutoff=_format_timestamp(cutoff),
        reference_windows=len(batch_mature),
        stream_windows=len(stream_mature),
        matched_windows=len(common) - len(all_mismatches),
        pending_batch_windows=len(batch_rows) - len(batch_mature),
        pending_stream_windows=len(stream_rows) - len(stream_mature),
        missing_windows=len(missing),
        unexpected_windows=len(unexpected),
        mismatched_windows=len(all_mismatches),
        missing_rate=missing_rate,
        unexpected_rate=unexpected_rate,
        mismatch_rate=mismatch_rate,
        reasons=tuple(reasons),
        missing_keys=tuple(missing[:limit]),
        unexpected_keys=tuple(unexpected[:limit]),
        mismatches=tuple(all_mismatches[:limit]),
        evidence_truncated=any(
            count > limit for count in (len(missing), len(unexpected), len(all_mismatches))
        ),
    )


def _coerce_snapshots(
    values: Iterable[FeatureSnapshot | Mapping[str, Any]], source: str
) -> list[FeatureSnapshot]:
    rows = [
        value if isinstance(value, FeatureSnapshot) else FeatureSnapshot.from_mapping(value)
        for value in values
    ]
    seen: set[str] = set()
    for row in rows:
        if row.key in seen:
            raise ValueError(f"duplicate {source} window: {row.key}")
        seen.add(row.key)
    return rows


def _compare(
    batch: FeatureSnapshot, stream: FeatureSnapshot, policy: ReconciliationPolicy
) -> FeatureMismatch | None:
    amount_delta = abs(float(batch.amount_sum) - float(stream.amount_sum))
    allowed_delta = max(
        policy.amount_abs_tolerance,
        policy.amount_rel_tolerance * abs(float(batch.amount_sum)),
    )
    differing: list[str] = []
    if batch.txn_count != stream.txn_count:
        differing.append("txn_count")
    if amount_delta > allowed_delta:
        differing.append("amount_sum")
    if not differing:
        return None
    return FeatureMismatch(
        key=batch.key,
        batch_txn_count=batch.txn_count,
        stream_txn_count=stream.txn_count,
        batch_amount_sum=float(batch.amount_sum),
        stream_amount_sum=float(stream.amount_sum),
        amount_abs_delta=amount_delta,
        amount_allowed_delta=allowed_delta,
        differing_metrics=tuple(differing),
    )


def _parse_timestamp(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field} must be an ISO-8601 timestamp") from exc
    else:
        raise ValueError(f"{field} must be a datetime or ISO-8601 timestamp")
    _require_aware(parsed, field)
    return parsed.astimezone(UTC)


def _require_aware(value: datetime, field: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _rate(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0.0


def _load_snapshots(path: Path) -> list[Mapping[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list) or any(not isinstance(item, dict) for item in payload):
        raise ValueError(f"{path} must contain a JSON array of objects")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Gate matured streaming feature windows against a batch reference."
    )
    parser.add_argument("--batch", type=Path, required=True)
    parser.add_argument("--stream", type=Path, required=True)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--maturation-minutes", type=float, default=20)
    parser.add_argument("--min-reference-windows", type=int, default=20)
    parser.add_argument("--max-missing-rate", type=float, default=0.01)
    parser.add_argument("--max-unexpected-rate", type=float, default=0.01)
    parser.add_argument("--max-mismatch-rate", type=float, default=0.01)
    parser.add_argument("--amount-abs-tolerance", type=float, default=0.01)
    parser.add_argument("--amount-rel-tolerance", type=float, default=0.001)
    args = parser.parse_args()
    policy = ReconciliationPolicy(
        maturation_lag=timedelta(minutes=args.maturation_minutes),
        min_reference_windows=args.min_reference_windows,
        max_missing_rate=args.max_missing_rate,
        max_unexpected_rate=args.max_unexpected_rate,
        max_mismatch_rate=args.max_mismatch_rate,
        amount_abs_tolerance=args.amount_abs_tolerance,
        amount_rel_tolerance=args.amount_rel_tolerance,
    )
    report = reconcile_feature_snapshots(
        _load_snapshots(args.batch),
        _load_snapshots(args.stream),
        as_of=_parse_timestamp(args.as_of, "as_of"),
        policy=policy,
    )
    print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    if not report.passed:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
