from datetime import UTC, datetime, timedelta
import json

import pytest

from src.reconcile_features import (
    FeatureSnapshot,
    ReconciliationPolicy,
    reconcile_feature_snapshots,
)


AS_OF = datetime(2026, 9, 21, 12, tzinfo=UTC)


def snapshot(
    customer: str,
    minute: int,
    *,
    count: int = 2,
    amount: float = 30.0,
    end_offset: timedelta = timedelta(0),
) -> FeatureSnapshot:
    start = AS_OF - timedelta(hours=2) + timedelta(minutes=minute)
    return FeatureSnapshot(
        customer,
        start,
        start + timedelta(minutes=5) + end_offset,
        count,
        amount,
    )


def policy(**overrides: object) -> ReconciliationPolicy:
    values = {
        "maturation_lag": timedelta(minutes=20),
        "min_reference_windows": 2,
        "max_missing_rate": 0.0,
        "max_unexpected_rate": 0.0,
        "max_mismatch_rate": 0.0,
        "amount_abs_tolerance": 0.01,
        "amount_rel_tolerance": 0.001,
    }
    values.update(overrides)
    return ReconciliationPolicy(**values)


def test_identical_mature_windows_pass() -> None:
    rows = [snapshot("customer-b", 5), snapshot("customer-a", 0)]

    report = reconcile_feature_snapshots(rows, reversed(rows), as_of=AS_OF, policy=policy())

    assert report.passed is True
    assert report.matched_windows == 2
    assert report.reasons == ()
    assert report.to_dict()["maturity_cutoff"] == "2026-09-21T11:40:00Z"


def test_recent_windows_are_pending_not_missing() -> None:
    recent = FeatureSnapshot(
        "customer-recent",
        AS_OF - timedelta(minutes=15),
        AS_OF - timedelta(minutes=10),
        1,
        5.0,
    )
    mature = [snapshot("customer-a", 0), snapshot("customer-b", 5)]

    report = reconcile_feature_snapshots([*mature, recent], mature, as_of=AS_OF, policy=policy())

    assert report.passed is True
    assert report.pending_batch_windows == 1
    assert report.missing_windows == 0


def test_missing_stream_window_fails_with_deterministic_key() -> None:
    rows = [snapshot("customer-b", 5), snapshot("customer-a", 0)]

    report = reconcile_feature_snapshots(rows, rows[:1], as_of=AS_OF, policy=policy())

    assert report.passed is False
    assert report.reasons == ("missing_stream_windows",)
    assert report.missing_windows == 1
    assert report.missing_keys[0].startswith("customer-a|")


def test_unexpected_stream_window_has_independent_rate_gate() -> None:
    batch = [snapshot("customer-a", 0), snapshot("customer-b", 5)]
    stream = [*batch, snapshot("customer-c", 10)]

    report = reconcile_feature_snapshots(batch, stream, as_of=AS_OF, policy=policy())

    assert report.passed is False
    assert report.reasons == ("unexpected_stream_windows",)
    assert report.unexpected_rate == pytest.approx(1 / 3)


def test_amount_within_absolute_tolerance_passes() -> None:
    batch = [snapshot("customer-a", 0, amount=10.0), snapshot("customer-b", 5)]
    stream = [snapshot("customer-a", 0, amount=10.009), snapshot("customer-b", 5)]

    report = reconcile_feature_snapshots(batch, stream, as_of=AS_OF, policy=policy())

    assert report.passed is True
    assert report.mismatched_windows == 0


def test_count_and_amount_mismatch_preserve_metric_evidence() -> None:
    batch = [
        snapshot("customer-a", 0, count=2, amount=100.0),
        snapshot("customer-b", 5),
    ]
    stream = [
        snapshot("customer-a", 0, count=3, amount=101.0),
        snapshot("customer-b", 5),
    ]

    report = reconcile_feature_snapshots(batch, stream, as_of=AS_OF, policy=policy())

    assert report.reasons == ("feature_value_mismatch",)
    assert report.mismatches[0].differing_metrics == ("txn_count", "amount_sum")
    assert report.mismatches[0].amount_allowed_delta == pytest.approx(0.1)


def test_insufficient_reference_evidence_fails_closed() -> None:
    row = snapshot("customer-a", 0)

    report = reconcile_feature_snapshots([row], [row], as_of=AS_OF, policy=policy())

    assert report.passed is False
    assert report.reasons == ("insufficient_reference_windows",)


def test_duplicate_window_is_rejected() -> None:
    row = snapshot("customer-a", 0)

    with pytest.raises(ValueError, match="duplicate batch window"):
        reconcile_feature_snapshots([row, row], [row], as_of=AS_OF, policy=policy())


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("window_start", "2026-09-21T10:00:00", "timezone-aware"),
        ("txn_count", True, "must be an integer"),
        ("amount_sum", float("nan"), "finite and non-negative"),
    ],
)
def test_malformed_snapshot_is_rejected(field: str, value: object, message: str) -> None:
    raw = {
        "customer_id": "customer-a",
        "window_start": "2026-09-21T10:00:00Z",
        "window_end": "2026-09-21T10:05:00Z",
        "txn_count": 2,
        "amount_sum": 30.0,
    }
    raw[field] = value

    with pytest.raises(ValueError, match=message):
        reconcile_feature_snapshots([raw], [raw], as_of=AS_OF, policy=policy())


def test_report_is_json_serializable_and_evidence_is_sorted() -> None:
    rows = [snapshot("customer-z", 5), snapshot("customer-a", 0)]

    report = reconcile_feature_snapshots(rows, [], as_of=AS_OF, policy=policy())
    encoded = json.dumps(report.to_dict(), sort_keys=True)

    assert '"passed": false' in encoded
    assert report.missing_keys == tuple(sorted(report.missing_keys))


def test_invalid_policy_is_rejected() -> None:
    with pytest.raises(ValueError, match="max_missing_rate"):
        ReconciliationPolicy(max_missing_rate=1.1)
