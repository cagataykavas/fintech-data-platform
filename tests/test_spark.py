from datetime import UTC, datetime, timedelta

import pytest


pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from spark.banking_features import TRANSACTION_SCHEMA, build_feature_table
from spark.structured_streaming import validate_events


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("fintech-data-platform-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_velocity_windows_exclude_the_current_event(spark: SparkSession) -> None:
    start = datetime(2026, 1, 1, 12, tzinfo=UTC)
    rows = [
        ("evt-00000001", "customer-1", "merchant-1", start, 10.0, "TRY", "card", "TR"),
        (
            "evt-00000002",
            "customer-1",
            "merchant-2",
            start + timedelta(hours=1),
            25.0,
            "TRY",
            "card",
            "DE",
        ),
    ]
    result = (
        build_feature_table(spark.createDataFrame(rows, TRANSACTION_SCHEMA))
        .orderBy("event_time")
        .collect()
    )

    assert result[0]["txn_count_24h"] == 0
    assert result[0]["amount_sum_24h"] == 0.0
    assert result[1]["txn_count_24h"] == 1
    assert result[1]["amount_sum_24h"] == 10.0
    assert result[1]["is_cross_border"] == 1


def test_stream_validation_uses_the_canonical_event_id(spark: SparkSession) -> None:
    schema = TRANSACTION_SCHEMA
    now = datetime(2026, 1, 1, tzinfo=UTC)
    events = spark.createDataFrame(
        [
            ("evt-00000001", "customer-1", "merchant-1", now, 10.0, "TRY", "card", "TR"),
            ("evt-00000002", "customer-1", "merchant-1", now, -1.0, "TRY", "card", "TR"),
        ],
        schema,
    )

    valid, invalid = validate_events(events)

    assert valid.select("event_id").first()["event_id"] == "evt-00000001"
    assert invalid.select("validation_error").first()["validation_error"] == "negative_amount"
