from __future__ import annotations

import argparse
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("country", StringType(), False),
        StructField("channel", StringType(), False),
    ]
)


@dataclass(frozen=True)
class StreamingConfig:
    bootstrap_servers: str
    topic: str
    checkpoint_path: str
    output_path: str
    watermark: str = "15 minutes"
    trigger_seconds: int = 30


def parse_kafka_events(raw: DataFrame) -> DataFrame:
    decoded = raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("json_payload"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    )
    parsed = decoded.withColumn(
        "event",
        F.from_json("json_payload", TRANSACTION_SCHEMA, {"mode": "PERMISSIVE"}),
    )
    return parsed.select("kafka_key", "kafka_timestamp", "kafka_partition", "kafka_offset", "event.*")


def validate_events(events: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid_condition = (
        F.col("transaction_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("amount").isNotNull()
        & (F.col("amount") >= 0)
        & F.col("currency").isNotNull()
    )

    enriched = events.withColumn(
        "validation_error",
        F.when(F.col("transaction_id").isNull(), F.lit("missing_transaction_id"))
        .when(F.col("customer_id").isNull(), F.lit("missing_customer_id"))
        .when(F.col("event_time").isNull(), F.lit("missing_event_time"))
        .when(F.col("amount").isNull(), F.lit("missing_amount"))
        .when(F.col("amount") < 0, F.lit("negative_amount"))
        .when(F.col("currency").isNull(), F.lit("missing_currency")),
    )
    return enriched.filter(valid_condition).drop("validation_error"), enriched.filter(~valid_condition)


def add_stream_features(events: DataFrame, watermark: str) -> DataFrame:
    """Create event-time features that are safe and bounded in streaming execution.

    Stateful rolling per-customer features are intentionally kept separate because
    their production design depends on state-store size, retention and latency SLOs.
    This stream focuses on deduplication, event-time windows and aggregate signals.
    """
    deduplicated = (
        events.withWatermark("event_time", watermark)
        .dropDuplicatesWithinWatermark(["transaction_id"])
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("event_hour", F.hour("event_time"))
        .withColumn("is_night", F.col("event_hour").between(0, 5).cast("int"))
        .withColumn("is_cross_border", (F.col("country") != F.lit("TR")).cast("int"))
        .withColumn("log_amount", F.log1p("amount"))
    )

    five_minute = (
        deduplicated.groupBy(
            F.window("event_time", "5 minutes", "1 minute"),
            "customer_id",
        )
        .agg(
            F.count("transaction_id").alias("txn_count_5m"),
            F.sum("amount").alias("amount_sum_5m"),
            F.max("amount").alias("amount_max_5m"),
            F.sum("is_cross_border").alias("cross_border_count_5m"),
            F.sum("is_night").alias("night_count_5m"),
            F.approx_count_distinct("merchant_id").alias("merchant_diversity_5m"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
    )
    return five_minute


def kafka_source(spark: SparkSession, config: StreamingConfig) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrap_servers)
        .option("subscribe", config.topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def write_features(features: DataFrame, config: StreamingConfig):
    return (
        features.writeStream.format("parquet")
        .outputMode("append")
        .option("path", config.output_path)
        .option("checkpointLocation", config.checkpoint_path)
        .partitionBy("window_start")
        .trigger(processingTime=f"{config.trigger_seconds} seconds")
        .queryName("transaction_risk_features")
        .start()
    )


def build_session() -> SparkSession:
    return (
        SparkSession.builder.appName("fintech-structured-streaming")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        .config("spark.sql.shuffle.partitions", "64")
        .getOrCreate()
    )


def run(config: StreamingConfig) -> None:
    spark = build_session()
    raw = kafka_source(spark, config)
    events = parse_kafka_events(raw)
    valid, invalid = validate_events(events)

    # Invalid events are observable instead of silently discarded. A production
    # deployment would normally route them to a quarantine table/topic.
    invalid_query = (
        invalid.writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .queryName("invalid_transactions")
        .start()
    )

    features = add_stream_features(valid, config.watermark)
    feature_query = write_features(features, config)

    try:
        feature_query.awaitTermination()
    finally:
        invalid_query.stop()
        feature_query.stop()
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka -> PySpark Structured Streaming risk features")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--checkpoint", default="data/checkpoints/transaction-risk")
    parser.add_argument("--output", default="data/streaming/customer-risk-features")
    parser.add_argument("--watermark", default="15 minutes")
    parser.add_argument("--trigger-seconds", type=int, default=30)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(
        StreamingConfig(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            checkpoint_path=args.checkpoint,
            output_path=args.output,
            watermark=args.watermark,
            trigger_seconds=args.trigger_seconds,
        )
    )


if __name__ == "__main__":
    main()
