from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType


TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("channel", StringType(), False),
        StructField("country", StringType(), False),
    ]
)


def read_transactions(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.schema(TRANSACTION_SCHEMA)
        .option("mode", "FAILFAST")
        .parquet(path)
        .filter(F.col("amount") >= 0)
        .dropDuplicates(["transaction_id"])
    )


def add_event_features(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("event_date", F.to_date("event_time"))
        .withColumn("event_hour", F.hour("event_time"))
        .withColumn("is_night", F.when(F.col("event_hour").between(0, 5), 1).otherwise(0))
        .withColumn("is_cross_border", F.when(F.col("country") != F.lit("TR"), 1).otherwise(0))
        .withColumn("log_amount", F.log1p("amount"))
    )


def add_customer_velocity_features(df: DataFrame) -> DataFrame:
    """Create leakage-aware rolling features using only rows strictly before the current event."""
    ordered = Window.partitionBy("customer_id").orderBy(F.col("event_time").cast("long"))

    previous_24h = ordered.rangeBetween(-24 * 3600, -1)
    previous_7d = ordered.rangeBetween(-7 * 24 * 3600, -1)
    previous_30d = ordered.rangeBetween(-30 * 24 * 3600, -1)

    return (
        df.withColumn("txn_count_24h", F.count("transaction_id").over(previous_24h))
        .withColumn("amount_sum_24h", F.sum("amount").over(previous_24h))
        .withColumn("amount_avg_7d", F.avg("amount").over(previous_7d))
        .withColumn("amount_std_30d", F.stddev_pop("amount").over(previous_30d))
        .withColumn("cross_border_count_7d", F.sum("is_cross_border").over(previous_7d))
        .withColumn("night_txn_count_7d", F.sum("is_night").over(previous_7d))
        .withColumn("previous_transaction_time", F.lag("event_time", 1).over(ordered))
        .withColumn(
            "seconds_since_previous_txn",
            F.col("event_time").cast("long") - F.col("previous_transaction_time").cast("long"),
        )
        .fillna(
            {
                "txn_count_24h": 0,
                "amount_sum_24h": 0.0,
                "amount_avg_7d": 0.0,
                "amount_std_30d": 0.0,
                "cross_border_count_7d": 0,
                "night_txn_count_7d": 0,
            }
        )
    )


def add_peer_features(df: DataFrame) -> DataFrame:
    """Compare an event with contemporaneous customer-segment peers without target leakage."""
    daily_peer = Window.partitionBy("event_date", "channel")
    return (
        df.withColumn("peer_amount_mean", F.avg("amount").over(daily_peer))
        .withColumn("peer_amount_std", F.stddev_pop("amount").over(daily_peer))
        .withColumn(
            "amount_peer_zscore",
            F.when(
                F.col("peer_amount_std") > 0,
                (F.col("amount") - F.col("peer_amount_mean")) / F.col("peer_amount_std"),
            ).otherwise(F.lit(0.0)),
        )
    )


def build_feature_table(df: DataFrame) -> DataFrame:
    features = add_event_features(df)
    features = add_customer_velocity_features(features)
    features = add_peer_features(features)

    selected = [
        "transaction_id",
        "customer_id",
        "event_time",
        "amount",
        "channel",
        "country",
        "log_amount",
        "is_night",
        "is_cross_border",
        "txn_count_24h",
        "amount_sum_24h",
        "amount_avg_7d",
        "amount_std_30d",
        "cross_border_count_7d",
        "night_txn_count_7d",
        "seconds_since_previous_txn",
        "amount_peer_zscore",
    ]
    return features.select(*selected)


def write_partitioned(df: DataFrame, output_path: str) -> None:
    (
        df.withColumn("event_date", F.to_date("event_time"))
        .repartition("event_date")
        .write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(output_path)
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("banking-feature-engineering")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "64")
        .getOrCreate()
    )

    transactions = read_transactions(spark, "data/silver/transactions")
    feature_table = build_feature_table(transactions)
    write_partitioned(feature_table, "data/features/transactions")

    feature_table.orderBy(F.desc("amount_peer_zscore")).show(20, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
