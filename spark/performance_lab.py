from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("segment", StringType(), False),
        StructField("country", StringType(), False),
    ]
)

TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("amount_cents", IntegerType(), False),
    ]
)


def explain(df: DataFrame, title: str) -> None:
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)
    df.explain(mode="formatted")


def broadcast_dimension_join(transactions: DataFrame, customers: DataFrame) -> DataFrame:
    """Use when the dimension table is small enough to fit safely on every executor."""
    return transactions.join(F.broadcast(customers), on="customer_id", how="left")


def repartition_for_large_join(transactions: DataFrame, customers: DataFrame, partitions: int = 64) -> DataFrame:
    """Explicitly repartition both large sides by the join key to make intent inspectable."""
    left = transactions.repartition(partitions, "customer_id")
    right = customers.repartition(partitions, "customer_id")
    return left.join(right, on="customer_id", how="left")


def identify_hot_keys(transactions: DataFrame, top_n: int = 20) -> DataFrame:
    return (
        transactions.groupBy("customer_id")
        .count()
        .orderBy(F.desc("count"))
        .limit(top_n)
    )


def salted_join(
    transactions: DataFrame,
    customers: DataFrame,
    *,
    hot_customer_ids: list[str],
    salt_buckets: int = 8,
) -> DataFrame:
    """Demonstrate key salting for a deliberately skewed customer join.

    Hot transaction keys are distributed across multiple salts. Matching customer
    dimension rows are replicated across those salts only for hot keys. This reduces
    a single hot shuffle partition at the cost of controlled dimension duplication.
    """
    hot_array = F.array(*[F.lit(value) for value in hot_customer_ids])

    left = transactions.withColumn(
        "salt",
        F.when(
            F.array_contains(hot_array, F.col("customer_id")),
            F.pmod(F.xxhash64("transaction_id"), F.lit(salt_buckets)),
        ).otherwise(F.lit(0)),
    )

    hot_customers = customers.filter(F.col("customer_id").isin(hot_customer_ids))
    normal_customers = customers.filter(~F.col("customer_id").isin(hot_customer_ids)).withColumn("salt", F.lit(0))

    salts = F.array(*[F.lit(i) for i in range(salt_buckets)])
    replicated_hot = hot_customers.withColumn("salt", F.explode(salts))
    right = normal_customers.unionByName(replicated_hot)

    return left.join(right, on=["customer_id", "salt"], how="left").drop("salt")


def aggregate_without_python_udf(enriched: DataFrame) -> DataFrame:
    """Prefer Catalyst-optimizable built-ins over row-wise Python UDFs when possible."""
    return (
        enriched.withColumn("amount", F.col("amount_cents") / F.lit(100.0))
        .groupBy("segment", "country")
        .agg(
            F.count("transaction_id").alias("transaction_count"),
            F.sum("amount").alias("gross_volume"),
            F.avg("amount").alias("average_ticket"),
            F.expr("percentile_approx(amount, 0.95)").alias("p95_ticket"),
        )
    )


def write_optimized_parquet(df: DataFrame, path: str, partitions: int = 32) -> None:
    """Avoid uncontrolled tiny-file creation in a toy batch output."""
    (
        df.repartition(partitions, "country")
        .write.mode("overwrite")
        .partitionBy("country")
        .option("compression", "snappy")
        .parquet(path)
    )


def configure_session() -> SparkSession:
    return (
        SparkSession.builder.appName("fintech-spark-performance-lab")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", "64")
        .getOrCreate()
    )


def main() -> None:
    spark = configure_session()

    transactions = spark.read.schema(TRANSACTION_SCHEMA).parquet("data/silver/transaction_minimal")
    customers = spark.read.schema(CUSTOMER_SCHEMA).parquet("data/silver/customer_dimension")

    hot_keys = [row["customer_id"] for row in identify_hot_keys(transactions, 5).collect()]
    print("hot customer keys:", hot_keys)

    broadcasted = broadcast_dimension_join(transactions, customers)
    explain(broadcasted, "Broadcast hash join candidate")

    salted = salted_join(transactions, customers, hot_customer_ids=hot_keys, salt_buckets=8)
    explain(salted, "Salted skew-mitigation join")

    result = aggregate_without_python_udf(salted)
    result.show(50, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
