# Spark Performance Playbook for Banking Data

This note accompanies `banking_features.py` and `performance_lab.py`. It is written as an interview and debugging reference rather than a claim that one tuning recipe fits every cluster.

## Mental model

Spark jobs are slow for concrete reasons: too much data read, expensive shuffles, skewed partitions, bad join strategies, excessive serialization, too many tiny files, memory pressure, repeated recomputation, or Python/JVM boundaries.

The first step is not changing random config values. Inspect the **physical plan**, stage metrics and data distribution.

## 1. Read less data

Prefer columnar formats such as Parquet and select only required columns.

```python
transactions = (
    spark.read.parquet(path)
    .select("transaction_id", "customer_id", "event_time", "amount", "country")
    .filter(F.col("event_time") >= F.lit(cutoff))
)
```

Why it helps:

- column pruning reduces bytes read;
- partition pruning can skip directories/files;
- predicate pushdown can reduce row decoding.

## 2. Understand shuffle

Operations that commonly introduce shuffle include:

- `groupBy`;
- `distinct` / `dropDuplicates`;
- many joins;
- global sorts;
- repartitioning;
- some window functions.

A shuffle writes intermediate data and redistributes it across executors. The network/disk/serialization cost can dominate the business logic itself.

## 3. Broadcast dimensions when genuinely small

```python
fact.join(F.broadcast(customer_dimension), "customer_id")
```

This avoids shuffling the large fact side. It is appropriate only when the small side can safely fit into executor memory.

Do not force broadcast because the table has a comforting name like `dimension`; measure its actual size.

## 4. Diagnose skew before salting

```python
transactions.groupBy("customer_id").count().orderBy(F.desc("count")).show(20)
```

If one customer or merchant owns a huge fraction of events, one shuffle partition can become the straggler that determines stage runtime.

Potential mitigations:

- Adaptive Query Execution skew join handling;
- broadcast the other side;
- preaggregate before the join;
- use a better partition key;
- salt only known hot keys;
- isolate pathological keys into a separate path.

`performance_lab.py` demonstrates controlled salting of hot customer IDs.

## 5. Salting trade-off

Salting distributes a hot key across multiple partition keys, but the matching dimension rows must be replicated across salts.

It exchanges **controlled duplication** for **less concentrated shuffle work**. It is not free and should not be applied blindly to every key.

## 6. AQE

Useful settings in modern Spark:

```text
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.skewJoin.enabled=true
```

Adaptive Query Execution can change join strategies and partition counts using runtime statistics. It reduces the need for some manual tuning, but it does not rescue poor data modeling or extreme skew automatically.

## 7. Partition count

Too few partitions:

- low parallelism;
- huge task memory footprint;
- long stragglers.

Too many partitions:

- scheduler overhead;
- tiny output files;
- excessive task startup cost.

`spark.sql.shuffle.partitions` should reflect workload size and cluster resources, not a memorized interview number.

## 8. `repartition` vs `coalesce`

`repartition` performs shuffle and can increase or decrease partition count. Use it when redistributing data by a key is important.

`coalesce` is typically used to reduce partitions with less movement. It can produce uneven partitions if used carelessly.

## 9. Avoid Python UDFs when built-ins work

Prefer:

```python
F.log1p("amount")
F.when(...)
F.expr("percentile_approx(amount, 0.95)")
```

over row-wise Python UDFs when the computation can be expressed using Spark SQL functions.

Built-ins remain visible to Catalyst and execute efficiently in the JVM engine.

## 10. Cache deliberately

Caching can help when the same expensive intermediate result is consumed multiple times.

It hurts when:

- the dataframe is used once;
- it evicts more valuable data;
- serialization/storage overhead exceeds recomputation cost.

Ask: **what repeated computation am I eliminating?**

## 11. Tiny files

Banking pipelines can produce huge numbers of tiny Parquet files when every microbatch/partition writes independently.

Consequences:

- expensive metadata/listing operations;
- inefficient scans;
- poor downstream planning.

Control output partitioning and run compaction where architecture requires it.

## 12. Windows and leakage

Rolling banking features often use window functions. Two separate concerns matter:

1. performance: partition/sort cost;
2. correctness: current/future observations must not leak into features.

Example:

```python
previous_30d = (
    Window.partitionBy("customer_id")
    .orderBy(F.col("event_time").cast("long"))
    .rangeBetween(-30 * 86400, -1)
)
```

The `-1` upper bound intentionally excludes the current event.

## Interview checklist

When asked "this Spark job is slow, what do you do?", walk through:

1. inspect physical plan with `explain("formatted")`;
2. check bytes/files/columns being read;
3. find shuffle-heavy stages;
4. inspect partition sizes and skew;
5. inspect join strategy;
6. confirm AQE configuration;
7. evaluate partition counts;
8. remove unnecessary Python UDFs/actions;
9. check spill, executor memory and GC;
10. address tiny files / output layout;
11. measure again after one change rather than tuning blindly.
