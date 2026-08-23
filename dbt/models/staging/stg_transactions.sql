select
    cast(event_id as varchar) as event_id,
    cast(customer_id as varchar) as customer_id,
    cast(event_time as timestamptz) as event_time,
    cast(amount as double) as amount,
    cast(currency as varchar) as currency,
    cast(merchant_id as varchar) as merchant_id,
    cast(country as varchar) as country,
    cast(channel as varchar) as channel,
    cast(event_date as date) as event_date,
    cast(ingestion_run_id as varchar) as ingestion_run_id
from read_parquet(
    '{{ env_var("SILVER_GLOB", "../data/silver/transactions/**/*.parquet") }}',
    hive_partitioning = true
)
