with base as (
    select
        customer_id,
        date_trunc('day', event_time) as event_date,
        count(*) as transaction_count,
        sum(amount) as total_amount,
        avg(amount) as average_amount,
        max(amount) as max_amount,
        count(distinct merchant_id) as unique_merchants,
        count(*) filter (where channel = 'atm') as atm_transactions
    from {{ ref('stg_transactions') }}
    group by 1, 2
),

features as (
    select
        *,
        avg(total_amount) over (
            partition by customer_id
            order by event_date
            rows between 6 preceding and current row
        ) as rolling_7d_spend,
        avg(transaction_count) over (
            partition by customer_id
            order by event_date
            rows between 29 preceding and current row
        ) as rolling_30d_txn_count
    from base
)

select * from features
