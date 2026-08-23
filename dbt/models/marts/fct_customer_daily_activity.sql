select
    customer_id,
    event_date,
    count(*) as transaction_count,
    round(sum(amount), 2) as gross_amount,
    round(avg(amount), 2) as average_amount,
    count(distinct merchant_id) as merchant_count,
    count(distinct country) as country_count,
    sum(case when channel = 'bank_transfer' then 1 else 0 end) as transfer_count
from {{ ref('stg_transactions') }}
group by 1, 2
