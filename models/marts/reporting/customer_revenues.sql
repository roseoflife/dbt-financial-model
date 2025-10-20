-- Mart model: Aggregate revenue by customer for reporting
{{ config(materialized="table") }}
select
    date_trunc('month', transaction_date) as revenue_month,
    customer_id,
    sum(total_revenue) as monthly_revenue,
    sum(transaction_count) as monthly_transactions
from {{ ref("int_net_revenues") }}
group by date_trunc('month', transaction_date), customer_id
