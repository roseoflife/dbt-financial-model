-- Mart model: Aggregate revenue by customer for reporting
{{ config(materialized='table') }}
SELECT 
    DATE_TRUNC('month', transaction_date) AS revenue_month,
    customer_id,
    SUM(daily_revenue) AS monthly_revenue,
    SUM(transaction_count) AS monthly_transactions
FROM {{ ref('int_net_revenues') }}
GROUP BY 
    DATE_TRUNC('month', transaction_date),
    customer_id