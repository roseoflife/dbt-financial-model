-- Mart model: Aggregate revenue by customer for reporting
{{ config(materialized='table') }}

SELECT
  customer_id,
  DATE_TRUNC('month', transaction_date) AS month,
  SUM(net_revenue) AS total_revenue
FROM {{ ref('int_net_revenue') }}
GROUP BY customer_id, month
ORDER BY customer_id, month