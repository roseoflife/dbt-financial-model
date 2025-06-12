-- Intermediate model: Calculate net revenue from transactions and refunds
{{ config(materialized='table') }}

WITH transactions AS (
  SELECT
    customer_id,
    transaction_date,
    transaction_amount
  FROM {{ ref('stg_transactions') }}
),
refunds AS (
  SELECT
    customer_id,
    refund_date,
    refund_amount
  FROM {{ source('raw', 'refunds') }}
)
SELECT
  t.customer_id,
  t.transaction_date,
  SUM(t.transaction_amount - COALESCE(r.refund_amount, 0)) AS net_revenue
--   {{ calculate_net_revenue('transaction_amount', 'refund_amount') }}
FROM transactions t
LEFT JOIN refunds r
  ON t.customer_id = r.customer_id
  AND t.transaction_date = r.refund_date
GROUP BY t.customer_id, t.transaction_date