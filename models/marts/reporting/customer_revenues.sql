{{
  config(
    materialized='incremental',
    unique_key=['customer_id', 'revenue_month']
  )
}}

SELECT 
    DATE_TRUNC('month', transaction_date) AS revenue_month,
    customer_id,
    SUM(total_revenue) AS monthly_revenue,
    SUM(transaction_count) AS monthly_transactions
FROM {{ ref('int_net_revenues') }}
{% if is_incremental() %}
  -- Only run this filter on subsequent runs (not first run)
  WHERE transaction_date >= (
    SELECT DATEADD('month', -1, MAX(revenue_month)) 
    FROM {{ this }}
  )
{% endif %}
GROUP BY 
    DATE_TRUNC('month', transaction_date),
    customer_id