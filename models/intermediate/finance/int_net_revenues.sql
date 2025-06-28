{{
  config(
    materialized='view',
    unique_key=['customer_id', 'transaction_date', 'transaction_currency']
  )
}}

SELECT 
    t.transaction_date,
    t.customer_id,
    t.transaction_currency,
    -- SUM(t.transaction_amount) AS daily_revenue,
    SUM(t.transaction_amount) AS total_revenue,
    COUNT(*) AS transaction_count,
    AVG(t.transaction_amount) AS avg_amount
FROM {{ ref('stg_transactions') }} t
WHERE t.transaction_date >= CURRENT_DATE() - INTERVAL '13 months'
{% if is_incremental() %}
  AND t.transaction_date >= (SELECT MAX(transaction_date) FROM {{ this }})
{% endif %}
GROUP BY 
    t.transaction_date,
    t.customer_id,
    t.transaction_currency