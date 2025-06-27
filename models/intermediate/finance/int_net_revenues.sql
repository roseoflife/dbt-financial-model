{{
  config(
    materialized='incremental',
    unique_key='transaction_date',
    on_schema_change='fail',
    pre_hook="DELETE FROM {{ this }} WHERE transaction_date >= (SELECT MAX(transaction_date) - INTERVAL '7 days' FROM {{ this }})",
    post_hook="ANALYZE TABLE {{ this }}"
  )
}}

WITH date_filter AS (
  -- Optimization 1: Date filtering to limit data processing window
  SELECT 
    CURRENT_DATE() - INTERVAL '13 months' AS start_date,
    CURRENT_DATE() AS end_date
),

transaction_aggregates AS (
  -- Optimization 2: Pre-aggregate transaction data first
  SELECT 
    t.transaction_date,
    t.customer_id,
    t.product_category,
    SUM(t.transaction_amount) AS gross_amount,
    SUM(t.discount_amount) AS total_discounts,
    SUM(t.tax_amount) AS total_tax,
    COUNT(*) AS transaction_count,
    AVG(t.transaction_amount) AS avg_transaction_amount
  FROM {{ ref('stg_transactions') }} t
  CROSS JOIN date_filter df
  WHERE t.transaction_date >= df.start_date
    AND t.transaction_date <= df.end_date
    {% if is_incremental() %}
      -- Incremental processing: only process new/changed data
      AND t.transaction_date >= (SELECT MAX(transaction_date) FROM {{ this }})
    {% endif %}
  GROUP BY 
    t.transaction_date,
    t.customer_id,
    t.product_category
),

customer_attributes AS (
  -- Optimization 3: Create customer attributes from transaction data
  -- Note: In a real scenario, this would come from a dedicated customers table
  SELECT DISTINCT
    customer_id,
    CASE 
      WHEN customer_id % 3 = 0 THEN 'Enterprise'
      WHEN customer_id % 3 = 1 THEN 'SMB' 
      ELSE 'Individual'
    END AS customer_segment,
    CASE 
      WHEN customer_id % 2 = 0 THEN 'Premium'
      ELSE 'Standard'
    END AS customer_tier,
    CASE 
      WHEN customer_id % 4 = 0 THEN 'North'
      WHEN customer_id % 4 = 1 THEN 'South'
      WHEN customer_id % 4 = 2 THEN 'East'
      ELSE 'West'
    END AS customer_region
  FROM {{ ref('stg_transactions') }}
),

revenue_calculations AS (
  -- Optimization 4: Efficient revenue calculations with proper business logic
  SELECT 
    ta.transaction_date,
    ta.customer_id,
    ca.customer_segment,
    ca.customer_tier,
    ca.customer_region,
    ta.product_category,
    ta.gross_amount,
    ta.total_discounts,
    ta.total_tax,
    ta.transaction_count,
    ta.avg_transaction_amount,
    
    -- Net revenue calculation
    (ta.gross_amount - ta.total_discounts - ta.total_tax) AS net_revenue,
    
    -- Revenue recognition adjustments
    CASE 
      WHEN ta.product_category = 'subscription' THEN 
        (ta.gross_amount - ta.total_discounts - ta.total_tax) / 12.0  -- Monthly recognition
      ELSE 
        (ta.gross_amount - ta.total_discounts - ta.total_tax)  -- Immediate recognition
    END AS recognized_revenue,
    
    -- Performance metrics
    CASE 
      WHEN ta.gross_amount > 0 THEN 
        (ta.total_discounts / ta.gross_amount) * 100 
      ELSE 0 
    END AS discount_percentage,
    
    CURRENT_TIMESTAMP() AS last_updated
    
  FROM transaction_aggregates ta
  LEFT JOIN customer_attributes ca 
    ON ta.customer_id = ca.customer_id
)

SELECT * FROM revenue_calculations

-- Optimization 5: Add clustering for time-series data (Snowflake)
-- This will be applied in dbt_project.yml or as a post-hook