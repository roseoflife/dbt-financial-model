{{
  config(
    materialized='incremental',
    unique_key=['customer_id', 'revenue_month'],
    on_schema_change='fail'
  )
}}

WITH date_spine AS (
  -- Optimization 1: Generate date spine for consistent monthly reporting
  SELECT DISTINCT
    DATE_TRUNC('month', transaction_date) AS date_month
  FROM {{ ref('int_net_revenues') }}
  WHERE transaction_date >= DATEADD('year', -2, CURRENT_DATE())
),

customer_monthly_base AS (
  -- Optimization 2: Monthly aggregation with efficient window functions
  SELECT 
    DATE_TRUNC('month', transaction_date) AS revenue_month,
    customer_id,
    customer_segment,
    customer_tier,
    customer_region,
    
    -- Revenue metrics
    SUM(net_revenue) AS monthly_net_revenue,
    SUM(recognized_revenue) AS monthly_recognized_revenue,
    SUM(gross_amount) AS monthly_gross_revenue,
    SUM(total_discounts) AS monthly_discounts,
    SUM(transaction_count) AS monthly_transaction_count,
    
    -- Average metrics
    AVG(avg_transaction_amount) AS avg_transaction_value,
    AVG(discount_percentage) AS avg_discount_rate
    
  FROM {{ ref('int_net_revenue') }}
  {% if is_incremental() %}
    WHERE transaction_date >= (
      SELECT DATEADD('month', -3, MAX(revenue_month)) 
      FROM {{ this }}
    )
  {% endif %}
  GROUP BY 1, 2, 3, 4, 5
),

customer_revenue_metrics AS (
  -- Optimization 3: Calculate advanced customer metrics efficiently
  SELECT 
    cmb.*,
    
    -- Customer lifetime metrics (using window functions for performance)
    SUM(monthly_net_revenue) OVER (
      PARTITION BY customer_id 
      ORDER BY revenue_month 
      ROWS UNBOUNDED PRECEDING
    ) AS customer_lifetime_revenue,
    
    -- Monthly growth calculations
    LAG(monthly_net_revenue, 1) OVER (
      PARTITION BY customer_id 
      ORDER BY revenue_month
    ) AS previous_month_revenue,
    
    -- Calculate revenue trend
    CASE 
      WHEN LAG(monthly_net_revenue, 1) OVER (
        PARTITION BY customer_id ORDER BY revenue_month
      ) > 0 THEN
        ((monthly_net_revenue - LAG(monthly_net_revenue, 1) OVER (
          PARTITION BY customer_id ORDER BY revenue_month
        )) / LAG(monthly_net_revenue, 1) OVER (
          PARTITION BY customer_id ORDER BY revenue_month
        )) * 100
      ELSE NULL
    END AS revenue_growth_rate,
    
    -- Customer segment performance
    AVG(monthly_net_revenue) OVER (
      PARTITION BY customer_segment, revenue_month
    ) AS segment_avg_revenue,
    
    -- Ranking within segment
    RANK() OVER (
      PARTITION BY customer_segment, revenue_month 
      ORDER BY monthly_net_revenue DESC
    ) AS revenue_rank_in_segment,
    
    -- Customer status classification
    CASE 
      WHEN monthly_net_revenue > 0 THEN 'Active'
      WHEN customer_lifetime_revenue > 0 THEN 'Dormant'
      ELSE 'New'
    END AS customer_status,
    
    CURRENT_TIMESTAMP() AS last_updated
    
  FROM customer_monthly_base cmb
),

final_customer_revenues AS (
  -- Optimization 4: Final transformations and data quality checks
  SELECT 
    revenue_month,
    customer_id,
    customer_segment,
    customer_tier,
    customer_region,
    customer_status,
    
    -- Revenue metrics
    COALESCE(monthly_net_revenue, 0) AS monthly_net_revenue,
    COALESCE(monthly_recognized_revenue, 0) AS monthly_recognized_revenue,
    COALESCE(monthly_gross_revenue, 0) AS monthly_gross_revenue,
    COALESCE(monthly_discounts, 0) AS monthly_discounts,
    COALESCE(monthly_transaction_count, 0) AS monthly_transaction_count,
    
    -- Calculated metrics
    COALESCE(customer_lifetime_revenue, 0) AS customer_lifetime_revenue,
    COALESCE(revenue_growth_rate, 0) AS revenue_growth_rate,
    COALESCE(avg_transaction_value, 0) AS avg_transaction_value,
    COALESCE(avg_discount_rate, 0) AS avg_discount_rate,
    
    -- Performance indicators
    COALESCE(segment_avg_revenue, 0) AS segment_avg_revenue,
    COALESCE(revenue_rank_in_segment, 999) AS revenue_rank_in_segment,
    
    -- Flags for business logic
    CASE 
      WHEN monthly_net_revenue >= segment_avg_revenue THEN TRUE 
      ELSE FALSE 
    END AS above_segment_average,
    
    CASE 
      WHEN revenue_growth_rate > 10 THEN 'Growing'
      WHEN revenue_growth_rate BETWEEN -10 AND 10 THEN 'Stable'
      ELSE 'Declining'
    END AS revenue_trend,
    
    last_updated
    
  FROM customer_revenue_metrics
)

SELECT * FROM final_customer_revenues