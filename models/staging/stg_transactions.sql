# models/staging/stg_transactions.sql
-- Staging model: Lightly transform raw transaction data
{{ config(materialized='view') }}

SELECT
  transaction_id,
  customer_id,
  transaction_date::DATE AS transaction_date,
  amount::FLOAT AS transaction_amount,
  currency AS transaction_currency
FROM {{ source('raw', 'transactions') }}
WHERE amount IS NOT NULL
# models/staging/stg_transactions.sql end