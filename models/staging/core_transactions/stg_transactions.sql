-- Staging model: Lightly transform raw transaction data
{{ config(materialized="view") }}

select
    transaction_id,
    customer_id,
    transaction_date::date as transaction_date,
    amount::float as transaction_amount,
    currency as transaction_currency
from {{ source("raw", "transactions") }}
where amount is not null
