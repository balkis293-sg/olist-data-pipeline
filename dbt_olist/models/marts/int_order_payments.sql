{{ config(materialized='view') }}

select
    order_id,
    sum(payment_value) as order_revenue,
    count(*) as payment_row_count,
    max(payment_installments) as max_payment_installments
from {{ ref('stg_payments') }}
group by order_id