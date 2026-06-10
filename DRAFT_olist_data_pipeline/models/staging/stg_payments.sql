{{ config(materialized='view') }}

select
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    cast(payment_value as numeric) as payment_value
from {{ source('olist_raw', 'order_payments') }}
where order_id is not null