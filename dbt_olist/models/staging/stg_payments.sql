{{ config(materialized='view') }}

select
    trim(order_id) as order_id,
    cast(payment_sequential as int64) as payment_sequential,
    lower(trim(payment_type)) as payment_type,
    cast(payment_installments as int64) as payment_installments,
    cast(payment_value as numeric) as payment_value
from {{ source('olist_raw', 'order_payments') }}
where order_id is not null
  and payment_value is not null
  and cast(payment_value as numeric) >= 0