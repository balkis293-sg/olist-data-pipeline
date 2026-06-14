{{ config(materialized='view') }}

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    cast(shipping_limit_date as timestamp) as shipping_limit_date,
    cast(price as numeric) as price,
    cast(freight_value as numeric) as freight_value,
    cast(price as numeric) + cast(freight_value as numeric) as total_item_value
from {{ source('olist_raw', 'order_items') }}
where order_id is not null and product_id is not null and seller_id is not null