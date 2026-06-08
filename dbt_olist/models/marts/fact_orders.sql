-- models/marts/fact_orders.sql

{{ config(materialized='table') }}

WITH staging_order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
)

SELECT
    order_item_id AS order_item_sk,    -- Surrogate Key
    order_id AS order_key,             -- Links to an order dimension if you have one
    product_id AS product_key,         -- Links to dim_products
    seller_id AS seller_key,           -- Links to dim_sellers
    price,
    freight_value
FROM staging_order_items