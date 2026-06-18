-- models/marts/fact_orders.sql

{{ config(materialized='table') }}

WITH staging_order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

staging_orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
)

SELECT
    CONCAT(i.order_id, '-', CAST(i.order_item_id AS STRING)) AS order_item_sk,

    i.order_id AS order_key,
    i.product_id AS product_key,
    i.seller_id AS seller_key,
    o.customer_id AS customer_key,

    CAST(FORMAT_DATE('%Y%m%d', DATE(o.order_purchase_timestamp)) AS STRING) AS date_key,

    i.price,
    i.freight_value,

    (i.price + i.freight_value) AS total_sale_amount,

    SAFE_DIVIDE(
        i.freight_value,
        i.price + i.freight_value
    ) AS freight_percentage

FROM staging_order_items i
LEFT JOIN staging_orders o
    ON i.order_id = o.order_id
WHERE o.order_id IS NOT NULL