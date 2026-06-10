-- models/marts/fact_orders.sql

{{ config(materialized='table') }}

WITH staging_order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

staging_orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
)

SELECT
    -- Primary Key for the Fact Table
    i.order_item_id AS order_item_sk,    
    
    -- Foreign Keys connecting to your Dimensions
    i.order_id AS order_key,             
    i.product_id AS product_key,         
    i.seller_id AS seller_key,           
    o.customer_id AS customer_key,       
    
    -- Formatting the timestamp into a string 'YYYYMMDD' to match dim_date
    CAST(FORMAT_DATE('%Y%m%d', DATE(o.order_purchase_timestamp)) AS STRING) AS date_key,
    
    -- Your Facts (Measurable Numbers)
    i.price,
    i.freight_value

FROM staging_order_items i
-- We join the orders table to bring in the customer and date information!
LEFT JOIN staging_orders o 
    ON i.order_id = o.order_id