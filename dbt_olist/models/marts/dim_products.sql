-- models/marts/dim_products.sql

{{ config(materialized='table') }}

WITH staging_products AS (
    SELECT * FROM {{ ref('stg_products') }}
)

SELECT
    product_id AS product_key,             -- Using product_id as our primary key
    product_category_name AS category_name,
    product_weight_g AS weight_g
    
    -- Note: If Lizhou or Balkis already joined the English translations 
    -- in their 'stg_products' file, you can also add that column here like this:
    -- category_english
FROM staging_products