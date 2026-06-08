-- models/marts/dim_sellers.sql

{{ config(materialized='table') }}

WITH staging_sellers AS (
    SELECT * FROM {{ ref('stg_sellers') }}
)

SELECT
    seller_id AS seller_key,               -- Using seller_id as our primary key
    seller_city,
    seller_state,
    seller_zip_code_prefix
FROM staging_sellers