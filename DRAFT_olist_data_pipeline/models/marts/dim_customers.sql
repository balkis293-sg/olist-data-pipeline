-- models/marts/dim_customers.sql

{{ config(materialized='table') }}

WITH staging_customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
)

SELECT
    customer_id AS customer_key,       -- Using customer_id as our primary key
    customer_unique_id,
    customer_city,
    customer_state
FROM staging_customers