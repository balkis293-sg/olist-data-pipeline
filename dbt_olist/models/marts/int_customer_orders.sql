{{ config(materialized='view') }}

with orders as (
    select *
    from {{ ref('stg_orders') }}
),

customers as (
    select *
    from {{ ref('stg_customers') }}
),

payments as (
    select *
    from {{ ref('int_order_payments') }}
)

select
    c.customer_unique_id,
    c.customer_id,
    c.customer_city,
    c.customer_state,
    o.order_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_purchase_date,
    coalesce(p.order_revenue, 0) as order_revenue
from orders o
left join customers c
    on o.customer_id = c.customer_id
left join payments p
    on o.order_id = p.order_id
where o.order_status = 'delivered'