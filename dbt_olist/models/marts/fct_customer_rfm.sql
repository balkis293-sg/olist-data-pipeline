{{ config(materialized='table') }}

with customer_orders as (
    select *
    from {{ ref('int_customer_orders') }}
),

analysis_date as (
    select max(order_purchase_date) as max_order_date
    from customer_orders
),

rfm as (
    select
        customer_unique_id,
        min(order_purchase_date) as first_purchase_date,
        max(order_purchase_date) as last_purchase_date,

        date_diff(
            (select max_order_date from analysis_date),
            max(order_purchase_date),
            day
        ) as recency_days,

        count(distinct order_id) as frequency,
        sum(coalesce(order_revenue, 0)) as monetary_value, 
        avg(coalesce(order_revenue, 0)) as avg_order_value
    from customer_orders
    group by customer_unique_id
)

select
    customer_unique_id,
    first_purchase_date,
    last_purchase_date,
    recency_days,
    frequency,
    monetary_value,
    avg_order_value,

    case
        when frequency >= 2 then true
        else false
    end as is_repeat_buyer,

    case
        when recency_days > 180 then true
        else false
    end as is_churn_risk,

    case
        when recency_days <= 90 and frequency >= 2 and monetary_value >= 300 then 'Champions'
        when recency_days <= 180 and frequency >= 2 then 'Loyal Customers'
        when recency_days > 180 and frequency >= 2 then 'At-Risk Customers'
        when frequency = 1 and recency_days > 180 then 'Lost Customers'
        else 'New / Low Activity Customers'
    end as customer_segment
from rfm