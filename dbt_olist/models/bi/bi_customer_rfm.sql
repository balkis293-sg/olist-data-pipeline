{{ config(materialized='view') }}

with customer_orders as (

    select
        c.customer_unique_id,
        c.customer_key as customer_id,
        c.customer_city,
        c.customer_state,
        f.order_key,
        d.full_date as purchase_date,
        sum(f.price + f.freight_value) as order_value

    from {{ ref('fact_orders') }} f
    left join {{ ref('dim_customers') }} c
        on f.customer_key = c.customer_key
    left join {{ ref('dim_date') }} d
        on f.date_key = d.date_key

    group by
        c.customer_unique_id,
        c.customer_key,
        c.customer_city,
        c.customer_state,
        f.order_key,
        d.full_date

),

analysis_date as (

    select
        date_add(max(purchase_date), interval 1 day) as analysis_date
    from customer_orders

),

rfm as (

    select
        customer_unique_id,
        any_value(customer_id) as customer_id,
        any_value(customer_city) as customer_city,
        any_value(customer_state) as customer_state,

        min(purchase_date) as first_purchase_date,
        max(purchase_date) as last_purchase_date,

        date_diff(
            (select a.analysis_date from analysis_date a),
            max(purchase_date),
            day
        ) as recency_days,

        count(distinct order_key) as frequency,
        sum(order_value) as monetary_value,
        avg(order_value) as avg_order_value

    from customer_orders
    group by customer_unique_id

)

select *
from rfm