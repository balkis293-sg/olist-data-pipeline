{{ config(materialized='view') }}

select
    customer_segment,
    count(*) as customer_count,
    sum(monetary_value) as total_revenue,
    avg(monetary_value) as avg_customer_value,
    avg(frequency) as avg_frequency,
    avg(recency_days) as avg_recency_days,

    round(
        count(*) * 100.0 / sum(count(*)) over (),
        2
    ) as pct_customers,

    round(
        sum(monetary_value) * 100.0 / sum(sum(monetary_value)) over (),
        2
    ) as pct_revenue

from {{ ref('bi_customer_segments') }}

group by customer_segment