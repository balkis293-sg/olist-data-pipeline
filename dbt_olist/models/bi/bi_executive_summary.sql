{{ config(materialized='view') }}

select
    count(*) as total_customers,
    sum(monetary_value) as total_revenue,
    avg(monetary_value) as avg_customer_value,
    avg(avg_order_value) as avg_order_value,
    avg(frequency) as avg_frequency,
    avg(recency_days) as avg_recency_days,

    countif(customer_segment = 'Champions') as champion_customers,
    countif(customer_segment = 'Loyal Customers') as loyal_customers,
    countif(customer_segment = 'At-Risk Customers') as at_risk_customers,
    countif(customer_segment = 'Lost Customers') as lost_customers,

    round(
        countif(customer_segment = 'Champions') * 100.0 / count(*),
        2
    ) as pct_champions,

    round(
        countif(customer_segment in ('At-Risk Customers', 'Lost Customers')) * 100.0 / count(*),
        2
    ) as pct_churn_risk_customers

from {{ ref('bi_customer_segments') }}