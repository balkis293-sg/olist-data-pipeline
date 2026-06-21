{{ config(materialized='view') }}

with rfm as (

    select *
    from {{ ref('bi_customer_rfm') }}

),

scored as (

    select
        *,

        ntile(5) over (order by recency_days desc) as recency_score,
        ntile(5) over (order by frequency asc) as frequency_score,
        ntile(5) over (order by monetary_value asc) as monetary_score

    from rfm

),

segmented as (

    select
        *,

        case
            when recency_score >= 4
                 and frequency_score >= 4
                 and monetary_score >= 4
                then 'Champions'

            when recency_score >= 3
                 and frequency_score >= 3
                then 'Loyal Customers'

            when recency_score <= 2
                 and frequency_score >= 3
                then 'At-Risk Customers'

            when recency_score <= 2
                 and frequency_score <= 2
                then 'Lost Customers'

            when recency_score >= 4
                 and frequency_score <= 2
                then 'Promising'

            else 'Need Attention'
        end as customer_segment,

        recency_score + frequency_score + monetary_score as rfm_total_score

    from scored

)

select *
from segmented