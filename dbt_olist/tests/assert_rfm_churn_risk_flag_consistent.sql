select
customer_unique_id,
recency_days,
is_churn_risk
from {{ ref('fct_customer_rfm') }}
where (recency_days > 180 and is_churn_risk = false)
or (recency_days <= 180 and is_churn_risk = true)