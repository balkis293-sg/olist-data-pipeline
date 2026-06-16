select
customer_unique_id,
frequency,
is_repeat_buyer
from {{ ref('fct_customer_rfm') }}
where (frequency >= 2 and is_repeat_buyer = false)
or (frequency < 2 and is_repeat_buyer = true)