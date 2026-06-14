{{ config(materialized='view') }}

select
    trim(customer_id) as customer_id,
    trim(customer_unique_id) as customer_unique_id,
    cast(customer_zip_code_prefix as int64) as customer_zip_code_prefix,
    lower(trim(customer_city)) as customer_city,
    upper(trim(customer_state)) as customer_state
from {{ source('olist_raw', 'customers') }}
where customer_id is not null
  and customer_unique_id is not null