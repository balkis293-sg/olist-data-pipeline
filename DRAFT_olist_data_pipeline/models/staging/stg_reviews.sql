{{ config(materialized='view') }}

select
    order_id,
    review_id,
    review_score,
    review_comment_title,
    review_comment_message
from {{ source('olist_raw', 'order_reviews') }}
where order_id is not null