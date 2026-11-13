-- How each seller performs on sales, reviews and on-time delivery.
select
    seller_id,
    seller_state,
    count(distinct order_id) as orders,
    round(sum(price), 2) as revenue,
    round(avg(review_score), 2) as avg_review_score,
    round(avg(case when is_late then 1.0 else 0.0 end) filter (where is_late is not null), 4)
        as late_delivery_rate
from {{ ref('stg_sales') }}
group by 1, 2
