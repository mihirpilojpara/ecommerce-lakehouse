-- Which product categories sell the most, and how happy customers are with them.
select
    category,
    count(distinct order_id) as orders,
    count(*) as items_sold,
    round(sum(price), 2) as revenue,
    round(avg(price), 2) as avg_price,
    round(avg(review_score), 2) as avg_review_score,
    round(avg(delivery_days), 1) as avg_delivery_days
from {{ ref('stg_sales') }}
group by 1
order by revenue desc
