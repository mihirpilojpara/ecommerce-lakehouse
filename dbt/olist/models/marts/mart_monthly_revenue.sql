-- Revenue, orders and customer satisfaction per month.
select
    make_date(purchase_year, purchase_month, 1) as month_start,
    count(distinct order_id) as orders,
    count(*) as items_sold,
    count(distinct customer_unique_id) as customers,
    round(sum(price), 2) as product_revenue,
    round(sum(freight_value), 2) as freight_revenue,
    round(sum(item_total) / count(distinct order_id), 2) as avg_order_value,
    round(avg(review_score), 2) as avg_review_score
from {{ ref('stg_sales') }}
group by 1
order by 1
