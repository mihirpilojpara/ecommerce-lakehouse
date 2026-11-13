-- Sold items joined to their dimensions. Canceled and unavailable orders are
-- excluded because they never produced revenue.
select
    f.order_id,
    f.order_item_id,
    d.date as purchase_date,
    d.year as purchase_year,
    d.month as purchase_month,
    c.customer_unique_id,
    c.customer_state,
    p.category_name_english as category,
    s.seller_id,
    s.seller_state,
    f.order_status,
    f.price,
    f.freight_value,
    f.item_total,
    f.allocated_payment_value,
    f.review_score,
    f.delivery_days,
    f.estimated_delivery_days,
    f.is_late
from {{ source('gold', 'fact_order_items') }} as f
join {{ source('gold', 'dim_date') }} as d on f.purchase_date_key = d.date_key
join {{ source('gold', 'dim_customer') }} as c on f.customer_sk = c.customer_sk
join {{ source('gold', 'dim_product') }} as p on f.product_sk = p.product_sk
join {{ source('gold', 'dim_seller') }} as s on f.seller_sk = s.seller_sk
where f.order_status not in ('canceled', 'unavailable')
