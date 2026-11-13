-- Reconciliation: total revenue in the monthly mart must equal the total in the
-- gold fact table (same filters). Catches rows lost or duplicated by joins.
with mart as (
    select sum(product_revenue) as revenue from {{ ref('mart_monthly_revenue') }}
),
gold as (
    select round(sum(price), 2) as revenue
    from {{ source('gold', 'fact_order_items') }}
    where order_status not in ('canceled', 'unavailable')
)
select mart.revenue as mart_revenue, gold.revenue as gold_revenue
from mart, gold
where abs(mart.revenue - gold.revenue) > 1
