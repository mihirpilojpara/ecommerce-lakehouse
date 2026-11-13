-- A singular test: it fails if this query returns any rows.
select *
from {{ ref('mart_seller_performance') }}
where late_delivery_rate < 0 or late_delivery_rate > 1
