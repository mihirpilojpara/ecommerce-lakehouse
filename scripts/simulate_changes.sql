-- Simulates a day of activity in the source system so you can watch the
-- pipeline pick up ONLY the changed rows on the next run.
--
-- Run:  docker compose exec postgres psql -U postgres -d olist -f /scripts/simulate_changes.sql

\connect olist

-- 1. 50 shipped orders get delivered today.
UPDATE orders
SET order_status = 'delivered',
    order_delivered_customer_date = now()
WHERE order_id IN (
    SELECT order_id FROM orders WHERE order_status = 'shipped' ORDER BY order_id LIMIT 50
);

-- 2. 20 customers change their review score.
UPDATE order_reviews
SET review_score = LEAST(review_score + 1, 5)
WHERE review_id IN (
    SELECT review_id FROM order_reviews WHERE review_score < 5 ORDER BY review_id LIMIT 20
);

-- 3. A brand-new seller registers.
INSERT INTO sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state)
VALUES (md5(now()::text), '01001', 'sao paulo', 'SP');

SELECT 'orders' AS table_name, count(*) AS rows_changed_last_hour
FROM orders WHERE _updated_at > now() - interval '1 hour'
UNION ALL
SELECT 'order_reviews', count(*) FROM order_reviews WHERE _updated_at > now() - interval '1 hour'
UNION ALL
SELECT 'sellers', count(*) FROM sellers WHERE _updated_at > now() - interval '1 hour';
