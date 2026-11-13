from datetime import date, datetime
from decimal import Decimal

from lakehouse.transforms.gold import build_dim_date, build_fact_order_items

ORDERS_SCHEMA = (
    "order_id string, customer_id string, order_status string, "
    "order_purchase_timestamp timestamp, order_approved_at timestamp, "
    "order_delivered_carrier_date timestamp, order_delivered_customer_date timestamp, "
    "order_estimated_delivery_date timestamp, _updated_at timestamp, _ingested_at timestamp"
)
ITEMS_SCHEMA = (
    "order_id string, order_item_id int, product_id string, seller_id string, "
    "shipping_limit_date timestamp, price decimal(12,2), freight_value decimal(12,2), "
    "_updated_at timestamp, _ingested_at timestamp"
)
PAYMENTS_SCHEMA = (
    "order_id string, payment_sequential int, payment_type string, "
    "payment_installments int, payment_value decimal(12,2)"
)
REVIEWS_SCHEMA = "review_id string, order_id string, review_score int"

BOUGHT = datetime(2026, 3, 1, 12, 0)
T = datetime(2026, 3, 1)


def _fact(spark, delivered, estimated):
    orders = spark.createDataFrame(
        [("o1", "c1", "delivered", BOUGHT, None, None, delivered, estimated, T, T)], ORDERS_SCHEMA
    )
    items = spark.createDataFrame(
        [
            ("o1", 1, "p1", "s1", None, Decimal("8.00"), Decimal("2.00"), T, T),
            ("o1", 2, "p2", "s1", None, Decimal("15.00"), Decimal("5.00"), T, T),
        ],
        ITEMS_SCHEMA,
    )
    payments = spark.createDataFrame([("o1", 1, "credit_card", 3, Decimal("30.00"))], PAYMENTS_SCHEMA)
    reviews = spark.createDataFrame([("r1", "o1", 4)], REVIEWS_SCHEMA)
    return build_fact_order_items(orders, items, payments, reviews)


def test_fact_has_one_row_per_item(spark):
    fact = _fact(spark, datetime(2026, 3, 5), datetime(2026, 3, 10))
    assert fact.count() == 2
    assert fact.select("order_item_sk").distinct().count() == 2


def test_payment_is_split_across_items_by_value(spark):
    fact = _fact(spark, datetime(2026, 3, 5), datetime(2026, 3, 10))
    allocated = {r.order_item_id: r.allocated_payment_value for r in fact.collect()}
    # item totals are 10 and 20 out of 30, payment is 30
    assert allocated == {1: 10.0, 2: 20.0}


def test_late_delivery_flag_and_days(spark):
    on_time = _fact(spark, datetime(2026, 3, 5), datetime(2026, 3, 10)).first()
    late = _fact(spark, datetime(2026, 3, 12), datetime(2026, 3, 10)).first()
    not_delivered = _fact(spark, None, datetime(2026, 3, 10)).first()

    assert on_time.is_late is False
    assert on_time.delivery_days == 4
    assert late.is_late is True
    assert not_delivered.is_late is None
    assert on_time.purchase_date_key == 20260301


def test_dim_date_covers_every_day(spark):
    dim = build_dim_date(spark, date(2026, 2, 27), date(2026, 3, 2))
    keys = [r.date_key for r in dim.orderBy("date_key").collect()]
    assert keys == [20260227, 20260228, 20260301, 20260302]
    weekend = {r.date_key: r.is_weekend for r in dim.collect()}
    assert weekend[20260228] is True  # Saturday
    assert weekend[20260302] is False  # Monday
