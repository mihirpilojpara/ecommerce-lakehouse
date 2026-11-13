from datetime import datetime

from lakehouse.transforms.silver import (
    clean_customers,
    clean_geolocation,
    clean_products,
    dedupe_latest,
)

T1 = datetime(2026, 1, 1, 10, 0)
T2 = datetime(2026, 1, 2, 10, 0)


def test_dedupe_latest_keeps_most_recent_version(spark):
    df = spark.createDataFrame(
        [("o1", "shipped", T1, T1), ("o1", "delivered", T2, T2), ("o2", "created", T1, T1)],
        "order_id string, order_status string, _updated_at timestamp, _ingested_at timestamp",
    )
    result = {r.order_id: r.order_status for r in dedupe_latest(df, ["order_id"]).collect()}
    assert result == {"o1": "delivered", "o2": "created"}


def test_clean_customers_normalises_text_and_zip(spark):
    df = spark.createDataFrame(
        [(" c1 ", "u1", "1037", "  Sao Paulo ", "sp", T1, T1)],
        "customer_id string, customer_unique_id string, customer_zip_code_prefix string, "
        "customer_city string, customer_state string, _updated_at timestamp, _ingested_at timestamp",
    )
    row = clean_customers(df).first()
    assert row.customer_id == "c1"
    assert row.customer_zip_code_prefix == "01037"
    assert row.customer_city == "sao paulo"
    assert row.customer_state == "SP"


def test_clean_products_fixes_column_names_and_missing_category(spark):
    df = spark.createDataFrame(
        [("p1", None, 40, 300, 1, 500, 10, 10, 10, T1, T1)],
        "product_id string, product_category_name string, product_name_lenght int, "
        "product_description_lenght int, product_photos_qty int, product_weight_g int, "
        "product_length_cm int, product_height_cm int, product_width_cm int, "
        "_updated_at timestamp, _ingested_at timestamp",
    )
    row = clean_products(df).first()
    assert row.product_category_name == "unknown"
    assert row.product_name_length == 40
    assert "product_name_lenght" not in clean_products(df).columns


def test_clean_geolocation_drops_points_outside_brazil_and_averages(spark):
    df = spark.createDataFrame(
        [
            ("01037", -23.0, -46.0, "sao paulo", "SP", T1, T1),
            ("01037", -24.0, -47.0, "sao paulo", "SP", T1, T1),
            ("01037", 41.0, -8.0, "porto", "SP", T1, T1),  # a bad point in Portugal
        ],
        "geolocation_zip_code_prefix string, geolocation_lat double, geolocation_lng double, "
        "geolocation_city string, geolocation_state string, _updated_at timestamp, _ingested_at timestamp",
    )
    rows = clean_geolocation(df).collect()
    assert len(rows) == 1
    assert rows[0].lat == -23.5
    assert rows[0].lng == -46.5
