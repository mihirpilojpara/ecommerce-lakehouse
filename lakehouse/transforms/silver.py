"""Bronze -> silver cleaning.

Each clean_* function takes raw bronze rows and returns rows with:
  * explicit column types (schema enforcement)
  * trimmed / normalised text (city names lower case, states upper case)
  * fixed column names (the source spells "length" as "lenght")
  * lineage columns kept: _updated_at (from source) and _ingested_at (bronze)

They are pure functions on DataFrames, so they are easy to unit test.
"""

from collections.abc import Callable, Sequence

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


def _lineage() -> list[Column]:
    # A function, not a module constant: Column objects need an active SparkSession.
    return [F.col("_updated_at").cast("timestamp").alias("_updated_at"), F.col("_ingested_at")]


def _text(c: str) -> Column:
    return F.trim(F.col(c))


def _lower(c: str) -> Column:
    return F.lower(F.trim(F.col(c)))


def _upper(c: str) -> Column:
    return F.upper(F.trim(F.col(c)))


def _zip(c: str) -> Column:
    # Zip prefixes like "01037" lose the leading zero if anything treats them as numbers.
    return F.lpad(F.trim(F.col(c).cast("string")), 5, "0")


def _ts(c: str) -> Column:
    return F.col(c).cast("timestamp")


def _money(c: str) -> Column:
    return F.col(c).cast("decimal(12,2)")


def dedupe_latest(df: DataFrame, keys: Sequence[str], order_col: str = "_updated_at") -> DataFrame:
    """Keep one row per key: the most recently updated (then most recently ingested)."""
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc(), F.col("_ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def clean_customers(df: DataFrame) -> DataFrame:
    return df.select(
        _text("customer_id").alias("customer_id"),
        _text("customer_unique_id").alias("customer_unique_id"),
        _zip("customer_zip_code_prefix").alias("customer_zip_code_prefix"),
        _lower("customer_city").alias("customer_city"),
        _upper("customer_state").alias("customer_state"),
        *_lineage(),
    )


def clean_sellers(df: DataFrame) -> DataFrame:
    return df.select(
        _text("seller_id").alias("seller_id"),
        _zip("seller_zip_code_prefix").alias("seller_zip_code_prefix"),
        _lower("seller_city").alias("seller_city"),
        _upper("seller_state").alias("seller_state"),
        *_lineage(),
    )


def clean_orders(df: DataFrame) -> DataFrame:
    return df.select(
        _text("order_id").alias("order_id"),
        _text("customer_id").alias("customer_id"),
        _lower("order_status").alias("order_status"),
        _ts("order_purchase_timestamp").alias("order_purchase_timestamp"),
        _ts("order_approved_at").alias("order_approved_at"),
        _ts("order_delivered_carrier_date").alias("order_delivered_carrier_date"),
        _ts("order_delivered_customer_date").alias("order_delivered_customer_date"),
        _ts("order_estimated_delivery_date").alias("order_estimated_delivery_date"),
        *_lineage(),
    )


def clean_order_items(df: DataFrame) -> DataFrame:
    return df.select(
        _text("order_id").alias("order_id"),
        F.col("order_item_id").cast("int").alias("order_item_id"),
        _text("product_id").alias("product_id"),
        _text("seller_id").alias("seller_id"),
        _ts("shipping_limit_date").alias("shipping_limit_date"),
        _money("price").alias("price"),
        _money("freight_value").alias("freight_value"),
        *_lineage(),
    )


def clean_order_payments(df: DataFrame) -> DataFrame:
    return df.select(
        _text("order_id").alias("order_id"),
        F.col("payment_sequential").cast("int").alias("payment_sequential"),
        _lower("payment_type").alias("payment_type"),
        F.col("payment_installments").cast("int").alias("payment_installments"),
        _money("payment_value").alias("payment_value"),
        *_lineage(),
    )


def clean_order_reviews(df: DataFrame) -> DataFrame:
    return df.select(
        _text("review_id").alias("review_id"),
        _text("order_id").alias("order_id"),
        F.col("review_score").cast("int").alias("review_score"),
        _text("review_comment_title").alias("review_comment_title"),
        _text("review_comment_message").alias("review_comment_message"),
        _ts("review_creation_date").alias("review_creation_date"),
        _ts("review_answer_timestamp").alias("review_answer_timestamp"),
        *_lineage(),
    )


def clean_products(df: DataFrame) -> DataFrame:
    return df.select(
        _text("product_id").alias("product_id"),
        F.coalesce(_lower("product_category_name"), F.lit("unknown")).alias("product_category_name"),
        F.col("product_name_lenght").cast("int").alias("product_name_length"),
        F.col("product_description_lenght").cast("int").alias("product_description_length"),
        F.col("product_photos_qty").cast("int").alias("product_photos_qty"),
        F.col("product_weight_g").cast("int").alias("product_weight_g"),
        F.col("product_length_cm").cast("int").alias("product_length_cm"),
        F.col("product_height_cm").cast("int").alias("product_height_cm"),
        F.col("product_width_cm").cast("int").alias("product_width_cm"),
        *_lineage(),
    )


def clean_category_translation(df: DataFrame) -> DataFrame:
    return df.select(
        _lower("product_category_name").alias("product_category_name"),
        _lower("product_category_name_english").alias("product_category_name_english"),
        *_lineage(),
    )


# Brazil's bounding box. The raw geolocation data has some points in Europe and
# the ocean, which would ruin any distance calculation.
BRAZIL_LAT = (-35.0, 6.0)
BRAZIL_LNG = (-75.0, -33.0)


def clean_geolocation(df: DataFrame) -> DataFrame:
    """Raw geolocation has ~1M rows with many points per zip prefix.
    Silver keeps one row per zip: the average of the points inside Brazil."""
    in_brazil = F.col("geolocation_lat").between(*BRAZIL_LAT) & F.col("geolocation_lng").between(*BRAZIL_LNG)
    return (
        df.filter(in_brazil)
        .withColumn("zip_code_prefix", _zip("geolocation_zip_code_prefix"))
        .groupBy("zip_code_prefix")
        .agg(
            F.round(F.avg("geolocation_lat"), 6).alias("lat"),
            F.round(F.avg("geolocation_lng"), 6).alias("lng"),
            F.first(_lower("geolocation_city"), ignorenulls=True).alias("city"),
            F.first(_upper("geolocation_state"), ignorenulls=True).alias("state"),
            F.max(F.col("_updated_at").cast("timestamp")).alias("_updated_at"),
            F.max("_ingested_at").alias("_ingested_at"),
        )
    )


CLEANERS: dict[str, Callable[[DataFrame], DataFrame]] = {
    "customers": clean_customers,
    "geolocation": clean_geolocation,
    "orders": clean_orders,
    "order_items": clean_order_items,
    "order_payments": clean_order_payments,
    "order_reviews": clean_order_reviews,
    "products": clean_products,
    "sellers": clean_sellers,
    "category_translation": clean_category_translation,
}
