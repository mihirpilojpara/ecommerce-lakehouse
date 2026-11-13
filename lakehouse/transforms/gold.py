"""Silver -> gold: a star schema for analytics.

    dim_date      one row per calendar day
    dim_customer  one row per customer_id, with location
    dim_product   one row per product, with English category name
    dim_seller    one row per seller, with location
    fact_order_items  one row per item sold (grain: order_id + order_item_id)

Surrogate keys (*_sk) are a 64-bit hash of the natural key. They are stable
across runs, so facts and dimensions can be rebuilt independently and still join.
"""

from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def sk(*cols: str) -> Column:
    return F.xxhash64(*[F.col(c) for c in cols])


def date_key(c: str) -> Column:
    return F.date_format(F.col(c), "yyyyMMdd").cast("int")


def build_dim_date(spark: SparkSession, start: date, end: date) -> DataFrame:
    days = spark.range(1).select(F.explode(F.sequence(F.lit(start), F.lit(end))).alias("date"))
    return days.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("date"),
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.dayofmonth("date").alias("day_of_month"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )


def date_bounds(orders: DataFrame) -> tuple[date, date]:
    row = orders.agg(
        F.min(F.to_date("order_purchase_timestamp")).alias("start"),
        F.greatest(
            F.max(F.to_date("order_estimated_delivery_date")),
            F.max(F.to_date("order_delivered_customer_date")),
        ).alias("end"),
    ).first()
    return row["start"], row["end"]


def build_dim_customer(customers: DataFrame, geo: DataFrame) -> DataFrame:
    g = geo.select(
        F.col("zip_code_prefix").alias("customer_zip_code_prefix"),
        F.col("lat").alias("customer_lat"),
        F.col("lng").alias("customer_lng"),
    )
    return customers.join(g, "customer_zip_code_prefix", "left").select(
        sk("customer_id").alias("customer_sk"),
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        "customer_lat",
        "customer_lng",
    )


def build_dim_seller(sellers: DataFrame, geo: DataFrame) -> DataFrame:
    g = geo.select(
        F.col("zip_code_prefix").alias("seller_zip_code_prefix"),
        F.col("lat").alias("seller_lat"),
        F.col("lng").alias("seller_lng"),
    )
    return sellers.join(g, "seller_zip_code_prefix", "left").select(
        sk("seller_id").alias("seller_sk"),
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
        "seller_lat",
        "seller_lng",
    )


def build_dim_product(products: DataFrame, translation: DataFrame) -> DataFrame:
    t = translation.select("product_category_name", "product_category_name_english")
    return products.join(t, "product_category_name", "left").select(
        sk("product_id").alias("product_sk"),
        "product_id",
        F.col("product_category_name").alias("category_name"),
        F.coalesce("product_category_name_english", "product_category_name").alias("category_name_english"),
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        (F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm")).alias(
            "product_volume_cm3"
        ),
        "product_photos_qty",
    )


def build_fact_order_items(
    orders: DataFrame, items: DataFrame, payments: DataFrame, reviews: DataFrame
) -> DataFrame:
    """One row per item sold.

    An order's payment is recorded once per order, not per item. To analyse
    revenue by product or seller we spread the order's payment across its items
    in proportion to each item's price + freight.
    """
    order_payments = payments.groupBy("order_id").agg(
        F.sum("payment_value").cast("double").alias("order_payment_value"),
        F.max("payment_installments").alias("payment_installments"),
    )
    order_reviews = reviews.groupBy("order_id").agg(F.round(F.avg("review_score"), 2).alias("review_score"))

    order_window = Window.partitionBy("order_id")
    item_total = (F.col("price") + F.col("freight_value")).cast("double")

    purchased = F.col("order_purchase_timestamp")
    delivered = F.col("order_delivered_customer_date")
    estimated = F.col("order_estimated_delivery_date")

    return (
        items.join(orders.drop("_updated_at", "_ingested_at"), "order_id", "inner")
        .join(order_payments, "order_id", "left")
        .join(order_reviews, "order_id", "left")
        .withColumn("item_total", item_total)
        .withColumn("order_items_total", F.sum("item_total").over(order_window))
        .select(
            sk("order_id", "order_item_id").alias("order_item_sk"),
            "order_id",
            "order_item_id",
            sk("customer_id").alias("customer_sk"),
            sk("product_id").alias("product_sk"),
            sk("seller_id").alias("seller_sk"),
            date_key("order_purchase_timestamp").alias("purchase_date_key"),
            "order_status",
            "price",
            "freight_value",
            F.round("item_total", 2).alias("item_total"),
            F.round(
                F.when(
                    F.col("order_items_total") > 0,
                    F.col("order_payment_value") * F.col("item_total") / F.col("order_items_total"),
                ),
                2,
            ).alias("allocated_payment_value"),
            "payment_installments",
            "review_score",
            F.datediff(delivered, purchased).alias("delivery_days"),
            F.datediff(estimated, purchased).alias("estimated_delivery_days"),
            F.when(delivered.isNull(), F.lit(None).cast("boolean"))
            .otherwise(F.to_date(delivered) > F.to_date(estimated))
            .alias("is_late"),
            purchased.alias("order_purchase_timestamp"),
        )
    )
