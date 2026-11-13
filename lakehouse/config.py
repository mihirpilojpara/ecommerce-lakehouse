"""Paths, source connection settings and the list of source tables.

Everything is read from environment variables so the same code runs inside
Docker, in CI and on a laptop.
"""

import os
from dataclasses import dataclass

LAKEHOUSE_ROOT = os.getenv("LAKEHOUSE_ROOT", "/opt/lakehouse")
BRONZE_ROOT = f"{LAKEHOUSE_ROOT}/bronze"
SILVER_ROOT = f"{LAKEHOUSE_ROOT}/silver"
GOLD_ROOT = f"{LAKEHOUSE_ROOT}/gold"
META_ROOT = f"{LAKEHOUSE_ROOT}/_meta"
WAREHOUSE_ROOT = f"{LAKEHOUSE_ROOT}/warehouse"

SOURCE_DB = {
    "host": os.getenv("SOURCE_DB_HOST", "localhost"),
    "port": os.getenv("SOURCE_DB_PORT", "5433"),
    "name": os.getenv("SOURCE_DB_NAME", "olist"),
    "user": os.getenv("SOURCE_DB_USER", "postgres"),
    "password": os.getenv("SOURCE_DB_PASSWORD", "postgres"),
}
JDBC_URL = f"jdbc:postgresql://{SOURCE_DB['host']}:{SOURCE_DB['port']}/{SOURCE_DB['name']}"


@dataclass(frozen=True)
class SourceTable:
    """A table in the source database.

    keys: the columns that identify one row. Silver uses them to de-duplicate
    and to MERGE. An empty tuple means the table has no natural key and is
    rebuilt in full (geolocation is aggregated to one row per zip code).
    """

    name: str
    keys: tuple[str, ...]


SOURCE_TABLES: dict[str, SourceTable] = {
    t.name: t
    for t in [
        SourceTable("customers", ("customer_id",)),
        SourceTable("geolocation", ()),
        SourceTable("orders", ("order_id",)),
        SourceTable("order_items", ("order_id", "order_item_id")),
        SourceTable("order_payments", ("order_id", "payment_sequential")),
        SourceTable("order_reviews", ("review_id", "order_id")),
        SourceTable("products", ("product_id",)),
        SourceTable("sellers", ("seller_id",)),
        SourceTable("category_translation", ("product_category_name",)),
    ]
}

GOLD_TABLES = ["dim_date", "dim_customer", "dim_product", "dim_seller", "fact_order_items"]


def bronze_path(table: str) -> str:
    return f"{BRONZE_ROOT}/{table}"


def silver_path(table: str) -> str:
    return f"{SILVER_ROOT}/{table}"


def gold_path(table: str) -> str:
    return f"{GOLD_ROOT}/{table}"
