"""Silver -> gold star schema (Delta).

Gold is rebuilt from silver on every run. With ~100K orders this takes seconds
and keeps the logic simple; at larger scale the fact table would be MERGEd by
date partition instead.

Usage: python -m lakehouse.jobs.gold
"""

from pathlib import Path

from delta.tables import DeltaTable

from lakehouse.config import WAREHOUSE_ROOT, gold_path, silver_path
from lakehouse.delta_utils import overwrite
from lakehouse.spark import get_spark
from lakehouse.transforms import gold


def run() -> None:
    spark = get_spark("gold")

    def silver(name: str):
        return spark.read.format("delta").load(silver_path(name))

    orders = silver("orders")
    geo = silver("geolocation")

    start, end = gold.date_bounds(orders)
    tables = {
        "dim_date": gold.build_dim_date(spark, start, end),
        "dim_customer": gold.build_dim_customer(silver("customers"), geo),
        "dim_seller": gold.build_dim_seller(silver("sellers"), geo),
        "dim_product": gold.build_dim_product(silver("products"), silver("category_translation")),
        "fact_order_items": gold.build_fact_order_items(
            orders, silver("order_items"), silver("order_payments"), silver("order_reviews")
        ),
    }

    for name, df in tables.items():
        overwrite(df, gold_path(name))
        print(f"[gold] {name}: {spark.read.format('delta').load(gold_path(name)).count()} rows")

    # Compact the fact table into fewer, larger files ordered by date, so
    # queries that filter on a date range read less data.
    DeltaTable.forPath(spark, gold_path("fact_order_items")).optimize().executeZOrderBy("purchase_date_key")

    Path(WAREHOUSE_ROOT).mkdir(parents=True, exist_ok=True)  # dbt's DuckDB file lives here
    spark.stop()


if __name__ == "__main__":
    run()
