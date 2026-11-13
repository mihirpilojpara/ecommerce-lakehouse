"""Bronze -> silver (Delta).

Reads only bronze rows that arrived since the last silver run, cleans them,
keeps the latest version of each key, checks data quality, then MERGEs them
into silver. Silver always holds exactly one current row per key.

Geolocation has no natural key; it is aggregated to one row per zip code and
rebuilt in full each run.

Usage: python -m lakehouse.jobs.silver --table orders
"""

import argparse

from pyspark.sql import functions as F

from lakehouse import quality, watermark
from lakehouse.config import SOURCE_TABLES, bronze_path, silver_path
from lakehouse.delta_utils import overwrite, upsert
from lakehouse.spark import get_spark
from lakehouse.transforms.silver import CLEANERS, dedupe_latest


def run(table: str) -> int:
    source = SOURCE_TABLES[table]
    spark = get_spark(f"silver-{table}")
    wm_key = f"silver__{table}"
    since = watermark.get(wm_key)

    bronze = spark.read.format("delta").load(bronze_path(table))
    if since and source.keys:
        bronze = bronze.filter(F.col("_ingested_at") > F.lit(since).cast("timestamp"))
    bronze = bronze.cache()

    new_watermark = bronze.agg(F.max("_ingested_at")).first()[0]
    if new_watermark is None:
        print(f"[silver] {table}: nothing new in bronze")
        spark.stop()
        return 0

    cleaned = CLEANERS[table](bronze)
    if source.keys:
        cleaned = dedupe_latest(cleaned, source.keys)
        key_cols = source.keys
    else:
        key_cols = ("zip_code_prefix",)  # geolocation after aggregation
    cleaned = cleaned.cache()

    quality.enforce(spark, quality.checks_for(table, cleaned, key_cols))

    rows = cleaned.count()
    if source.keys:
        upsert(spark, cleaned, silver_path(table), source.keys)
    else:
        overwrite(cleaned, silver_path(table))

    watermark.save(wm_key, new_watermark.isoformat(sep=" "))
    print(f"[silver] {table}: merged {rows} rows")
    spark.stop()
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", required=True, choices=sorted(SOURCE_TABLES))
    args = parser.parse_args()
    run(args.table)


if __name__ == "__main__":
    main()
