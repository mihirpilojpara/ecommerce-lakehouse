"""Source (Postgres) -> bronze (Delta).

Incremental extract: only rows whose _updated_at is newer than the last saved
watermark are read. Bronze is append-only and keeps every version of a row,
plus when and in which batch it arrived. Nothing is cleaned here; bronze is the
raw history you can always rebuild silver from.

Usage: python -m lakehouse.jobs.bronze --table orders
"""

import argparse
from datetime import UTC, datetime

from pyspark.sql import functions as F

from lakehouse import watermark
from lakehouse.config import JDBC_URL, SOURCE_DB, SOURCE_TABLES, bronze_path
from lakehouse.spark import get_spark


def extract_query(table: str, since: str | None) -> str:
    if since:
        return f"(SELECT * FROM public.{table} WHERE _updated_at > TIMESTAMPTZ '{since}') AS src"
    return f"(SELECT * FROM public.{table}) AS src"


def run(table: str) -> int:
    if table not in SOURCE_TABLES:
        raise ValueError(f"Unknown table {table!r}. Choose from: {', '.join(SOURCE_TABLES)}")

    spark = get_spark(f"bronze-{table}")
    wm_key = f"bronze__{table}"
    since = watermark.get(wm_key)
    print(f"[bronze] {table}: extracting rows updated after {since or 'the beginning'}")

    batch_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    df = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", extract_query(table, since))
        .option("user", SOURCE_DB["user"])
        .option("password", SOURCE_DB["password"])
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")
        .load()
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingest_date", F.current_date())
        .withColumn("_batch_id", F.lit(batch_id))
        .cache()
    )

    rows = df.count()
    if rows == 0:
        print(f"[bronze] {table}: no new rows")
        spark.stop()
        return 0

    new_watermark = df.agg(F.max("_updated_at")).first()[0]

    (
        df.write.format("delta")
        .mode("append")
        .partitionBy("_ingest_date")
        .option("mergeSchema", "true")  # new source columns are added, not rejected
        .save(bronze_path(table))
    )

    # Saved only after the write succeeded: a failed run re-reads the same rows.
    watermark.save(wm_key, f"{new_watermark.isoformat(sep=' ')}+00:00")
    print(f"[bronze] {table}: appended {rows} rows (batch {batch_id}), watermark -> {new_watermark}")
    spark.stop()
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", required=True, choices=sorted(SOURCE_TABLES))
    args = parser.parse_args()
    run(args.table)


if __name__ == "__main__":
    main()
