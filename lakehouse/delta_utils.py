"""Small helpers around Delta Lake writes."""

from collections.abc import Sequence

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def upsert(spark: SparkSession, df: DataFrame, path: str, keys: Sequence[str]) -> None:
    """MERGE df into the Delta table at path: update rows whose keys match, insert the rest.

    Running it twice with the same input gives the same result (idempotent),
    which is what makes retries safe.
    """
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").mode("overwrite").save(path)
        return

    condition = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def overwrite(df: DataFrame, path: str) -> None:
    """Replace the table's contents in one atomic Delta commit."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
