"""Creates a SparkSession with Delta Lake and the Postgres JDBC driver."""

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

POSTGRES_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.4"


def get_spark(app_name: str = "olist-lakehouse") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        # The data is small (~100K orders); the default of 200 shuffle partitions
        # would create hundreds of tiny tasks and tiny files.
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = configure_spark_with_delta_pip(builder, extra_packages=[POSTGRES_JDBC_PACKAGE]).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


if __name__ == "__main__":
    # `python -m lakehouse.spark` downloads the Delta/JDBC jars once, so the
    # parallel bronze tasks don't all race to download them.
    session = get_spark("warmup")
    print(f"Spark {session.version} ready")
    session.stop()
