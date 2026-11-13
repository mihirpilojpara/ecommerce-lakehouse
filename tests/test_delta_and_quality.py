import pytest

from lakehouse import quality
from lakehouse.delta_utils import upsert


def test_upsert_updates_existing_and_inserts_new_rows(spark, tmp_path):
    path = str(tmp_path / "orders")
    schema = "order_id string, order_status string"

    upsert(spark, spark.createDataFrame([("o1", "shipped"), ("o2", "created")], schema), path, ["order_id"])
    upsert(spark, spark.createDataFrame([("o1", "delivered"), ("o3", "created")], schema), path, ["order_id"])

    rows = {r.order_id: r.order_status for r in spark.read.format("delta").load(path).collect()}
    assert rows == {"o1": "delivered", "o2": "created", "o3": "created"}


def test_upsert_is_idempotent(spark, tmp_path):
    path = str(tmp_path / "orders")
    df = spark.createDataFrame([("o1", "shipped")], "order_id string, order_status string")
    upsert(spark, df, path, ["order_id"])
    upsert(spark, df, path, ["order_id"])
    assert spark.read.format("delta").load(path).count() == 1


def test_quality_checks_catch_bad_rows(spark):
    df = spark.createDataFrame(
        [("r1", "o1", 5), ("r2", "o2", 9), ("r2", "o2", 1), (None, "o3", 3)],
        "review_id string, order_id string, review_score int",
    )
    checks = quality.checks_for("order_reviews", df, ("review_id", "order_id"))
    results = {r.check: r.failed_rows for r in checks}
    assert results["not_null(review_id)"] == 1
    assert results["unique(review_id, order_id)"] == 1
    assert results["in_range(review_score, 1, 5)"] == 1


def test_enforce_raises_on_error_but_not_on_warning(spark):
    warn = quality.CheckResult("products", "in_range(product_weight_g, 0, None)", 3, "warn")
    quality.enforce(spark, [warn])  # does not raise

    error = quality.CheckResult("orders", "not_null(order_id)", 1, "error")
    with pytest.raises(quality.DataQualityError):
        quality.enforce(spark, [error])
