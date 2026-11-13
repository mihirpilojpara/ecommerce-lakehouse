"""Lightweight data-quality checks run on silver data before it is written.

Each check returns a CheckResult. "error" checks stop the pipeline; "warn"
checks are only logged. All results are appended to a Delta table
(_meta/dq_results) so you can see data quality over time.
"""

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import META_ROOT


class DataQualityError(Exception):
    pass


@dataclass
class CheckResult:
    table: str
    check: str
    failed_rows: int
    severity: str = "error"  # "error" or "warn"

    @property
    def passed(self) -> bool:
        return self.failed_rows == 0


def not_null(df: DataFrame, table: str, columns: Iterable[str]) -> list[CheckResult]:
    return [CheckResult(table, f"not_null({c})", df.filter(F.col(c).isNull()).count()) for c in columns]


def unique(df: DataFrame, table: str, keys: Iterable[str]) -> CheckResult:
    keys = list(keys)
    duplicates = df.groupBy(*keys).count().filter(F.col("count") > 1).count()
    return CheckResult(table, f"unique({', '.join(keys)})", duplicates)


def in_range(
    df: DataFrame, table: str, column: str, low=None, high=None, severity: str = "error"
) -> CheckResult:
    condition = F.lit(False)
    if low is not None:
        condition = condition | (F.col(column) < low)
    if high is not None:
        condition = condition | (F.col(column) > high)
    failed = df.filter(F.col(column).isNotNull() & condition).count()
    return CheckResult(table, f"in_range({column}, {low}, {high})", failed, severity)


def accepted_values(
    df: DataFrame, table: str, column: str, values: Iterable[str], severity: str = "error"
) -> CheckResult:
    values = list(values)
    failed = df.filter(F.col(column).isNotNull() & ~F.col(column).isin(values)).count()
    return CheckResult(table, f"accepted_values({column})", failed, severity)


ORDER_STATUSES = [
    "created",
    "approved",
    "invoiced",
    "processing",
    "shipped",
    "delivered",
    "canceled",
    "unavailable",
]


def checks_for(table: str, df: DataFrame, keys: tuple[str, ...]) -> list[CheckResult]:
    """The rules for each silver table."""
    results: list[CheckResult] = []
    if keys:
        results += not_null(df, table, keys)
        results.append(unique(df, table, keys))

    if table == "orders":
        results += not_null(df, table, ["customer_id", "order_purchase_timestamp"])
        results.append(accepted_values(df, table, "order_status", ORDER_STATUSES))
    elif table == "order_items":
        results.append(in_range(df, table, "price", low=0))
        results.append(in_range(df, table, "freight_value", low=0))
    elif table == "order_payments":
        results.append(in_range(df, table, "payment_value", low=0))
    elif table == "order_reviews":
        results.append(in_range(df, table, "review_score", low=1, high=5))
    elif table == "products":
        results.append(in_range(df, table, "product_weight_g", low=0, severity="warn"))
    return results


def enforce(spark: SparkSession, results: list[CheckResult]) -> None:
    """Log every result, save them, and fail if any error-level check failed."""
    for r in results:
        status = "PASS" if r.passed else ("FAIL" if r.severity == "error" else "WARN")
        print(f"[DQ] {status:4} {r.table}.{r.check} failed_rows={r.failed_rows}")

    if results:
        checked_at = datetime.now(UTC).replace(tzinfo=None)
        rows = [(r.table, r.check, r.severity, r.failed_rows, r.passed, checked_at) for r in results]
        (
            spark.createDataFrame(
                rows,
                "table_name string, check_name string, severity string, "
                "failed_rows long, passed boolean, checked_at timestamp",
            )
            .write.format("delta")
            .mode("append")
            .save(f"{META_ROOT}/dq_results")
        )

    errors = [r for r in results if not r.passed and r.severity == "error"]
    if errors:
        names = ", ".join(f"{r.table}.{r.check}" for r in errors)
        raise DataQualityError(f"{len(errors)} data quality check(s) failed: {names}")
