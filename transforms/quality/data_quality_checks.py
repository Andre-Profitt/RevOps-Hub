"""
DATA QUALITY ASSERTIONS
=======================
Reusable data quality checks for Foundry transforms.

Usage in transforms:
    from quality.data_quality_checks import DataQualityChecker

    @transform(...)
    def compute(ctx, input_df, output):
        df = input_df.dataframe()
        checker = DataQualityChecker(df, "my_transform")

        # Run checks
        checker.assert_not_empty()
        checker.assert_no_nulls("account_id")
        checker.assert_unique("opportunity_id")
        checker.assert_values_in("stage_name", ["Discovery", "Proposal", ...])

        # Get summary and fail if any checks failed
        checker.finalize()  # Raises if failures

        output.write_dataframe(df)
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class CheckResult:
    """Result of a single data quality check."""
    check_name: str
    passed: bool
    message: str
    details: dict = field(default_factory=dict)


class DataQualityChecker:
    """
    Fluent interface for data quality assertions.

    Example:
        checker = DataQualityChecker(df, "deal_health_transform")
        checker.assert_not_empty()
        checker.assert_no_nulls("account_id", "opportunity_id")
        checker.assert_unique("opportunity_id")
        checker.finalize()  # Raises DataQualityError if any failures
    """

    def __init__(self, df: DataFrame, transform_name: str, fail_fast: bool = False):
        self.df = df
        self.transform_name = transform_name
        self.fail_fast = fail_fast
        self.results: List[CheckResult] = []
        self._count: Optional[int] = None

    @property
    def count(self) -> int:
        """Lazy-evaluated row count."""
        if self._count is None:
            self._count = self.df.count()
        return self._count

    def _record(self, result: CheckResult):
        """Record a check result."""
        self.results.append(result)
        if self.fail_fast and not result.passed:
            raise DataQualityError(f"[{self.transform_name}] {result.check_name}: {result.message}")

    # =========================================
    # EXISTENCE CHECKS
    # =========================================

    def assert_not_empty(self, min_rows: int = 1) -> "DataQualityChecker":
        """Assert the dataframe has at least min_rows rows."""
        passed = self.count >= min_rows
        self._record(CheckResult(
            check_name="not_empty",
            passed=passed,
            message=f"Expected >= {min_rows} rows, got {self.count}",
            details={"min_rows": min_rows, "actual_rows": self.count}
        ))
        return self

    def assert_row_count_in_range(self, min_rows: int, max_rows: int) -> "DataQualityChecker":
        """Assert row count is within expected range."""
        passed = min_rows <= self.count <= max_rows
        self._record(CheckResult(
            check_name="row_count_in_range",
            passed=passed,
            message=f"Expected {min_rows}-{max_rows} rows, got {self.count}",
            details={"min_rows": min_rows, "max_rows": max_rows, "actual_rows": self.count}
        ))
        return self

    # =========================================
    # NULL CHECKS
    # =========================================

    def assert_no_nulls(self, *columns: str) -> "DataQualityChecker":
        """Assert specified columns have no null values."""
        for col in columns:
            null_count = self.df.filter(F.col(col).isNull()).count()
            passed = null_count == 0
            self._record(CheckResult(
                check_name=f"no_nulls_{col}",
                passed=passed,
                message=f"Column '{col}' has {null_count} null values",
                details={"column": col, "null_count": null_count}
            ))
        return self

    def assert_null_rate_below(self, column: str, max_rate: float) -> "DataQualityChecker":
        """Assert null rate is below threshold (0.0 - 1.0)."""
        null_count = self.df.filter(F.col(column).isNull()).count()
        actual_rate = null_count / self.count if self.count > 0 else 0
        passed = actual_rate <= max_rate
        self._record(CheckResult(
            check_name=f"null_rate_{column}",
            passed=passed,
            message=f"Column '{column}' null rate {actual_rate:.2%} exceeds threshold {max_rate:.2%}",
            details={"column": column, "null_rate": actual_rate, "threshold": max_rate}
        ))
        return self

    # =========================================
    # UNIQUENESS CHECKS
    # =========================================

    def assert_unique(self, *columns: str) -> "DataQualityChecker":
        """Assert specified column(s) form a unique key."""
        col_list = list(columns)
        distinct_count = self.df.select(col_list).distinct().count()
        passed = distinct_count == self.count
        duplicate_count = self.count - distinct_count
        self._record(CheckResult(
            check_name=f"unique_{'+'.join(col_list)}",
            passed=passed,
            message=f"Columns {col_list} have {duplicate_count} duplicate combinations",
            details={"columns": col_list, "duplicates": duplicate_count}
        ))
        return self

    # =========================================
    # VALUE CHECKS
    # =========================================

    def assert_values_in(self, column: str, allowed_values: List[str]) -> "DataQualityChecker":
        """Assert all values in column are in the allowed set."""
        invalid = self.df.filter(~F.col(column).isin(allowed_values))
        invalid_count = invalid.count()
        passed = invalid_count == 0

        details = {"column": column, "allowed": allowed_values, "invalid_count": invalid_count}
        if not passed:
            # Get sample of invalid values
            sample = [row[column] for row in invalid.select(column).distinct().limit(5).collect()]
            details["invalid_sample"] = sample

        self._record(CheckResult(
            check_name=f"values_in_{column}",
            passed=passed,
            message=f"Column '{column}' has {invalid_count} values not in allowed set",
            details=details
        ))
        return self

    def assert_positive(self, *columns: str) -> "DataQualityChecker":
        """Assert numeric columns have only positive values."""
        for col in columns:
            negative_count = self.df.filter(F.col(col) < 0).count()
            passed = negative_count == 0
            self._record(CheckResult(
                check_name=f"positive_{col}",
                passed=passed,
                message=f"Column '{col}' has {negative_count} negative values",
                details={"column": col, "negative_count": negative_count}
            ))
        return self

    def assert_in_range(self, column: str, min_val: float, max_val: float) -> "DataQualityChecker":
        """Assert values are within a numeric range."""
        out_of_range = self.df.filter(
            (F.col(column) < min_val) | (F.col(column) > max_val)
        ).count()
        passed = out_of_range == 0
        self._record(CheckResult(
            check_name=f"in_range_{column}",
            passed=passed,
            message=f"Column '{column}' has {out_of_range} values outside [{min_val}, {max_val}]",
            details={"column": column, "min": min_val, "max": max_val, "out_of_range": out_of_range}
        ))
        return self

    # =========================================
    # REFERENTIAL INTEGRITY
    # =========================================

    def assert_foreign_key(self, column: str, reference_df: DataFrame, reference_column: str) -> "DataQualityChecker":
        """Assert all values exist in a reference dataset."""
        ref_values = set(row[reference_column] for row in reference_df.select(reference_column).distinct().collect())
        orphan_count = self.df.filter(~F.col(column).isin(ref_values)).count()
        passed = orphan_count == 0
        self._record(CheckResult(
            check_name=f"fk_{column}",
            passed=passed,
            message=f"Column '{column}' has {orphan_count} orphan values not in reference",
            details={"column": column, "orphan_count": orphan_count}
        ))
        return self

    # =========================================
    # FRESHNESS CHECKS
    # =========================================

    def assert_recent_data(self, date_column: str, max_age_hours: int = 24) -> "DataQualityChecker":
        """Assert the most recent record is within max_age_hours."""
        max_date = self.df.agg(F.max(date_column)).collect()[0][0]
        if max_date is None:
            passed = False
            age_hours = float('inf')
        else:
            age_hours = (datetime.now() - max_date).total_seconds() / 3600
            passed = age_hours <= max_age_hours

        self._record(CheckResult(
            check_name=f"freshness_{date_column}",
            passed=passed,
            message=f"Most recent data is {age_hours:.1f} hours old (max: {max_age_hours}h)",
            details={"column": date_column, "age_hours": age_hours, "max_age_hours": max_age_hours}
        ))
        return self

    # =========================================
    # SCHEMA CHECKS
    # =========================================

    def assert_columns_exist(self, *columns: str) -> "DataQualityChecker":
        """Assert specified columns exist in the dataframe."""
        existing = set(self.df.columns)
        missing = [c for c in columns if c not in existing]
        passed = len(missing) == 0
        self._record(CheckResult(
            check_name="columns_exist",
            passed=passed,
            message=f"Missing columns: {missing}" if missing else "All required columns present",
            details={"required": list(columns), "missing": missing}
        ))
        return self

    # =========================================
    # FINALIZATION
    # =========================================

    def get_summary(self) -> dict:
        """Get a summary of all check results."""
        passed = [r for r in self.results if r.passed]
        failed = [r for r in self.results if not r.passed]

        return {
            "transform": self.transform_name,
            "timestamp": datetime.now().isoformat(),
            "row_count": self.count,
            "total_checks": len(self.results),
            "passed": len(passed),
            "failed": len(failed),
            "failures": [{"check": r.check_name, "message": r.message} for r in failed]
        }

    def finalize(self, raise_on_failure: bool = True) -> dict:
        """
        Finalize checks and optionally raise on failures.

        Returns summary dict. If raise_on_failure=True and there are failures,
        raises DataQualityError.
        """
        summary = self.get_summary()

        if raise_on_failure and summary["failed"] > 0:
            failure_msgs = "\n".join(f"  - {f['check']}: {f['message']}" for f in summary["failures"])
            raise DataQualityError(
                f"[{self.transform_name}] {summary['failed']} data quality check(s) failed:\n{failure_msgs}"
            )

        return summary


class DataQualityError(Exception):
    """Raised when data quality checks fail."""
    pass


# =========================================
# COMMON ASSERTION SETS
# =========================================

def assert_opportunity_quality(df: DataFrame, transform_name: str) -> DataQualityChecker:
    """Standard quality checks for opportunity datasets."""
    return (
        DataQualityChecker(df, transform_name)
        .assert_not_empty()
        .assert_no_nulls("opportunity_id", "account_id")
        .assert_unique("opportunity_id")
        .assert_positive("amount")
        .assert_values_in("stage_name", [
            "Prospecting", "Discovery", "Solution Design",
            "Proposal", "Negotiation", "Verbal Commit", "Closed Won", "Closed Lost"
        ])
    )


def assert_health_score_quality(df: DataFrame, transform_name: str) -> DataQualityChecker:
    """Standard quality checks for health score datasets."""
    return (
        DataQualityChecker(df, transform_name)
        .assert_not_empty()
        .assert_no_nulls("opportunity_id", "health_score")
        .assert_in_range("health_score", 0, 100)
        .assert_values_in("health_category", ["Healthy", "Monitor", "At Risk", "Critical"])
    )


def assert_forecast_quality(df: DataFrame, transform_name: str) -> DataQualityChecker:
    """Standard quality checks for forecast datasets."""
    return (
        DataQualityChecker(df, transform_name)
        .assert_not_empty()
        .assert_positive("forecast_amount", "closed_amount")
    )
