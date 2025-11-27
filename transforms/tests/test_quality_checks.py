"""
Unit tests for DataQualityChecker.
"""

import pytest
from pyspark.sql import functions as F
from quality.data_quality_checks import (
    DataQualityChecker,
    DataQualityError,
    assert_opportunity_quality,
    assert_health_score_quality,
)


class TestDataQualityChecker:
    """Tests for the DataQualityChecker class."""

    def test_assert_not_empty_passes(self, spark):
        """Test not_empty check passes with data."""
        df = spark.createDataFrame([("1",), ("2",)], ["id"])
        checker = DataQualityChecker(df, "test")
        checker.assert_not_empty()
        summary = checker.get_summary()
        assert summary["failed"] == 0

    def test_assert_not_empty_fails(self, spark):
        """Test not_empty check fails with empty data."""
        df = spark.createDataFrame([], "id: string")
        checker = DataQualityChecker(df, "test")
        checker.assert_not_empty()
        summary = checker.get_summary()
        assert summary["failed"] == 1

    def test_assert_no_nulls_passes(self, spark):
        """Test no_nulls check passes with valid data."""
        df = spark.createDataFrame([("1", "a"), ("2", "b")], ["id", "name"])
        checker = DataQualityChecker(df, "test")
        checker.assert_no_nulls("id", "name")
        summary = checker.get_summary()
        assert summary["failed"] == 0

    def test_assert_no_nulls_fails(self, spark):
        """Test no_nulls check fails with null values."""
        df = spark.createDataFrame([("1", "a"), ("2", None)], ["id", "name"])
        checker = DataQualityChecker(df, "test")
        checker.assert_no_nulls("name")
        summary = checker.get_summary()
        assert summary["failed"] == 1

    def test_assert_unique_passes(self, spark):
        """Test unique check passes with unique values."""
        df = spark.createDataFrame([("1",), ("2",), ("3",)], ["id"])
        checker = DataQualityChecker(df, "test")
        checker.assert_unique("id")
        summary = checker.get_summary()
        assert summary["failed"] == 0

    def test_assert_unique_fails(self, spark):
        """Test unique check fails with duplicates."""
        df = spark.createDataFrame([("1",), ("1",), ("3",)], ["id"])
        checker = DataQualityChecker(df, "test")
        checker.assert_unique("id")
        summary = checker.get_summary()
        assert summary["failed"] == 1

    def test_assert_values_in_passes(self, spark):
        """Test values_in check passes with valid values."""
        df = spark.createDataFrame([("A",), ("B",), ("C",)], ["status"])
        checker = DataQualityChecker(df, "test")
        checker.assert_values_in("status", ["A", "B", "C", "D"])
        summary = checker.get_summary()
        assert summary["failed"] == 0

    def test_assert_values_in_fails(self, spark):
        """Test values_in check fails with invalid values."""
        df = spark.createDataFrame([("A",), ("B",), ("X",)], ["status"])
        checker = DataQualityChecker(df, "test")
        checker.assert_values_in("status", ["A", "B", "C"])
        summary = checker.get_summary()
        assert summary["failed"] == 1

    def test_assert_positive_passes(self, spark):
        """Test positive check passes with positive values."""
        df = spark.createDataFrame([(1.0,), (100.0,), (0.5,)], ["amount"])
        checker = DataQualityChecker(df, "test")
        checker.assert_positive("amount")
        summary = checker.get_summary()
        assert summary["failed"] == 0

    def test_assert_positive_fails(self, spark):
        """Test positive check fails with negative values."""
        df = spark.createDataFrame([(1.0,), (-5.0,), (0.5,)], ["amount"])
        checker = DataQualityChecker(df, "test")
        checker.assert_positive("amount")
        summary = checker.get_summary()
        assert summary["failed"] == 1

    def test_assert_in_range_passes(self, spark):
        """Test in_range check passes with values in range."""
        df = spark.createDataFrame([(50.0,), (75.0,), (25.0,)], ["score"])
        checker = DataQualityChecker(df, "test")
        checker.assert_in_range("score", 0, 100)
        summary = checker.get_summary()
        assert summary["failed"] == 0

    def test_assert_in_range_fails(self, spark):
        """Test in_range check fails with values out of range."""
        df = spark.createDataFrame([(50.0,), (150.0,), (25.0,)], ["score"])
        checker = DataQualityChecker(df, "test")
        checker.assert_in_range("score", 0, 100)
        summary = checker.get_summary()
        assert summary["failed"] == 1

    def test_assert_columns_exist_passes(self, spark):
        """Test columns_exist check passes when columns present."""
        df = spark.createDataFrame([("1", "a")], ["id", "name"])
        checker = DataQualityChecker(df, "test")
        checker.assert_columns_exist("id", "name")
        summary = checker.get_summary()
        assert summary["failed"] == 0

    def test_assert_columns_exist_fails(self, spark):
        """Test columns_exist check fails when columns missing."""
        df = spark.createDataFrame([("1", "a")], ["id", "name"])
        checker = DataQualityChecker(df, "test")
        checker.assert_columns_exist("id", "name", "missing_col")
        summary = checker.get_summary()
        assert summary["failed"] == 1

    def test_finalize_raises_on_failure(self, spark):
        """Test finalize raises DataQualityError on failure."""
        df = spark.createDataFrame([], "id: string")
        checker = DataQualityChecker(df, "test")
        checker.assert_not_empty()

        with pytest.raises(DataQualityError):
            checker.finalize(raise_on_failure=True)

    def test_finalize_does_not_raise_when_disabled(self, spark):
        """Test finalize doesn't raise when raise_on_failure=False."""
        df = spark.createDataFrame([], "id: string")
        checker = DataQualityChecker(df, "test")
        checker.assert_not_empty()

        summary = checker.finalize(raise_on_failure=False)
        assert summary["failed"] == 1

    def test_chained_checks(self, spark):
        """Test multiple checks can be chained fluently."""
        df = spark.createDataFrame(
            [("1", 50.0), ("2", 75.0)],
            ["id", "score"]
        )

        summary = (
            DataQualityChecker(df, "test")
            .assert_not_empty()
            .assert_no_nulls("id")
            .assert_unique("id")
            .assert_in_range("score", 0, 100)
            .get_summary()
        )

        assert summary["total_checks"] == 4
        assert summary["passed"] == 4
        assert summary["failed"] == 0


class TestOpportunityQualityAssertions:
    """Tests for opportunity-specific quality assertions."""

    def test_assert_opportunity_quality_passes(self, sample_opportunities):
        """Test opportunity quality passes with valid data."""
        summary = assert_opportunity_quality(
            sample_opportunities, "test"
        ).finalize(raise_on_failure=False)

        assert summary["failed"] == 0

    def test_assert_opportunity_quality_catches_invalid_stage(self, spark, opportunity_schema):
        """Test opportunity quality catches invalid stage names."""
        data = [
            ("OPP-001", "Test", "ACC-001", "Test", "REP-001", "Test",
             100000.0, "Invalid Stage", "Pipeline", None, None,
             False, False, 50.0, 10, None, 2, False, False, None, 0.0, None, 50),
        ]
        df = spark.createDataFrame(data, opportunity_schema)

        summary = assert_opportunity_quality(df, "test").finalize(raise_on_failure=False)
        assert summary["failed"] > 0


class TestHealthScoreQualityAssertions:
    """Tests for health score quality assertions."""

    def test_assert_health_score_quality_passes(self, spark):
        """Test health score quality passes with valid data."""
        df = spark.createDataFrame([
            ("OPP-001", 85, "Healthy"),
            ("OPP-002", 45, "At Risk"),
        ], ["opportunity_id", "health_score", "health_category"])

        summary = assert_health_score_quality(df, "test").finalize(raise_on_failure=False)
        assert summary["failed"] == 0

    def test_assert_health_score_quality_catches_invalid_range(self, spark):
        """Test health score quality catches out-of-range scores."""
        df = spark.createDataFrame([
            ("OPP-001", 150, "Healthy"),  # Invalid score > 100
        ], ["opportunity_id", "health_score", "health_category"])

        summary = assert_health_score_quality(df, "test").finalize(raise_on_failure=False)
        assert summary["failed"] > 0
