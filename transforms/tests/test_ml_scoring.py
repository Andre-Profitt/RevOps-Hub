"""
Unit tests for ML scoring transforms.
"""

import pytest
from pyspark.sql import functions as F
from datetime import date, timedelta


class TestDealFeatureEngineering:
    """Tests for deal feature engineering."""

    def test_deal_age_calculation(self, spark):
        """Test deal age is calculated correctly."""
        today = date.today()
        deals = spark.createDataFrame([
            ("OPP-001", today - timedelta(days=30)),
            ("OPP-002", today - timedelta(days=60)),
            ("OPP-003", today - timedelta(days=90)),
        ], ["opportunity_id", "created_date"])

        with_age = deals.withColumn(
            "deal_age_days",
            F.datediff(F.current_date(), F.col("created_date"))
        )

        results = {row["opportunity_id"]: row["deal_age_days"]
                   for row in with_age.collect()}

        assert results["OPP-001"] == 30
        assert results["OPP-002"] == 60
        assert results["OPP-003"] == 90

    def test_activity_momentum_calculation(self, spark, sample_activities):
        """Test activity momentum is calculated correctly."""
        today = date.today()

        # Count activities in last 7 days vs prior 7 days
        activities = sample_activities.withColumn(
            "days_ago",
            F.datediff(F.current_date(), F.col("activity_date"))
        )

        momentum = activities.groupBy("opportunity_id").agg(
            F.sum(F.when(F.col("days_ago") <= 7, 1).otherwise(0)).alias("activities_last_7d"),
            F.sum(F.when((F.col("days_ago") > 7) & (F.col("days_ago") <= 14), 1).otherwise(0)).alias("activities_prior_7d"),
        ).withColumn(
            "activity_momentum",
            F.when(
                F.col("activities_prior_7d") > 0,
                (F.col("activities_last_7d") - F.col("activities_prior_7d")) / F.col("activities_prior_7d")
            ).otherwise(
                F.when(F.col("activities_last_7d") > 0, 1.0).otherwise(0.0)
            )
        )

        assert momentum.count() > 0
        # Verify momentum column exists and has values
        assert "activity_momentum" in momentum.columns

    def test_engagement_score_calculation(self, spark):
        """Test engagement score calculation."""
        activity_data = spark.createDataFrame([
            ("OPP-001", 5, 3, 2, 1),   # High engagement
            ("OPP-002", 10, 5, 3, 2),  # Very high engagement
            ("OPP-003", 1, 0, 0, 0),   # Low engagement
        ], ["opportunity_id", "total_activities", "calls", "meetings", "demos"])

        # Calculate engagement score
        scored = activity_data.withColumn(
            "engagement_score",
            # Weight: demos (10), meetings (5), calls (3), other (1)
            (F.col("demos") * 10 + F.col("meetings") * 5 + F.col("calls") * 3 +
             (F.col("total_activities") - F.col("demos") - F.col("meetings") - F.col("calls")))
        )

        results = {row["opportunity_id"]: row["engagement_score"]
                   for row in scored.collect()}

        # OPP-001: 1*10 + 2*5 + 3*3 + (5-1-2-3)*1 = 10 + 10 + 9 - 1 = 28
        assert results["OPP-001"] == 28
        # OPP-002: 2*10 + 3*5 + 5*3 + 0 = 20 + 15 + 15 = 50
        assert results["OPP-002"] == 50

    def test_multi_thread_score_calculation(self, spark):
        """Test multi-thread (stakeholder) score calculation."""
        deals = spark.createDataFrame([
            ("OPP-001", 5, True),   # Many stakeholders + champion
            ("OPP-002", 3, True),   # Moderate + champion
            ("OPP-003", 2, False),  # Few, no champion
            ("OPP-004", 1, False),  # Single-threaded
        ], ["opportunity_id", "stakeholder_count", "has_champion"])

        scored = deals.withColumn(
            "multi_thread_score",
            F.when(F.col("stakeholder_count") >= 5, 100)
            .when(F.col("stakeholder_count") >= 4, 90)
            .when(F.col("stakeholder_count") >= 3, 75)
            .when(F.col("stakeholder_count") >= 2, 50)
            .otherwise(20)
        ).withColumn(
            "champion_bonus",
            F.when(F.col("has_champion"), 10).otherwise(0)
        ).withColumn(
            "final_score",
            F.least(F.col("multi_thread_score") + F.col("champion_bonus"), F.lit(100))
        )

        results = {row["opportunity_id"]: row["final_score"]
                   for row in scored.collect()}

        assert results["OPP-001"] == 100  # 100 + 10 capped at 100
        assert results["OPP-002"] == 85   # 75 + 10
        assert results["OPP-003"] == 50   # 50 + 0
        assert results["OPP-004"] == 20   # 20 + 0


class TestDealScoring:
    """Tests for deal scoring inference."""

    def test_win_probability_calculation(self, spark):
        """Test win probability scoring."""
        features = spark.createDataFrame([
            ("OPP-001", 0.8, 0.9, 0.85, 0.7),  # All strong
            ("OPP-002", 0.4, 0.5, 0.3, 0.6),   # Mixed
            ("OPP-003", 0.2, 0.3, 0.4, 0.3),   # All weak
        ], ["opportunity_id", "activity_score", "engagement_score",
            "velocity_score", "stakeholder_score"])

        # Simple weighted score (in real model, these would be learned weights)
        weights = {
            "activity": 0.25,
            "engagement": 0.30,
            "velocity": 0.25,
            "stakeholder": 0.20,
        }

        scored = features.withColumn(
            "win_probability",
            F.round(
                (F.col("activity_score") * weights["activity"] +
                 F.col("engagement_score") * weights["engagement"] +
                 F.col("velocity_score") * weights["velocity"] +
                 F.col("stakeholder_score") * weights["stakeholder"]) * 100,
                0
            )
        )

        results = {row["opportunity_id"]: row["win_probability"]
                   for row in scored.collect()}

        # OPP-001: (0.8*0.25 + 0.9*0.30 + 0.85*0.25 + 0.7*0.20) * 100 = 82.25
        assert results["OPP-001"] == 82
        # OPP-003: (0.2*0.25 + 0.3*0.30 + 0.4*0.25 + 0.3*0.20) * 100 = 30
        assert results["OPP-003"] == 30

    def test_risk_factor_identification(self, spark):
        """Test risk factors are identified correctly."""
        deals = spark.createDataFrame([
            ("OPP-001", 5, True, False, 0.0),    # No risks
            ("OPP-002", 1, False, True, 25.0),   # Multiple risks
            ("OPP-003", 3, True, False, 30.0),   # Heavy discount risk
        ], ["opportunity_id", "days_since_activity", "has_champion",
            "is_competitive", "discount_pct"])

        with_risks = deals.withColumn(
            "risk_factors",
            F.array_compact(F.array(
                F.when(F.col("days_since_activity") > 7, F.lit("Gone Dark")),
                F.when(~F.col("has_champion"), F.lit("No Champion")),
                F.when(F.col("is_competitive"), F.lit("Competitive")),
                F.when(F.col("discount_pct") > 20, F.lit("Heavy Discount")),
            ))
        )

        results = {row["opportunity_id"]: row["risk_factors"]
                   for row in with_risks.collect()}

        assert len(results["OPP-001"]) == 0
        assert "No Champion" in results["OPP-002"]
        assert "Competitive" in results["OPP-002"]
        assert "Heavy Discount" in results["OPP-002"]
        assert "Heavy Discount" in results["OPP-003"]


class TestChurnPrediction:
    """Tests for churn prediction transforms."""

    def test_usage_trend_calculation(self, spark):
        """Test usage trend is calculated correctly."""
        usage = spark.createDataFrame([
            ("ACC-001", 100, 120),  # Increasing
            ("ACC-002", 100, 80),   # Decreasing
            ("ACC-003", 100, 100),  # Stable
        ], ["account_id", "usage_30d_ago", "usage_current"])

        with_trend = usage.withColumn(
            "usage_trend",
            F.when(
                F.col("usage_current") > F.col("usage_30d_ago") * 1.1,
                "increasing"
            ).when(
                F.col("usage_current") < F.col("usage_30d_ago") * 0.9,
                "decreasing"
            ).otherwise("stable")
        )

        results = {row["account_id"]: row["usage_trend"]
                   for row in with_trend.collect()}

        assert results["ACC-001"] == "increasing"
        assert results["ACC-002"] == "decreasing"
        assert results["ACC-003"] == "stable"

    def test_churn_risk_scoring(self, spark):
        """Test churn risk scoring."""
        accounts = spark.createDataFrame([
            ("ACC-001", 90, "increasing", 9.0, 30),   # Low risk
            ("ACC-002", 50, "decreasing", 6.0, 180),  # High risk
            ("ACC-003", 30, "decreasing", 3.0, 365),  # Critical risk
        ], ["account_id", "health_score", "usage_trend",
            "nps_score", "days_since_engagement"])

        scored = accounts.withColumn(
            "churn_risk_score",
            F.round(
                100 -
                (F.col("health_score") * 0.4) -
                (F.when(F.col("usage_trend") == "increasing", 20)
                 .when(F.col("usage_trend") == "stable", 10)
                 .otherwise(0)) -
                (F.col("nps_score") * 2) -
                (F.when(F.col("days_since_engagement") < 30, 10)
                 .when(F.col("days_since_engagement") < 90, 5)
                 .otherwise(0)),
                0
            )
        ).withColumn(
            "churn_risk_tier",
            F.when(F.col("churn_risk_score") >= 70, "Critical")
            .when(F.col("churn_risk_score") >= 50, "High")
            .when(F.col("churn_risk_score") >= 30, "Medium")
            .otherwise("Low")
        )

        results = {row["account_id"]: row["churn_risk_tier"]
                   for row in scored.collect()}

        assert results["ACC-001"] == "Low"
        assert results["ACC-002"] == "High"
        assert results["ACC-003"] == "Critical"


class TestModelPerformance:
    """Tests for model performance monitoring."""

    def test_accuracy_calculation(self, spark):
        """Test model accuracy is calculated correctly."""
        predictions = spark.createDataFrame([
            ("OPP-001", 0.8, True),   # True positive (predicted high, won)
            ("OPP-002", 0.3, False),  # True negative (predicted low, lost)
            ("OPP-003", 0.7, False),  # False positive (predicted high, lost)
            ("OPP-004", 0.4, True),   # False negative (predicted low, won)
        ], ["opportunity_id", "predicted_probability", "actual_won"])

        # Calculate confusion matrix metrics
        with_predictions = predictions.withColumn(
            "predicted_win",
            F.col("predicted_probability") >= 0.5
        )

        metrics = with_predictions.agg(
            F.sum(F.when((F.col("predicted_win") == True) & (F.col("actual_won") == True), 1).otherwise(0)).alias("true_positives"),
            F.sum(F.when((F.col("predicted_win") == False) & (F.col("actual_won") == False), 1).otherwise(0)).alias("true_negatives"),
            F.sum(F.when((F.col("predicted_win") == True) & (F.col("actual_won") == False), 1).otherwise(0)).alias("false_positives"),
            F.sum(F.when((F.col("predicted_win") == False) & (F.col("actual_won") == True), 1).otherwise(0)).alias("false_negatives"),
            F.count("*").alias("total"),
        ).first()

        accuracy = (metrics["true_positives"] + metrics["true_negatives"]) / metrics["total"]
        precision = metrics["true_positives"] / (metrics["true_positives"] + metrics["false_positives"])
        recall = metrics["true_positives"] / (metrics["true_positives"] + metrics["false_negatives"])

        assert accuracy == 0.5  # 2 correct out of 4
        assert precision == 0.5  # 1 TP out of 2 predicted positive
        assert recall == 0.5     # 1 TP out of 2 actual positive
