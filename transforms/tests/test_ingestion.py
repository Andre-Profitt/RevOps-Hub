"""
Unit tests for ingestion transforms.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType
from datetime import date


class TestSalesforceSync:
    """Tests for Salesforce sync transforms."""

    def test_opportunity_schema_mapping(self, spark):
        """Test Salesforce opportunity fields map correctly to RevOps schema."""
        # Simulate Salesforce raw data
        sf_data = spark.createDataFrame([
            ("001ABC", "Test Opp", "0011111", "0051111", 100000.0, "Negotiation",
             date(2024, 12, 1), 70, date(2024, 9, 1)),
        ], ["Id", "Name", "AccountId", "OwnerId", "Amount", "StageName",
            "CloseDate", "Probability", "CreatedDate"])

        # Apply mapping (simplified version of sync logic)
        mapped = sf_data.select(
            F.col("Id").alias("opportunity_id"),
            F.col("Name").alias("opportunity_name"),
            F.col("AccountId").alias("account_id"),
            F.col("OwnerId").alias("owner_id"),
            F.col("Amount").alias("amount"),
            F.col("StageName").alias("stage_name"),
            F.col("CloseDate").alias("close_date"),
            F.col("Probability").alias("probability"),
            F.col("CreatedDate").alias("created_date"),
        )

        assert mapped.count() == 1
        row = mapped.first()
        assert row["opportunity_id"] == "001ABC"
        assert row["amount"] == 100000.0
        assert row["stage_name"] == "Negotiation"

    def test_stage_normalization(self, spark):
        """Test that stage names are normalized correctly."""
        sf_data = spark.createDataFrame([
            ("1", "QUALIFICATION"),
            ("2", "Needs Analysis"),
            ("3", "Proposal/Price Quote"),
            ("4", "negotiation"),
        ], ["id", "stage"])

        # Normalize stages (simplified)
        normalized = sf_data.withColumn(
            "normalized_stage",
            F.when(F.lower(F.col("stage")).like("%qualif%"), "Discovery")
            .when(F.lower(F.col("stage")).like("%need%"), "Solution Design")
            .when(F.lower(F.col("stage")).like("%propos%"), "Proposal")
            .when(F.lower(F.col("stage")).like("%negot%"), "Negotiation")
            .otherwise("Prospecting")
        )

        results = {row["id"]: row["normalized_stage"]
                   for row in normalized.collect()}

        assert results["1"] == "Discovery"
        assert results["2"] == "Solution Design"
        assert results["3"] == "Proposal"
        assert results["4"] == "Negotiation"


class TestHubspotSync:
    """Tests for HubSpot sync transforms."""

    def test_hubspot_deal_stage_mapping(self, spark):
        """Test HubSpot deal stages map to RevOps stages."""
        stage_mapping = {
            "appointmentscheduled": "Discovery",
            "qualifiedtobuy": "Solution Design",
            "presentationscheduled": "Proposal",
            "decisionmakerboughtin": "Negotiation",
            "contractsent": "Verbal Commit",
            "closedwon": "Closed Won",
            "closedlost": "Closed Lost",
        }

        hs_data = spark.createDataFrame([
            (row[0], row[1]) for row in enumerate(stage_mapping.keys())
        ], ["id", "hs_stage"])

        mapped = hs_data
        for hs_stage, revops_stage in stage_mapping.items():
            mapped = mapped.withColumn(
                "revops_stage",
                F.when(
                    F.col("hs_stage") == hs_stage,
                    revops_stage
                ).otherwise(F.coalesce(F.col("revops_stage"), F.lit(None)))
            )

        results = {row["hs_stage"]: row["revops_stage"]
                   for row in mapped.collect()}

        for hs_stage, expected_revops in stage_mapping.items():
            assert results[hs_stage] == expected_revops

    def test_engagement_type_mapping(self, spark):
        """Test HubSpot engagement types map correctly."""
        type_mapping = {
            "CALL": "Call",
            "EMAIL": "Email",
            "MEETING": "Meeting",
            "NOTE": "Note",
            "TASK": "Task",
        }

        hs_data = spark.createDataFrame([
            (row[0], row[1]) for row in enumerate(type_mapping.keys())
        ], ["id", "type"])

        mapped = hs_data.withColumn(
            "activity_type",
            F.when(F.col("type") == "CALL", "Call")
            .when(F.col("type") == "EMAIL", "Email")
            .when(F.col("type") == "MEETING", "Meeting")
            .when(F.col("type") == "NOTE", "Note")
            .otherwise("Task")
        )

        results = {row["type"]: row["activity_type"]
                   for row in mapped.collect()}

        for hs_type, expected_type in type_mapping.items():
            assert results[hs_type] == expected_type


class TestTelemetryFeeds:
    """Tests for telemetry feed transforms."""

    def test_usage_health_scoring(self, spark):
        """Test usage health scores are calculated correctly."""
        usage_data = spark.createDataFrame([
            ("ACC-001", 10.0),  # High engagement
            ("ACC-002", 5.0),   # Medium engagement
            ("ACC-003", 2.0),   # Low engagement
            ("ACC-004", 0.5),   # Critical
        ], ["account_id", "engagement_score"])

        scored = usage_data.withColumn(
            "usage_health",
            F.when(F.col("engagement_score") >= 7, "Healthy")
            .when(F.col("engagement_score") >= 4, "Monitor")
            .when(F.col("engagement_score") >= 2, "At Risk")
            .otherwise("Critical")
        )

        results = {row["account_id"]: row["usage_health"]
                   for row in scored.collect()}

        assert results["ACC-001"] == "Healthy"
        assert results["ACC-002"] == "Monitor"
        assert results["ACC-003"] == "At Risk"
        assert results["ACC-004"] == "Critical"

    def test_support_health_scoring(self, spark):
        """Test support health scores are calculated correctly."""
        support_data = spark.createDataFrame([
            ("ACC-001", 0, 0, 0),   # Perfect - no issues
            ("ACC-002", 1, 2, 5),   # Some tickets
            ("ACC-003", 2, 5, 10),  # Many tickets
            ("ACC-004", 3, 10, 20), # Critical support issues
        ], ["account_id", "critical_tickets", "high_tickets", "open_tickets"])

        scored = support_data.withColumn(
            "support_health_score",
            F.greatest(
                F.lit(0),
                F.lit(100) -
                (F.col("critical_tickets") * 20) -
                (F.col("high_tickets") * 10) -
                (F.col("open_tickets") * 5)
            )
        ).withColumn(
            "support_health_status",
            F.when(F.col("support_health_score") >= 80, "Healthy")
            .when(F.col("support_health_score") >= 60, "Monitor")
            .when(F.col("support_health_score") >= 40, "At Risk")
            .otherwise("Critical")
        )

        results = {row["account_id"]: row["support_health_status"]
                   for row in scored.collect()}

        assert results["ACC-001"] == "Healthy"
        assert results["ACC-002"] == "Healthy"
        assert results["ACC-003"] == "Monitor"
        assert results["ACC-004"] == "Critical"


class TestActivityIngestion:
    """Tests for activity data ingestion."""

    def test_activity_deduplication(self, spark):
        """Test duplicate activities are handled correctly."""
        # Data with duplicates
        activities = spark.createDataFrame([
            ("ACT-001", "OPP-001", "Call", date(2024, 11, 1)),
            ("ACT-001", "OPP-001", "Call", date(2024, 11, 1)),  # Duplicate
            ("ACT-002", "OPP-001", "Email", date(2024, 11, 2)),
        ], ["activity_id", "opportunity_id", "activity_type", "activity_date"])

        deduped = activities.dropDuplicates(["activity_id"])

        assert deduped.count() == 2
        assert deduped.filter(F.col("activity_id") == "ACT-001").count() == 1

    def test_activity_date_validation(self, spark):
        """Test activities with future dates are flagged."""
        from datetime import date, timedelta

        future_date = date.today() + timedelta(days=30)

        activities = spark.createDataFrame([
            ("ACT-001", date.today()),
            ("ACT-002", future_date),
        ], ["activity_id", "activity_date"])

        validated = activities.withColumn(
            "is_future_date",
            F.col("activity_date") > F.current_date()
        )

        results = {row["activity_id"]: row["is_future_date"]
                   for row in validated.collect()}

        assert results["ACT-001"] == False
        assert results["ACT-002"] == True
