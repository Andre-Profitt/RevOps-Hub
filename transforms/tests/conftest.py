"""
Pytest fixtures for transform testing.

Provides:
- Spark session fixture
- Sample data fixtures for common schemas
- Mock Foundry context
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, DateType, TimestampType, BooleanType
)
from datetime import date, datetime, timedelta
from unittest.mock import MagicMock


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("RevOps Transform Tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .getOrCreate()
    )


@pytest.fixture
def mock_ctx():
    """Create a mock Foundry transform context."""
    ctx = MagicMock()
    ctx.spark = None  # Will be set by individual tests
    return ctx


# =============================================================================
# OPPORTUNITY FIXTURES
# =============================================================================

@pytest.fixture
def opportunity_schema():
    """Schema for opportunity data."""
    return StructType([
        StructField("opportunity_id", StringType(), False),
        StructField("opportunity_name", StringType(), True),
        StructField("account_id", StringType(), False),
        StructField("account_name", StringType(), True),
        StructField("owner_id", StringType(), True),
        StructField("owner_name", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("stage_name", StringType(), True),
        StructField("forecast_category", StringType(), True),
        StructField("close_date", DateType(), True),
        StructField("created_date", DateType(), True),
        StructField("is_closed", BooleanType(), True),
        StructField("is_won", BooleanType(), True),
        StructField("win_probability", DoubleType(), True),
        StructField("days_in_current_stage", IntegerType(), True),
        StructField("days_to_close", IntegerType(), True),
        StructField("stakeholder_count", IntegerType(), True),
        StructField("has_champion", BooleanType(), True),
        StructField("is_competitive", BooleanType(), True),
        StructField("competitor_mentioned", StringType(), True),
        StructField("discount_percent", DoubleType(), True),
        StructField("last_activity_date", DateType(), True),
        StructField("health_score_override", IntegerType(), True),
    ])


@pytest.fixture
def sample_opportunities(spark, opportunity_schema):
    """Generate sample opportunity data for testing."""
    data = [
        # Healthy deal
        ("OPP-001", "Acme Corp Enterprise", "ACC-001", "Acme Corp", "REP-001", "Sarah Chen",
         250000.0, "Negotiation", "Commit", date(2024, 12, 15), date(2024, 9, 1),
         False, False, 85.0, 12, None, 4, True, False, None, 10.0, date(2024, 11, 10), 85),

        # At-risk deal
        ("OPP-002", "Beta Inc Expansion", "ACC-002", "Beta Inc", "REP-002", "David Kim",
         150000.0, "Proposal", "Best Case", date(2024, 11, 30), date(2024, 8, 15),
         False, False, 55.0, 28, None, 2, False, True, "Competitor A", 15.0, date(2024, 10, 25), 45),

        # Closed won
        ("OPP-003", "Gamma Ltd Platform", "ACC-003", "Gamma Ltd", "REP-001", "Sarah Chen",
         180000.0, "Closed Won", "Closed", date(2024, 10, 30), date(2024, 7, 1),
         True, True, 100.0, 0, 121, 5, True, False, None, 5.0, date(2024, 10, 28), 100),

        # Closed lost
        ("OPP-004", "Delta Corp Suite", "ACC-004", "Delta Corp", "REP-002", "David Kim",
         120000.0, "Closed Lost", "Omitted", date(2024, 10, 15), date(2024, 6, 1),
         True, False, 0.0, 0, 136, 1, False, True, "Competitor B", 25.0, date(2024, 9, 30), 0),
    ]

    return spark.createDataFrame(data, opportunity_schema)


# =============================================================================
# ACTIVITY FIXTURES
# =============================================================================

@pytest.fixture
def activity_schema():
    """Schema for activity data."""
    return StructType([
        StructField("activity_id", StringType(), False),
        StructField("activity_type", StringType(), True),
        StructField("opportunity_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("owner_id", StringType(), True),
        StructField("owner_name", StringType(), True),
        StructField("subject", StringType(), True),
        StructField("activity_date", DateType(), True),
        StructField("duration_minutes", IntegerType(), True),
        StructField("is_completed", BooleanType(), True),
    ])


@pytest.fixture
def sample_activities(spark, activity_schema):
    """Generate sample activity data for testing."""
    today = date.today()
    data = [
        ("ACT-001", "Call", "OPP-001", "ACC-001", "REP-001", "Sarah Chen",
         "Discovery call", today - timedelta(days=2), 30, True),
        ("ACT-002", "Email", "OPP-001", "ACC-001", "REP-001", "Sarah Chen",
         "Follow up", today - timedelta(days=1), None, True),
        ("ACT-003", "Meeting", "OPP-001", "ACC-001", "REP-001", "Sarah Chen",
         "Demo presentation", today - timedelta(days=5), 60, True),
        ("ACT-004", "Call", "OPP-002", "ACC-002", "REP-002", "David Kim",
         "Check-in call", today - timedelta(days=20), 15, True),
        ("ACT-005", "Demo", "OPP-003", "ACC-003", "REP-001", "Sarah Chen",
         "Final demo", today - timedelta(days=30), 90, True),
    ]

    return spark.createDataFrame(data, activity_schema)


# =============================================================================
# ACCOUNT FIXTURES
# =============================================================================

@pytest.fixture
def account_schema():
    """Schema for account data."""
    return StructType([
        StructField("account_id", StringType(), False),
        StructField("account_name", StringType(), True),
        StructField("industry", StringType(), True),
        StructField("annual_revenue", DoubleType(), True),
        StructField("employee_count", IntegerType(), True),
        StructField("segment", StringType(), True),
        StructField("region", StringType(), True),
        StructField("owner_id", StringType(), True),
        StructField("owner_name", StringType(), True),
        StructField("created_date", DateType(), True),
    ])


@pytest.fixture
def sample_accounts(spark, account_schema):
    """Generate sample account data for testing."""
    data = [
        ("ACC-001", "Acme Corp", "Technology", 50000000.0, 500, "Enterprise", "West",
         "REP-001", "Sarah Chen", date(2023, 1, 15)),
        ("ACC-002", "Beta Inc", "Manufacturing", 15000000.0, 150, "Mid-Market", "East",
         "REP-002", "David Kim", date(2023, 3, 22)),
        ("ACC-003", "Gamma Ltd", "Finance", 100000000.0, 1000, "Enterprise", "West",
         "REP-001", "Sarah Chen", date(2022, 11, 1)),
        ("ACC-004", "Delta Corp", "Healthcare", 8000000.0, 80, "SMB", "Central",
         "REP-002", "David Kim", date(2024, 1, 10)),
    ]

    return spark.createDataFrame(data, account_schema)


# =============================================================================
# SALES REP FIXTURES
# =============================================================================

@pytest.fixture
def sales_rep_schema():
    """Schema for sales rep data."""
    return StructType([
        StructField("rep_id", StringType(), False),
        StructField("rep_name", StringType(), True),
        StructField("manager_id", StringType(), True),
        StructField("manager_name", StringType(), True),
        StructField("region", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("quota", DoubleType(), True),
        StructField("start_date", DateType(), True),
        StructField("max_deals", IntegerType(), True),
    ])


@pytest.fixture
def sample_sales_reps(spark, sales_rep_schema):
    """Generate sample sales rep data for testing."""
    data = [
        ("REP-001", "Sarah Chen", "MGR-001", "Mike Wilson", "West", "Enterprise",
         1500000.0, date(2022, 6, 1), 15),
        ("REP-002", "David Kim", "MGR-001", "Mike Wilson", "East", "Mid-Market",
         1000000.0, date(2023, 1, 15), 20),
        ("REP-003", "Emily Rodriguez", "MGR-002", "Lisa Brown", "Central", "SMB",
         750000.0, date(2024, 3, 1), 25),
    ]

    return spark.createDataFrame(data, sales_rep_schema)


# =============================================================================
# STAGE BENCHMARK FIXTURES
# =============================================================================

@pytest.fixture
def stage_benchmarks(spark):
    """Generate stage benchmark data for testing."""
    schema = StructType([
        StructField("stage_name", StringType(), False),
        StructField("stage_order", IntegerType(), True),
        StructField("expected_days", IntegerType(), True),
        StructField("conversion_rate", DoubleType(), True),
    ])

    data = [
        ("Prospecting", 1, 14, 0.8),
        ("Discovery", 2, 21, 0.7),
        ("Solution Design", 3, 14, 0.75),
        ("Proposal", 4, 14, 0.6),
        ("Negotiation", 5, 14, 0.8),
        ("Verbal Commit", 6, 7, 0.95),
        ("Closed Won", 7, 0, 1.0),
        ("Closed Lost", 8, 0, 0.0),
    ]

    return spark.createDataFrame(data, schema)
