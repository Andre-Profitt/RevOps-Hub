# /transforms/scenario/generate_scenario_accounts.py
"""
SCENARIO DATA GENERATOR: Accounts
=================================
Generates account data to support the Q4 scenario narrative.
Includes hero accounts and supporting accounts for opportunities.
"""

from transforms.api import transform, Output
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType
)
import random


@transform(
    output=Output("/RevOps/Scenario/accounts")
)
def compute(ctx, output):
    """Generate scenario account data."""

    spark = ctx.spark_session
    random.seed(42)

    accounts = []

    # ===========================================
    # HERO ACCOUNTS (from the scenario story)
    # ===========================================

    HERO_ACCOUNTS = [
        {
            "account_id": "ACC-001",
            "account_name": "Acme Corporation",
            "segment": "Enterprise",
            "industry": "Technology",
            "employee_count": 5000,
            "annual_revenue": 800000000,
            "is_customer": False,
            "current_arr": 0,
            "health_score": 48,
            "owner_id": "REP-002",
        },
        {
            "account_id": "ACC-002",
            "account_name": "GlobalTech Industries",
            "segment": "Enterprise",
            "industry": "Technology",
            "employee_count": 12000,
            "annual_revenue": 2500000000,
            "is_customer": True,
            "current_arr": 450000,
            "health_score": 92,
            "owner_id": "REP-004",
        },
        {
            "account_id": "ACC-003",
            "account_name": "Meridian Healthcare",
            "segment": "Enterprise",
            "industry": "Healthcare",
            "employee_count": 8000,
            "annual_revenue": 1200000000,
            "is_customer": False,
            "current_arr": 0,
            "health_score": 95,
            "owner_id": "REP-003",
        },
        {
            "account_id": "ACC-004",
            "account_name": "Pacific Financial Group",
            "segment": "Enterprise",
            "industry": "Financial Services",
            "employee_count": 6500,
            "annual_revenue": 950000000,
            "is_customer": False,
            "current_arr": 0,
            "health_score": 65,
            "owner_id": "REP-003",
        },
        {
            "account_id": "ACC-006",
            "account_name": "Vertex Software",
            "segment": "Mid-Market",
            "industry": "Technology",
            "employee_count": 450,
            "annual_revenue": 75000000,
            "is_customer": False,
            "current_arr": 0,
            "health_score": 35,
            "owner_id": "REP-005",
        },
        {
            "account_id": "ACC-008",
            "account_name": "Summit Education",
            "segment": "Mid-Market",
            "industry": "Education",
            "employee_count": 800,
            "annual_revenue": 120000000,
            "is_customer": True,
            "current_arr": 85000,
            "health_score": 88,
            "owner_id": "REP-009",
        },
        {
            "account_id": "ACC-014",
            "account_name": "Skyline Ventures",
            "segment": "SMB",
            "industry": "Technology",
            "employee_count": 85,
            "annual_revenue": 12000000,
            "is_customer": False,
            "current_arr": 0,
            "health_score": 42,
            "owner_id": "REP-010",
        },
    ]

    for acc in HERO_ACCOUNTS:
        accounts.append(Row(**acc))

    # ===========================================
    # SUPPORTING ACCOUNTS
    # ===========================================

    SUPPORTING_ACCOUNTS = [
        ("ACC-005", "Atlas Manufacturing", "Enterprise", "Manufacturing", 4200, 650000000),
        ("ACC-007", "Coastal Retail Co", "Mid-Market", "Retail", 1200, 180000000),
        ("ACC-009", "Harbor Logistics", "Mid-Market", "Logistics", 950, 140000000),
        ("ACC-010", "Titan Energy", "Enterprise", "Energy", 8500, 3200000000),
        ("ACC-011", "Continental Logistics", "Enterprise", "Logistics", 6200, 890000000),
        ("ACC-012", "Pinnacle Media", "Mid-Market", "Media", 600, 95000000),
        ("ACC-013", "Frontier Analytics", "Mid-Market", "Technology", 350, 55000000),
        ("ACC-015", "Cascade Health", "Mid-Market", "Healthcare", 1100, 165000000),
        ("ACC-016", "Redwood Partners", "Mid-Market", "Financial Services", 280, 42000000),
        ("ACC-017", "Maple Street Retail", "SMB", "Retail", 120, 18000000),
        ("ACC-018", "Cornerstone Consulting", "SMB", "Professional Services", 75, 9500000),
        ("ACC-019", "Brightside Marketing", "SMB", "Marketing", 45, 6500000),
        ("ACC-020", "Evergreen Solutions", "SMB", "Technology", 95, 14000000),
        # Competitive loss accounts
        ("ACC-021", "DataStream Corp", "Mid-Market", "Technology", 420, 68000000),
        ("ACC-022", "CloudNine Systems", "Mid-Market", "Technology", 380, 52000000),
        ("ACC-023", "Agile Works", "Mid-Market", "Technology", 290, 38000000),
        ("ACC-024", "FastTrack Inc", "Mid-Market", "Technology", 550, 85000000),
        ("ACC-025", "NoDecision LLC", "SMB", "Technology", 110, 15000000),
        ("ACC-026", "BudgetCut Co", "Mid-Market", "Retail", 340, 48000000),
        ("ACC-027", "Delayed Corp", "SMB", "Technology", 85, 11000000),
    ]

    reps = ["REP-001", "REP-002", "REP-003", "REP-004", "REP-005",
            "REP-006", "REP-007", "REP-008", "REP-009", "REP-010"]

    for acc_id, name, segment, industry, employees, revenue in SUPPORTING_ACCOUNTS:
        is_customer = random.choice([True, False, False])
        accounts.append(Row(
            account_id=acc_id,
            account_name=name,
            segment=segment,
            industry=industry,
            employee_count=employees,
            annual_revenue=float(revenue),
            is_customer=is_customer,
            current_arr=float(random.randint(50000, 200000)) if is_customer else 0.0,
            health_score=random.randint(50, 95) if is_customer else None,
            owner_id=random.choice(reps),
        ))

    # ===========================================
    # CREATE DATAFRAME
    # ===========================================

    schema = StructType([
        StructField("account_id", StringType(), False),
        StructField("account_name", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("industry", StringType(), True),
        StructField("employee_count", IntegerType(), True),
        StructField("annual_revenue", DoubleType(), True),
        StructField("is_customer", BooleanType(), True),
        StructField("current_arr", DoubleType(), True),
        StructField("health_score", IntegerType(), True),
        StructField("owner_id", StringType(), True),
    ])

    df = spark.createDataFrame(accounts, schema)
    output.write_dataframe(df)
