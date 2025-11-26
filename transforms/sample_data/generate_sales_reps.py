# /transforms/sample_data/generate_sales_reps.py
"""
SAMPLE DATA GENERATOR: Sales Reps
==================================
Generates sales rep and manager data.
"""

from transforms.api import transform, Output
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    DateType, IntegerType, BooleanType
)
import random
from datetime import datetime, timedelta


@transform(
    output=Output("/RevOps/Sample/sales_reps")
)
def compute(ctx, output):
    """Generate sales rep roster with quotas and performance data."""

    spark = ctx.spark_session
    random.seed(42)
    today = datetime.now().date()

    # Define the org structure
    REPS = [
        # West Region - Manager: Michael Torres
        ("Sarah Chen", "REP-001", "MGR-001", "Michael Torres", "West", "Enterprise", 1500000, 0.92),
        ("James Wilson", "REP-002", "MGR-001", "Michael Torres", "West", "Enterprise", 1200000, 0.78),
        ("Emily Rodriguez", "REP-003", "MGR-001", "Michael Torres", "West", "Mid-Market", 1400000, 1.15),

        # East Region - Manager: Jennifer Adams
        ("David Kim", "REP-004", "MGR-002", "Jennifer Adams", "East", "Enterprise", 1600000, 1.08),
        ("Lisa Wang", "REP-005", "MGR-002", "Jennifer Adams", "East", "Enterprise", 1300000, 0.65),
        ("Robert Johnson", "REP-006", "MGR-002", "Jennifer Adams", "East", "Mid-Market", 1450000, 0.88),

        # Central Region - Manager: Christopher Lee
        ("Amanda Foster", "REP-007", "MGR-003", "Christopher Lee", "Central", "Mid-Market", 1100000, 0.72),
        ("Daniel Martinez", "REP-008", "MGR-003", "Christopher Lee", "Central", "Mid-Market", 1250000, 0.95),
        ("Jessica Taylor", "REP-009", "MGR-003", "Christopher Lee", "Central", "SMB", 1350000, 1.22),

        # South Region - Manager: Rachel Green
        ("Kevin Brown", "REP-010", "MGR-004", "Rachel Green", "South", "SMB", 1000000, 0.55),
    ]

    MANAGERS = [
        ("Michael Torres", "MGR-001", "DIR-001", "Sarah Mitchell", "West", 4100000),
        ("Jennifer Adams", "MGR-002", "DIR-001", "Sarah Mitchell", "East", 4350000),
        ("Christopher Lee", "MGR-003", "DIR-002", "Tom Anderson", "Central", 3700000),
        ("Rachel Green", "MGR-004", "DIR-002", "Tom Anderson", "South", 2800000),
    ]

    reps = []

    for name, rep_id, mgr_id, mgr_name, region, segment, quota, attainment_pct in REPS:
        # Calculate derived metrics
        closed_revenue = quota * attainment_pct * random.uniform(0.85, 0.95)  # YTD
        pipeline = quota * random.uniform(2.0, 4.0)
        win_rate = random.uniform(0.18, 0.35)
        avg_deal_size = random.randint(50000, 200000)

        # Tenure affects performance patterns
        tenure_months = random.randint(6, 48)
        hire_date = today - timedelta(days=tenure_months * 30)

        reps.append(Row(
            rep_id=rep_id,
            rep_name=name,
            email=f"{name.lower().replace(' ', '.')}@company.com",
            manager_id=mgr_id,
            manager_name=mgr_name,
            region=region,
            segment=segment,
            role="Account Executive",
            hire_date=hire_date,
            tenure_months=tenure_months,
            is_active=True,
            annual_quota=float(quota),
            quarterly_quota=float(quota / 4),
            ytd_closed_revenue=float(closed_revenue),
            ytd_attainment=attainment_pct,
            current_pipeline=float(pipeline),
            pipeline_coverage=pipeline / (quota / 4),  # vs quarterly quota
            win_rate=win_rate,
            avg_deal_size=float(avg_deal_size),
            avg_sales_cycle_days=random.randint(30, 75),
            deals_in_pipeline=random.randint(8, 25),
            activities_this_month=random.randint(40, 120),
        ))

    schema = StructType([
        StructField("rep_id", StringType(), False),
        StructField("rep_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("manager_id", StringType(), True),
        StructField("manager_name", StringType(), True),
        StructField("region", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("role", StringType(), True),
        StructField("hire_date", DateType(), True),
        StructField("tenure_months", IntegerType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("annual_quota", DoubleType(), True),
        StructField("quarterly_quota", DoubleType(), True),
        StructField("ytd_closed_revenue", DoubleType(), True),
        StructField("ytd_attainment", DoubleType(), True),
        StructField("current_pipeline", DoubleType(), True),
        StructField("pipeline_coverage", DoubleType(), True),
        StructField("win_rate", DoubleType(), True),
        StructField("avg_deal_size", DoubleType(), True),
        StructField("avg_sales_cycle_days", IntegerType(), True),
        StructField("deals_in_pipeline", IntegerType(), True),
        StructField("activities_this_month", IntegerType(), True),
    ])

    df = spark.createDataFrame(reps, schema)
    output.write_dataframe(df)
