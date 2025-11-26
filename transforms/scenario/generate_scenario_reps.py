# /transforms/scenario/generate_scenario_reps.py
"""
SCENARIO DATA: Sales Reps
==========================
Rep data that matches the Q4 Crunch story.
Each rep has a specific narrative and performance pattern.
"""

from transforms.api import transform, Output
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    DateType, IntegerType, BooleanType
)
from datetime import datetime, timedelta


@transform(
    output=Output("/RevOps/Scenario/sales_reps")
)
def compute(ctx, output):
    """Generate story-driven sales rep data."""

    spark = ctx.spark_session

    # ===========================================
    # THE CAST - Each Rep Has a Story
    # ===========================================

    REPS = [
        # REP-001: Sarah Chen - Solid Performer
        {
            "rep_id": "REP-001",
            "rep_name": "Sarah Chen",
            "email": "sarah.chen@techflow.com",
            "manager_id": "MGR-001",
            "manager_name": "Michael Torres",
            "region": "West",
            "segment": "Enterprise",
            "role": "Senior Account Executive",
            "tenure_months": 28,
            "annual_quota": 4000000,
            "ytd_closed": 2800000,  # 92% pro-rated attainment
            "q4_closed": 1000000,
            "current_pipeline": 2400000,
            "win_rate": 0.26,
            "avg_deal_size": 185000,
            "avg_sales_cycle": 52,
            "avg_stakeholders": 3.2,
            "deals_in_pipeline": 7,
            "activities_this_month": 85,
            "story": "Solid, reliable performer. Strong discovery skills, slightly longer cycles.",
        },

        # REP-002: James Wilson - Veteran in a Slump
        {
            "rep_id": "REP-002",
            "rep_name": "James Wilson",
            "email": "james.wilson@techflow.com",
            "manager_id": "MGR-001",
            "manager_name": "Michael Torres",
            "region": "West",
            "segment": "Enterprise",
            "role": "Account Executive",
            "tenure_months": 36,
            "annual_quota": 3600000,
            "ytd_closed": 2100000,  # 78% - slumping
            "q4_closed": 500000,
            "current_pipeline": 2800000,
            "win_rate": 0.21,  # Below average
            "avg_deal_size": 165000,
            "avg_sales_cycle": 65,  # Deals dragging
            "avg_stakeholders": 2.1,  # Not multi-threading
            "deals_in_pipeline": 5,
            "activities_this_month": 62,  # Lower activity
            "story": "Was a top performer, now struggling. Deals stalling in negotiation. Needs coaching.",
        },

        # REP-003: Emily Rodriguez - Enterprise Closer
        {
            "rep_id": "REP-003",
            "rep_name": "Emily Rodriguez",
            "email": "emily.rodriguez@techflow.com",
            "manager_id": "MGR-001",
            "manager_name": "Michael Torres",
            "region": "West",
            "segment": "Enterprise",
            "role": "Senior Account Executive",
            "tenure_months": 32,
            "annual_quota": 4200000,
            "ytd_closed": 3600000,  # 115% - big deals
            "q4_closed": 1500000,
            "current_pipeline": 2100000,
            "win_rate": 0.29,
            "avg_deal_size": 320000,  # Bigger deals
            "avg_sales_cycle": 58,
            "avg_stakeholders": 3.8,
            "deals_in_pipeline": 5,
            "activities_this_month": 78,
            "story": "Enterprise specialist. 2 whale deals carrying her number. Meridian about to close.",
        },

        # REP-004: David Kim - Rising Star
        {
            "rep_id": "REP-004",
            "rep_name": "David Kim",
            "email": "david.kim@techflow.com",
            "manager_id": "MGR-002",
            "manager_name": "Jennifer Adams",
            "region": "East",
            "segment": "Enterprise",
            "role": "Account Executive",
            "tenure_months": 18,
            "annual_quota": 3800000,
            "ytd_closed": 3100000,  # 108% - improving fast
            "q4_closed": 1200000,
            "current_pipeline": 2600000,
            "win_rate": 0.28,
            "avg_deal_size": 195000,
            "avg_sales_cycle": 45,  # Fast cycles
            "avg_stakeholders": 4.0,  # Learned multi-threading
            "deals_in_pipeline": 8,
            "activities_this_month": 95,  # High activity
            "story": "Rising star. Adopted Jessica's multi-threading approach. GlobalTech closing soon.",
        },

        # REP-005: Lisa Wang - At Risk
        {
            "rep_id": "REP-005",
            "rep_name": "Lisa Wang",
            "email": "lisa.wang@techflow.com",
            "manager_id": "MGR-002",
            "manager_name": "Jennifer Adams",
            "region": "East",
            "segment": "Enterprise",
            "role": "Account Executive",
            "tenure_months": 14,
            "annual_quota": 3400000,
            "ytd_closed": 1650000,  # 65% - struggling
            "q4_closed": 200000,
            "current_pipeline": 1200000,  # Low pipeline
            "win_rate": 0.18,  # Low win rate
            "avg_deal_size": 140000,
            "avg_sales_cycle": 72,  # Deals drag
            "avg_stakeholders": 1.4,  # Single-threaded!
            "deals_in_pipeline": 4,
            "activities_this_month": 48,  # Low activity
            "story": "Multiple deals fell through. Single-threaded approach not working. Needs intervention.",
        },

        # REP-006: Robert Johnson - Steady Eddie
        {
            "rep_id": "REP-006",
            "rep_name": "Robert Johnson",
            "email": "robert.johnson@techflow.com",
            "manager_id": "MGR-002",
            "manager_name": "Jennifer Adams",
            "region": "East",
            "segment": "Mid-Market",
            "role": "Account Executive",
            "tenure_months": 24,
            "annual_quota": 3200000,
            "ytd_closed": 2100000,  # 88%
            "q4_closed": 800000,
            "current_pipeline": 1800000,
            "win_rate": 0.24,
            "avg_deal_size": 125000,
            "avg_sales_cycle": 48,
            "avg_stakeholders": 2.8,
            "deals_in_pipeline": 6,
            "activities_this_month": 72,
            "story": "Always hits, never exceeds. Consistent but needs to stretch.",
        },

        # REP-007: Amanda Foster - New Rep Ramping
        {
            "rep_id": "REP-007",
            "rep_name": "Amanda Foster",
            "email": "amanda.foster@techflow.com",
            "manager_id": "MGR-003",
            "manager_name": "Christopher Lee",
            "region": "Central",
            "segment": "Mid-Market",
            "role": "Account Executive",
            "tenure_months": 8,  # New
            "annual_quota": 2800000,
            "ytd_closed": 1500000,  # 72% for tenure
            "q4_closed": 300000,
            "current_pipeline": 1600000,
            "win_rate": 0.20,
            "avg_deal_size": 95000,
            "avg_sales_cycle": 55,
            "avg_stakeholders": 2.4,
            "deals_in_pipeline": 8,  # Many deals
            "activities_this_month": 92,  # High activity
            "story": "Still ramping. Good activity metrics, working on conversion. Potential.",
        },

        # REP-008: Daniel Martinez - Turnaround Story
        {
            "rep_id": "REP-008",
            "rep_name": "Daniel Martinez",
            "email": "daniel.martinez@techflow.com",
            "manager_id": "MGR-003",
            "manager_name": "Christopher Lee",
            "region": "Central",
            "segment": "Mid-Market",
            "role": "Account Executive",
            "tenure_months": 20,
            "annual_quota": 3000000,
            "ytd_closed": 2100000,  # 95% - turned around
            "q4_closed": 900000,
            "current_pipeline": 1900000,
            "win_rate": 0.25,
            "avg_deal_size": 110000,
            "avg_sales_cycle": 42,
            "avg_stakeholders": 3.1,
            "deals_in_pipeline": 7,
            "activities_this_month": 88,
            "story": "Was struggling Q2, coaching worked. Now back on track. Success story.",
        },

        # REP-009: Jessica Taylor - THE TOP PERFORMER
        {
            "rep_id": "REP-009",
            "rep_name": "Jessica Taylor",
            "email": "jessica.taylor@techflow.com",
            "manager_id": "MGR-003",
            "manager_name": "Christopher Lee",
            "region": "Central",
            "segment": "Mid-Market",
            "role": "Senior Account Executive",
            "tenure_months": 26,
            "annual_quota": 3400000,
            "ytd_closed": 3600000,  # 142% - CRUSHING IT
            "q4_closed": 1800000,
            "current_pipeline": 1400000,
            "win_rate": 0.38,  # HIGHEST WIN RATE
            "avg_deal_size": 145000,
            "avg_sales_cycle": 35,  # FASTEST CYCLES
            "avg_stakeholders": 4.2,  # SECRET: MULTI-THREADING
            "deals_in_pipeline": 6,
            "activities_this_month": 105,  # High but efficient
            "story": "Top performer. Secret: 4.2 stakeholders avg. Ready for promotion to Senior.",
        },

        # REP-010: Kevin Brown - Struggling New Rep
        {
            "rep_id": "REP-010",
            "rep_name": "Kevin Brown",
            "email": "kevin.brown@techflow.com",
            "manager_id": "MGR-004",
            "manager_name": "Rachel Green",
            "region": "South",
            "segment": "SMB",
            "role": "Account Executive",
            "tenure_months": 6,  # Newest
            "annual_quota": 2400000,
            "ytd_closed": 1000000,  # 55% - struggling
            "q4_closed": 0,  # No Q4 closes yet!
            "current_pipeline": 800000,
            "win_rate": 0.15,  # Lowest
            "avg_deal_size": 42000,
            "avg_sales_cycle": 68,  # Long for SMB
            "avg_stakeholders": 1.3,  # Single-threaded
            "deals_in_pipeline": 5,
            "activities_this_month": 75,
            "story": "Newest rep, struggling. Qualification issues. Needs pairing with Jessica.",
        },
    ]

    # Convert to rows
    rows = []
    for rep in REPS:
        # Calculate derived metrics
        ytd_attainment = rep["ytd_closed"] / rep["annual_quota"] if rep["annual_quota"] > 0 else 0
        quarterly_quota = rep["annual_quota"] / 4
        pipeline_coverage = rep["current_pipeline"] / quarterly_quota if quarterly_quota > 0 else 0

        rows.append(Row(
            rep_id=rep["rep_id"],
            rep_name=rep["rep_name"],
            email=rep["email"],
            manager_id=rep["manager_id"],
            manager_name=rep["manager_name"],
            region=rep["region"],
            segment=rep["segment"],
            role=rep["role"],
            hire_date=datetime(2024, 11, 15).date() - timedelta(days=rep["tenure_months"] * 30),
            tenure_months=rep["tenure_months"],
            is_active=True,
            annual_quota=float(rep["annual_quota"]),
            quarterly_quota=float(quarterly_quota),
            ytd_closed_revenue=float(rep["ytd_closed"]),
            q4_closed_revenue=float(rep["q4_closed"]),
            ytd_attainment=ytd_attainment,
            current_pipeline=float(rep["current_pipeline"]),
            pipeline_coverage=pipeline_coverage,
            win_rate=rep["win_rate"],
            avg_deal_size=float(rep["avg_deal_size"]),
            avg_sales_cycle_days=rep["avg_sales_cycle"],
            avg_stakeholders=rep["avg_stakeholders"],
            deals_in_pipeline=rep["deals_in_pipeline"],
            activities_this_month=rep["activities_this_month"],
            story=rep["story"],
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
        StructField("q4_closed_revenue", DoubleType(), True),
        StructField("ytd_attainment", DoubleType(), True),
        StructField("current_pipeline", DoubleType(), True),
        StructField("pipeline_coverage", DoubleType(), True),
        StructField("win_rate", DoubleType(), True),
        StructField("avg_deal_size", DoubleType(), True),
        StructField("avg_sales_cycle_days", IntegerType(), True),
        StructField("avg_stakeholders", DoubleType(), True),
        StructField("deals_in_pipeline", IntegerType(), True),
        StructField("activities_this_month", IntegerType(), True),
        StructField("story", StringType(), True),
    ])

    df = spark.createDataFrame(rows, schema)
    output.write_dataframe(df)
