# /transforms/dashboard/compute_dashboard_kpis.py
"""
DASHBOARD KPI AGGREGATION
=========================
Pre-computes the KPI card values for the Pipeline Health Dashboard.
Optimized for fast dashboard loading.
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType


@transform(
    opportunities=Input("/RevOps/Scenario/opportunities"),
    forecast=Input("/RevOps/Analytics/forecast_confidence"),
    output=Output("/RevOps/Dashboard/kpis")
)
def compute(ctx, opportunities, forecast, output):
    """Compute dashboard KPI values."""

    opps = opportunities.dataframe()
    fc = forecast.dataframe()

    # Constants
    CURRENT_DATE = F.to_date(F.lit("2024-11-15"))
    QUARTER_TARGET = 13000000

    # Q4 filter
    q4_opps = opps.filter(
        (F.col("close_date") >= "2024-10-01") &
        (F.col("close_date") <= "2024-12-31")
    )

    # Calculate closed won
    closed_won = q4_opps.filter(
        F.col("stage_name") == "Closed Won"
    ).agg(
        F.sum("amount").alias("closed_won_amount")
    ).collect()[0]["closed_won_amount"] or 0

    # Get forecast totals
    forecast_total = fc.filter(
        F.col("category") == "TOTAL FORECAST"
    ).select("weighted_amount", "confidence_pct").collect()[0]

    ai_forecast = forecast_total["weighted_amount"]
    confidence = forecast_total["confidence_pct"]

    # Commit amount
    commit_row = fc.filter(F.col("category") == "Commit").collect()
    commit_amount = commit_row[0]["amount"] if commit_row else 0
    commit_weighted = commit_row[0]["weighted_amount"] if commit_row else 0

    # Open pipeline
    open_pipeline = q4_opps.filter(
        ~F.col("stage_name").isin("Closed Won", "Closed Lost")
    ).agg(
        F.sum("amount").alias("total_pipeline")
    ).collect()[0]["total_pipeline"] or 0

    # Gap calculations
    gap_to_target = QUARTER_TARGET - ai_forecast
    gap_pct = gap_to_target / QUARTER_TARGET if QUARTER_TARGET > 0 else 0
    coverage = open_pipeline / (QUARTER_TARGET - closed_won) if (QUARTER_TARGET - closed_won) > 0 else 0

    # Risk level
    if gap_pct <= 0:
        risk_level = "Low"
    elif gap_pct <= 0.10:
        risk_level = "Medium"
    elif gap_pct <= 0.20:
        risk_level = "High"
    else:
        risk_level = "Critical"

    # Create output row
    spark = ctx.spark_session

    result = spark.createDataFrame([{
        "snapshot_date": "2024-11-15",
        "quarter": "Q4 2024",
        "team_filter": "All",
        "quarter_target": float(QUARTER_TARGET),
        "closed_won_amount": float(closed_won),
        "ai_forecast": float(ai_forecast),
        "ai_forecast_confidence": float(confidence / 100),
        "commit_amount": float(commit_amount),
        "commit_weighted": float(commit_weighted),
        "open_pipeline": float(open_pipeline),
        "gap_to_target": float(gap_to_target),
        "gap_pct": float(gap_pct),
        "coverage_ratio": float(coverage),
        "risk_level": risk_level,
        # Trends (would compare to prior snapshot in production)
        "forecast_change_1w": 200000.0,
        "commit_change_1w": -150000.0,
    }])

    output.write_dataframe(result)
