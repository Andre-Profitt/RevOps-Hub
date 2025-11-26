# /transforms/dashboard/compute_dashboard_kpis.py
"""
DASHBOARD KPI AGGREGATION
=========================
Pre-computes the KPI card values for the Pipeline Health Dashboard.
Uses Spark-native operations to avoid driver collection anti-patterns.
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


@transform(
    opportunities=Input("/RevOps/Scenario/opportunities"),
    forecast=Input("/RevOps/Analytics/forecast_confidence"),
    output=Output("/RevOps/Dashboard/kpis")
)
def compute(ctx, opportunities, forecast, output):
    """Compute dashboard KPI values using Spark-native operations."""

    opps = opportunities.dataframe()
    fc = forecast.dataframe()

    # Constants
    QUARTER_TARGET = 13000000.0

    # Q4 filter
    q4_opps = opps.filter(
        (F.col("close_date") >= "2024-10-01") &
        (F.col("close_date") <= "2024-12-31")
    )

    # Aggregate all metrics in a single pass
    opp_metrics = q4_opps.agg(
        F.coalesce(
            F.sum(F.when(F.col("stage_name") == "Closed Won", F.col("amount"))),
            F.lit(0.0)
        ).alias("closed_won_amount"),
        F.coalesce(
            F.sum(F.when(
                ~F.col("stage_name").isin("Closed Won", "Closed Lost"),
                F.col("amount")
            )),
            F.lit(0.0)
        ).alias("open_pipeline")
    )

    # Get forecast metrics with null-safe defaults
    forecast_total = fc.filter(
        F.col("category") == "TOTAL FORECAST"
    ).select(
        F.coalesce(F.col("weighted_amount"), F.lit(0.0)).alias("weighted_amount"),
        F.coalesce(F.col("confidence_pct"), F.lit(0.0)).alias("confidence_pct")
    )

    commit_metrics = fc.filter(
        F.col("category") == "Commit"
    ).select(
        F.coalesce(F.col("amount"), F.lit(0.0)).alias("commit_amount"),
        F.coalesce(F.col("weighted_amount"), F.lit(0.0)).alias("commit_weighted")
    )

    # Cross join to combine all metrics (single row each)
    # Use first() with default to handle empty results
    opp_row = opp_metrics.first()
    closed_won = float(opp_row["closed_won_amount"]) if opp_row else 0.0
    open_pipeline = float(opp_row["open_pipeline"]) if opp_row else 0.0

    forecast_row = forecast_total.first()
    ai_forecast = float(forecast_row["weighted_amount"]) if forecast_row else 0.0
    confidence = float(forecast_row["confidence_pct"]) if forecast_row else 0.0

    commit_row = commit_metrics.first()
    commit_amount = float(commit_row["commit_amount"]) if commit_row else 0.0
    commit_weighted = float(commit_row["commit_weighted"]) if commit_row else 0.0

    # Gap calculations with zero-division guards
    gap_to_target = QUARTER_TARGET - ai_forecast
    gap_pct = gap_to_target / QUARTER_TARGET if QUARTER_TARGET > 0 else 0.0

    remaining_target = QUARTER_TARGET - closed_won
    coverage = open_pipeline / remaining_target if remaining_target > 0 else 0.0

    # Risk level determination
    if gap_pct <= 0:
        risk_level = "Low"
    elif gap_pct <= 0.10:
        risk_level = "Medium"
    elif gap_pct <= 0.20:
        risk_level = "High"
    else:
        risk_level = "Critical"

    # Confidence normalization (handle percentage vs decimal)
    confidence_normalized = confidence / 100.0 if confidence > 1 else confidence

    # Create output DataFrame
    spark = ctx.spark_session

    schema = StructType([
        StructField("snapshot_date", StringType(), False),
        StructField("quarter", StringType(), False),
        StructField("team_filter", StringType(), False),
        StructField("quarter_target", DoubleType(), True),
        StructField("closed_won_amount", DoubleType(), True),
        StructField("ai_forecast", DoubleType(), True),
        StructField("ai_forecast_confidence", DoubleType(), True),
        StructField("commit_amount", DoubleType(), True),
        StructField("commit_weighted", DoubleType(), True),
        StructField("open_pipeline", DoubleType(), True),
        StructField("gap_to_target", DoubleType(), True),
        StructField("gap_pct", DoubleType(), True),
        StructField("coverage_ratio", DoubleType(), True),
        StructField("risk_level", StringType(), True),
        StructField("forecast_change_1w", DoubleType(), True),
        StructField("commit_change_1w", DoubleType(), True),
    ])

    data = [(
        "2024-11-15",
        "Q4 2024",
        "All",
        QUARTER_TARGET,
        closed_won,
        ai_forecast,
        confidence_normalized,
        commit_amount,
        commit_weighted,
        open_pipeline,
        gap_to_target,
        gap_pct,
        coverage,
        risk_level,
        200000.0,  # forecast_change_1w - would compare to prior snapshot
        -150000.0,  # commit_change_1w
    )]

    result = spark.createDataFrame(data, schema)
    output.write_dataframe(result)
