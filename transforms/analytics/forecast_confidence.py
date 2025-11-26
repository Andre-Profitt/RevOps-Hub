# /transforms/analytics/forecast_confidence.py
"""
FORECAST CONFIDENCE CALCULATOR
==============================
Provides realistic forecast assessments by:
1. Applying conversion rates by category
2. Adjusting for deal health
3. Comparing to target
4. Identifying "swing deals" that determine outcome

This powers the Forecast Confidence Advisor agent.
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F


@transform(
    opportunities=Input("/RevOps/Scenario/opportunities"),
    output=Output("/RevOps/Analytics/forecast_confidence")
)
def compute(ctx, opportunities, output):
    """Calculate health-weighted forecast confidence."""

    df = opportunities.dataframe()

    # Q4 date range
    Q4_START = "2024-10-01"
    Q4_END = "2024-12-31"
    QUARTER_TARGET = 13000000  # $13M target

    # Filter to Q4
    q4_opps = df.filter(
        (F.col("close_date") >= Q4_START) &
        (F.col("close_date") <= Q4_END)
    )

    # ===========================================
    # CATEGORY BREAKDOWN
    # ===========================================

    # Closed Won (100% confidence)
    closed_won = q4_opps.filter(F.col("stage_name") == "Closed Won").agg(
        F.sum("amount").alias("amount")
    ).withColumn("category", F.lit("Closed Won")
    ).withColumn("conversion_rate", F.lit(1.0)
    ).withColumn("confidence_pct", F.lit(100))

    # Commit (weighted by health)
    commit = q4_opps.filter(
        (F.col("forecast_category") == "Commit") &
        (F.col("stage_name") != "Closed Won") &
        (F.col("stage_name") != "Closed Lost")
    ).agg(
        F.sum("amount").alias("amount"),
        F.sum(
            F.when(F.col("health_score_override") >= 80, F.col("amount") * 0.95)
            .when(F.col("health_score_override") >= 60, F.col("amount") * 0.75)
            .otherwise(F.col("amount") * 0.50)
        ).alias("weighted_amount"),
        F.round(F.avg("health_score_override"), 0).alias("avg_health")
    ).withColumn("category", F.lit("Commit")
    ).withColumn(
        "conversion_rate",
        F.round(F.col("weighted_amount") / F.col("amount"), 2)
    ).withColumn("confidence_pct", F.round(F.col("conversion_rate") * 100, 0))

    # Best Case (40-50% historical rate, adjusted by health)
    best_case = q4_opps.filter(
        (F.col("forecast_category") == "Best Case") &
        (F.col("stage_name") != "Closed Lost")
    ).agg(
        F.sum("amount").alias("amount"),
        F.sum(
            F.when(F.col("health_score_override") >= 80, F.col("amount") * 0.50)
            .when(F.col("health_score_override") >= 60, F.col("amount") * 0.40)
            .otherwise(F.col("amount") * 0.25)
        ).alias("weighted_amount"),
        F.round(F.avg("health_score_override"), 0).alias("avg_health")
    ).withColumn("category", F.lit("Best Case")
    ).withColumn(
        "conversion_rate",
        F.round(F.col("weighted_amount") / F.col("amount"), 2)
    ).withColumn("confidence_pct", F.round(F.col("conversion_rate") * 100, 0))

    # Pipeline (20-30% historical rate)
    pipeline = q4_opps.filter(
        (F.col("forecast_category") == "Pipeline") &
        (F.col("stage_name") != "Closed Lost")
    ).agg(
        F.sum("amount").alias("amount"),
        F.sum(
            F.when(F.col("health_score_override") >= 80, F.col("amount") * 0.30)
            .when(F.col("health_score_override") >= 60, F.col("amount") * 0.20)
            .otherwise(F.col("amount") * 0.10)
        ).alias("weighted_amount"),
        F.round(F.avg("health_score_override"), 0).alias("avg_health")
    ).withColumn("category", F.lit("Pipeline")
    ).withColumn(
        "conversion_rate",
        F.round(F.col("weighted_amount") / F.col("amount"), 2)
    ).withColumn("confidence_pct", F.round(F.col("conversion_rate") * 100, 0))

    # ===========================================
    # COMBINE AND CALCULATE TOTALS
    # ===========================================

    # Union all categories
    combined = closed_won.select(
        "category", "amount", "confidence_pct"
    ).withColumn("weighted_amount", F.col("amount")).union(
        commit.select("category", "amount", "confidence_pct", "weighted_amount")
    ).union(
        best_case.select("category", "amount", "confidence_pct", "weighted_amount")
    ).union(
        pipeline.select("category", "amount", "confidence_pct", "weighted_amount")
    )

    # Add totals row
    totals = combined.agg(
        F.sum("amount").alias("amount"),
        F.sum("weighted_amount").alias("weighted_amount")
    ).withColumn("category", F.lit("TOTAL FORECAST")
    ).withColumn(
        "confidence_pct",
        F.round(F.col("weighted_amount") / F.col("amount") * 100, 0)
    )

    result = combined.union(totals.select("category", "amount", "confidence_pct", "weighted_amount"))

    # Add target comparison
    final = result.withColumn(
        "quarter_target", F.lit(QUARTER_TARGET)
    ).withColumn(
        "pct_of_target",
        F.when(
            F.col("category") == "TOTAL FORECAST",
            F.round(F.col("weighted_amount") / QUARTER_TARGET * 100, 1)
        ).otherwise(None)
    ).withColumn(
        "gap_to_target",
        F.when(
            F.col("category") == "TOTAL FORECAST",
            F.col("quarter_target") - F.col("weighted_amount")
        ).otherwise(None)
    ).withColumn(
        "risk_assessment",
        F.when(
            (F.col("category") == "TOTAL FORECAST") & (F.col("pct_of_target") >= 100),
            "ON TRACK"
        ).when(
            (F.col("category") == "TOTAL FORECAST") & (F.col("pct_of_target") >= 95),
            "ACHIEVABLE"
        ).when(
            (F.col("category") == "TOTAL FORECAST") & (F.col("pct_of_target") >= 85),
            "AT RISK"
        ).when(
            F.col("category") == "TOTAL FORECAST",
            "CRITICAL"
        ).otherwise(None)
    )

    output.write_dataframe(final)


@transform(
    opportunities=Input("/RevOps/Scenario/opportunities"),
    output=Output("/RevOps/Analytics/swing_deals")
)
def identify_swing_deals(ctx, opportunities, output):
    """Identify the deals that will determine if we hit target."""

    df = opportunities.dataframe()

    # Q4 open opportunities
    q4_open = df.filter(
        (F.col("close_date") >= "2024-10-01") &
        (F.col("close_date") <= "2024-12-31") &
        (F.col("is_closed") == False)
    )

    # Swing deals are large deals with moderate health (could go either way)
    swing_deals = q4_open.filter(
        (F.col("amount") >= 300000) |  # Large deals
        ((F.col("forecast_category") == "Commit") & (F.col("health_score_override") < 70)) |  # At-risk commit
        (F.col("is_hero_deal") == True)  # Named hero deals
    ).withColumn(
        "swing_category",
        F.when(
            (F.col("forecast_category") == "Commit") & (F.col("health_score_override") < 60),
            "MUST WIN - At Risk"
        ).when(
            (F.col("forecast_category") == "Commit") & (F.col("health_score_override") < 80),
            "MUST WIN - Monitor"
        ).when(
            F.col("forecast_category") == "Commit",
            "MUST WIN - Healthy"
        ).when(
            (F.col("forecast_category") == "Best Case") & (F.col("health_score_override") >= 70),
            "UPSIDE - Good Chance"
        ).otherwise("UPSIDE - Needs Work")
    ).withColumn(
        "win_probability",
        F.when(F.col("health_score_override") >= 80, 0.85)
        .when(F.col("health_score_override") >= 70, 0.70)
        .when(F.col("health_score_override") >= 60, 0.55)
        .when(F.col("health_score_override") >= 50, 0.40)
        .otherwise(0.25)
    ).withColumn(
        "expected_value",
        F.round(F.col("amount") * F.col("win_probability"), 0)
    ).select(
        "opportunity_id",
        "account_name",
        "opportunity_name",
        "amount",
        "owner_name",
        "stage_name",
        "forecast_category",
        "health_score_override",
        "swing_category",
        "win_probability",
        "expected_value",
        "days_in_current_stage",
        "stakeholder_count",
        "has_champion",
        "story_notes"
    ).orderBy(F.col("amount").desc())

    output.write_dataframe(swing_deals)
