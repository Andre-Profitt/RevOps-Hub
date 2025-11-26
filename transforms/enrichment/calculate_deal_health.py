# /transforms/enrichment/calculate_deal_health.py
"""
DEAL HEALTH SCORE CALCULATOR
============================
Calculates a composite health score for each opportunity based on 5 factors:
1. Velocity Score - Is the deal progressing normally?
2. Activity Score - Is there recent engagement?
3. Stakeholder Score - Is the deal multi-threaded?
4. Competition Score - Competitive pressure indicator
5. Discount Score - Heavy discounts = desperation

The scenario data includes health_score_override which represents the "real"
health we want to demonstrate. This transform shows the calculation methodology.
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@transform(
    opportunities=Input("/RevOps/Scenario/opportunities"),
    stage_benchmarks=Input("/RevOps/Reference/stage_benchmarks"),
    output=Output("/RevOps/Enriched/deal_health_scores")
)
def compute(ctx, opportunities, stage_benchmarks, output):
    """Calculate deal health scores with component breakdown."""

    opps = opportunities.dataframe()
    benchmarks = stage_benchmarks.dataframe()

    # Current date for calculations
    CURRENT_DATE = F.to_date(F.lit("2024-11-15"))

    # ===========================================
    # JOIN BENCHMARKS
    # ===========================================

    opps_with_benchmarks = opps.join(
        benchmarks.select(
            F.col("stage_name").alias("benchmark_stage"),
            F.col("expected_days").alias("benchmark_days")
        ),
        opps.stage_name == F.col("benchmark_stage"),
        "left"
    ).drop("benchmark_stage")

    # ===========================================
    # COMPONENT 1: VELOCITY SCORE (0-100)
    # ===========================================
    # How long has the deal been in current stage vs benchmark?

    velocity_scored = opps_with_benchmarks.withColumn(
        "velocity_ratio",
        F.when(F.col("benchmark_days").isNull(), 1.0)
        .otherwise(F.col("days_in_current_stage") / F.col("benchmark_days"))
    ).withColumn(
        "velocity_score",
        F.when(F.col("velocity_ratio") <= 0.5, 100)  # Ahead of schedule
        .when(F.col("velocity_ratio") <= 1.0, 85)     # On track
        .when(F.col("velocity_ratio") <= 1.5, 70)     # Slightly slow
        .when(F.col("velocity_ratio") <= 2.0, 50)     # Stalling
        .when(F.col("velocity_ratio") <= 3.0, 30)     # Major concern
        .otherwise(10)                                 # Critical
    )

    # ===========================================
    # COMPONENT 2: ACTIVITY SCORE (0-100)
    # ===========================================
    # When was the last customer engagement?

    activity_scored = velocity_scored.withColumn(
        "days_since_activity",
        F.datediff(CURRENT_DATE, F.col("last_activity_date"))
    ).withColumn(
        "activity_score",
        F.when(F.col("days_since_activity") <= 2, 100)   # Very active
        .when(F.col("days_since_activity") <= 5, 85)      # Normal
        .when(F.col("days_since_activity") <= 7, 70)      # Slowing
        .when(F.col("days_since_activity") <= 10, 50)     # Gone quiet
        .when(F.col("days_since_activity") <= 14, 30)     # Dark
        .otherwise(10)                                     # Gone cold
    )

    # ===========================================
    # COMPONENT 3: STAKEHOLDER SCORE (0-100)
    # ===========================================
    # Is the deal multi-threaded?

    stakeholder_scored = activity_scored.withColumn(
        "stakeholder_score",
        F.when(F.col("stakeholder_count") >= 5, 100)   # Excellent coverage
        .when(F.col("stakeholder_count") >= 4, 90)     # Strong
        .when(F.col("stakeholder_count") >= 3, 75)     # Acceptable
        .when(F.col("stakeholder_count") >= 2, 50)     # Risk
        .otherwise(20)                                  # Single-threaded (high risk)
    ).withColumn(
        "champion_bonus",
        F.when(F.col("has_champion") == True, 10).otherwise(0)
    ).withColumn(
        "stakeholder_score",
        F.least(F.col("stakeholder_score") + F.col("champion_bonus"), F.lit(100))
    )

    # ===========================================
    # COMPONENT 4: COMPETITION SCORE (0-100)
    # ===========================================
    # Are competitors involved?

    competition_scored = stakeholder_scored.withColumn(
        "competition_score",
        F.when(F.col("competitor_mentioned").isNull(), 100)     # No competitor
        .when(F.col("competitor_mentioned") == "None", 100)     # No competitor
        .when(F.col("is_competitive") == False, 90)             # Low risk
        .otherwise(60)                                           # Competitor active
    )

    # ===========================================
    # COMPONENT 5: DISCOUNT SCORE (0-100)
    # ===========================================
    # Heavy discounting = desperation

    discount_scored = competition_scored.withColumn(
        "discount_pct",
        F.when(F.col("discount_percent").isNull(), 0)
        .otherwise(F.col("discount_percent"))
    ).withColumn(
        "discount_score",
        F.when(F.col("discount_pct") == 0, 100)         # Full price
        .when(F.col("discount_pct") <= 10, 90)          # Normal
        .when(F.col("discount_pct") <= 15, 80)          # Acceptable
        .when(F.col("discount_pct") <= 20, 65)          # Concerning
        .when(F.col("discount_pct") <= 25, 45)          # Heavy
        .otherwise(25)                                   # Desperate
    )

    # ===========================================
    # COMPOSITE HEALTH SCORE
    # ===========================================
    # Weighted average of all components

    WEIGHTS = {
        "velocity": 0.25,
        "activity": 0.25,
        "stakeholder": 0.25,
        "competition": 0.15,
        "discount": 0.10
    }

    health_calculated = discount_scored.withColumn(
        "calculated_health_score",
        F.round(
            F.col("velocity_score") * WEIGHTS["velocity"] +
            F.col("activity_score") * WEIGHTS["activity"] +
            F.col("stakeholder_score") * WEIGHTS["stakeholder"] +
            F.col("competition_score") * WEIGHTS["competition"] +
            F.col("discount_score") * WEIGHTS["discount"],
            0
        )
    )

    # ===========================================
    # HEALTH CATEGORY AND RISK FLAGS
    # ===========================================

    final_df = health_calculated.withColumn(
        "health_category",
        F.when(F.col("health_score_override") >= 80, "Healthy")
        .when(F.col("health_score_override") >= 60, "Monitor")
        .when(F.col("health_score_override") >= 40, "At Risk")
        .otherwise("Critical")
    ).withColumn(
        "needs_attention",
        (F.col("health_score_override") < 70) & (F.col("is_closed") == False)
    ).withColumn(
        "risk_level",
        F.when(F.col("health_score_override") >= 80, 1)
        .when(F.col("health_score_override") >= 60, 2)
        .when(F.col("health_score_override") >= 40, 3)
        .otherwise(4)
    )

    # Select final columns
    output_df = final_df.select(
        "opportunity_id",
        "opportunity_name",
        "account_id",
        "account_name",
        "owner_id",
        "owner_name",
        "amount",
        "stage_name",
        "forecast_category",
        "close_date",
        "days_in_current_stage",
        "days_since_activity",
        "stakeholder_count",
        "has_champion",
        "is_competitive",
        "competitor_mentioned",
        "discount_pct",
        # Component scores
        "velocity_score",
        "activity_score",
        "stakeholder_score",
        "competition_score",
        "discount_score",
        # Composite scores
        "calculated_health_score",
        F.col("health_score_override").alias("health_score"),  # Use scenario override
        "health_category",
        "risk_level",
        "needs_attention",
        # Story elements
        "is_hero_deal",
        "story_notes"
    )

    output.write_dataframe(output_df)
