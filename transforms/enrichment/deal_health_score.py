# /transforms/enrichment/deal_health_score.py
"""
ENRICHMENT: Deal Health Score
==============================
Calculates a 0-100 health score for each opportunity.

This is the CORE scoring algorithm that powers deal prioritization,
risk alerts, and forecasting confidence.

SCORING COMPONENTS:
- Stage Velocity (25 pts): Is the deal moving through stages on time?
- Activity Recency (25 pts): When was the last customer engagement?
- Stakeholder Engagement (20 pts): Are multiple contacts involved?
- Competitive Pressure (15 pts): Is there known competition?
- Discount Risk (15 pts): Are we discounting heavily?

HEALTH CATEGORIES:
- Healthy (80-100): On track, no intervention needed
- Monitor (60-79): Watch closely, minor concerns
- At Risk (40-59): Needs attention, significant concerns
- Critical (0-39): Urgent intervention required
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType


@transform(
    opportunities=Input("/RevOps/Sample/opportunities"),
    stage_benchmarks=Input("/RevOps/Reference/stage_benchmarks"),
    output=Output("/RevOps/Enriched/opportunity_health_scored")
)
def compute(ctx, opportunities, stage_benchmarks, output):
    """
    Calculate Deal Health Score (0-100) for each opportunity.
    """

    opps_df = opportunities.dataframe()
    benchmarks_df = stage_benchmarks.dataframe()

    # ===========================================
    # JOIN WITH BENCHMARKS
    # ===========================================

    with_benchmarks = opps_df.join(
        benchmarks_df.select(
            F.col("stage").alias("benchmark_stage"),
            "benchmark_days",
            "max_healthy_days"
        ),
        opps_df.stage_name == F.col("benchmark_stage"),
        "left"
    )

    # ===========================================
    # 1. STAGE VELOCITY SCORE (25 points)
    # ===========================================
    # Are they moving through stages on time?

    scored = with_benchmarks.withColumn(
        "stage_velocity_score",
        F.when(
            F.col("stage_name").isin("Closed Won", "Closed Lost"),
            F.lit(25)  # Closed deals get full points
        ).when(
            F.col("benchmark_days").isNull(),
            F.lit(12)  # No benchmark, neutral score
        ).when(
            F.col("days_in_current_stage") <= F.col("benchmark_days"),
            F.lit(25)  # On track
        ).when(
            F.col("days_in_current_stage") <= F.col("benchmark_days") * 1.5,
            F.lit(18)  # Slightly behind
        ).when(
            F.col("days_in_current_stage") <= F.col("benchmark_days") * 2.0,
            F.lit(10)  # Behind
        ).when(
            F.col("days_in_current_stage") <= F.col("benchmark_days") * 3.0,
            F.lit(5)   # Very behind
        ).otherwise(
            F.lit(0)   # Stalled
        )
    )

    # ===========================================
    # 2. ACTIVITY RECENCY SCORE (25 points)
    # ===========================================
    # When was the last engagement?

    scored = scored.withColumn(
        "days_since_activity",
        F.datediff(F.current_date(), F.col("last_activity_date"))
    ).withColumn(
        "activity_score",
        F.when(
            F.col("stage_name").isin("Closed Won", "Closed Lost"),
            F.lit(25)  # Closed deals get full points
        ).when(
            F.col("days_since_activity") <= 3,
            F.lit(25)  # Very active
        ).when(
            F.col("days_since_activity") <= 7,
            F.lit(20)  # Active
        ).when(
            F.col("days_since_activity") <= 14,
            F.lit(12)  # Slowing down
        ).when(
            F.col("days_since_activity") <= 21,
            F.lit(5)   # Going dark
        ).otherwise(
            F.lit(0)   # Gone dark
        )
    )

    # ===========================================
    # 3. STAKEHOLDER ENGAGEMENT SCORE (20 points)
    # ===========================================
    # Multi-threading correlates with wins

    scored = scored.withColumn(
        "stakeholder_score",
        F.when(
            F.col("stage_name").isin("Closed Won", "Closed Lost"),
            F.lit(20)
        ).when(
            F.col("stakeholder_count") >= 5,
            F.lit(20)  # Excellent multi-threading
        ).when(
            F.col("stakeholder_count") >= 4,
            F.lit(17)
        ).when(
            F.col("stakeholder_count") >= 3,
            F.lit(14)
        ).when(
            F.col("stakeholder_count") >= 2,
            F.lit(10)
        ).otherwise(
            F.lit(4)   # Single-threaded risk
        )
    )

    # ===========================================
    # 4. COMPETITIVE PRESSURE SCORE (15 points)
    # ===========================================
    # Known competition adds risk

    scored = scored.withColumn(
        "competitive_score",
        F.when(
            F.col("stage_name").isin("Closed Won", "Closed Lost"),
            F.lit(15)
        ).when(
            F.col("competitor_mentioned").isNull(),
            F.lit(15)  # No known competition
        ).when(
            F.col("has_champion") == True,
            F.lit(10)  # Competition but we have champion
        ).otherwise(
            F.lit(5)   # Competition without champion
        )
    )

    # ===========================================
    # 5. DISCOUNT RISK SCORE (15 points)
    # ===========================================
    # Heavy discounting signals desperation

    scored = scored.withColumn(
        "discount_score",
        F.when(
            F.col("stage_name").isin("Closed Won", "Closed Lost"),
            F.lit(15)
        ).when(
            F.col("discount_percent") == 0,
            F.lit(15)  # Full price
        ).when(
            F.col("discount_percent") <= 10,
            F.lit(12)  # Standard discount
        ).when(
            F.col("discount_percent") <= 20,
            F.lit(8)   # Moderate discount
        ).when(
            F.col("discount_percent") <= 30,
            F.lit(4)   # Heavy discount
        ).otherwise(
            F.lit(1)   # Extreme discount
        )
    )

    # ===========================================
    # CALCULATE TOTAL HEALTH SCORE
    # ===========================================

    scored = scored.withColumn(
        "health_score",
        F.col("stage_velocity_score") +
        F.col("activity_score") +
        F.col("stakeholder_score") +
        F.col("competitive_score") +
        F.col("discount_score")
    )

    # ===========================================
    # ASSIGN HEALTH CATEGORY
    # ===========================================

    scored = scored.withColumn(
        "health_category",
        F.when(F.col("stage_name") == "Closed Won", F.lit("Won"))
        .when(F.col("stage_name") == "Closed Lost", F.lit("Lost"))
        .when(F.col("health_score") >= 80, F.lit("Healthy"))
        .when(F.col("health_score") >= 60, F.lit("Monitor"))
        .when(F.col("health_score") >= 40, F.lit("At Risk"))
        .otherwise(F.lit("Critical"))
    )

    # ===========================================
    # GENERATE RISK FLAGS (Array of strings)
    # ===========================================

    scored = scored.withColumn(
        "risk_flags",
        F.array_remove(
            F.array(
                F.when(
                    F.col("stage_velocity_score") <= 10,
                    F.lit("Stalled in stage")
                ).otherwise(F.lit(None)),
                F.when(
                    F.col("activity_score") <= 5,
                    F.lit("Gone dark")
                ).otherwise(F.lit(None)),
                F.when(
                    F.col("stakeholder_score") <= 10,
                    F.lit("Single-threaded")
                ).otherwise(F.lit(None)),
                F.when(
                    (F.col("competitive_score") <= 5) & (F.col("competitor_mentioned").isNotNull()),
                    F.lit("Competitive pressure")
                ).otherwise(F.lit(None)),
                F.when(
                    F.col("discount_score") <= 8,
                    F.lit("Heavy discounting")
                ).otherwise(F.lit(None)),
                F.when(
                    (F.col("has_champion") == False) & (F.col("probability") >= 0.4),
                    F.lit("No champion identified")
                ).otherwise(F.lit(None)),
            ),
            None  # Remove nulls from array
        )
    )

    # ===========================================
    # CALCULATE DAYS TO CLOSE
    # ===========================================

    scored = scored.withColumn(
        "days_to_close",
        F.when(
            F.col("is_closed") == True,
            F.lit(0)
        ).otherwise(
            F.datediff(F.col("close_date"), F.current_date())
        )
    )

    # ===========================================
    # SELECT FINAL COLUMNS
    # ===========================================

    result = scored.select(
        # Original opportunity fields
        "opportunity_id",
        "opportunity_name",
        "account_id",
        "account_name",
        "segment",
        "industry",
        "amount",
        "stage_name",
        "probability",
        "close_date",
        "days_in_current_stage",
        "owner_id",
        "owner_name",
        "manager_id",
        "manager_name",
        "region",
        "forecast_category",
        "created_date",
        "last_activity_date",
        "days_since_activity",
        "competitor_mentioned",
        "discount_percent",
        "has_champion",
        "stakeholder_count",
        "next_step",
        "loss_reason",
        "lead_source",
        "deal_type",
        "is_closed",
        "days_to_close",

        # Health score components
        "stage_velocity_score",
        "activity_score",
        "stakeholder_score",
        "competitive_score",
        "discount_score",

        # Final health metrics
        "health_score",
        "health_category",
        "risk_flags",

        # Benchmark reference
        "benchmark_days",
    )

    output.write_dataframe(result)
