# /transforms/analytics/leading_indicator_analytics.py
"""
LEADING INDICATOR ANALYTICS
===========================
ML-powered predictive analytics for sales outcomes.

Models:
1. Win Probability - Likelihood of deal closing successfully
2. Velocity Predictor - Expected days to close
3. Slip Risk - Probability deal slips to next quarter
4. Amount Risk - Likelihood of discount/downsell

Feature Engineering:
- Activity patterns (frequency, recency, type mix)
- Stakeholder engagement depth
- Stage velocity vs benchmarks
- Historical patterns by segment/rep
- Competitive signals
- Hygiene compliance
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, BooleanType
)


@transform(
    deal_health=Input("/RevOps/Enriched/deal_health_scores"),
    activities=Input("/RevOps/Scenario/activities"),
    benchmarks=Input("/RevOps/Reference/stage_benchmarks"),
    rep_performance=Input("/RevOps/Analytics/rep_performance"),
    output=Output("/RevOps/Analytics/win_probability_scores")
)
def compute_win_probability(ctx, deal_health, activities, benchmarks, rep_performance, output):
    """
    Calculate win probability for each open opportunity.

    This is a rule-based scoring model that mimics ML predictions.
    In production, this would be replaced with a trained ML model.

    Features used:
    - Health score (deal quality)
    - Stage progression velocity
    - Activity recency and volume
    - Stakeholder count
    - Rep historical win rate
    - Competitive presence
    - Discount level
    """

    spark = ctx.spark_session
    SCENARIO_DATE = F.to_date(F.lit("2024-11-15"))

    # Load data
    opps_df = deal_health.dataframe()
    activities_df = activities.dataframe()
    benchmarks_df = benchmarks.dataframe()
    rep_df = rep_performance.dataframe()

    # Filter to open opportunities
    open_opps = opps_df.filter(F.col("is_closed") == False)

    # Get activity metrics per opportunity
    activity_metrics = activities_df.groupBy("opportunity_id").agg(
        F.count("*").alias("total_activities"),
        F.sum(F.when(F.col("activity_type") == "Meeting", 1).otherwise(0)).alias("meeting_count"),
        F.sum(F.when(F.col("activity_type") == "Call", 1).otherwise(0)).alias("call_count"),
        F.max("activity_date").alias("last_activity_date")
    )

    # Join with rep performance for historical win rate
    rep_win_rates = rep_df.select(
        F.col("rep_id").alias("owner_id"),
        F.col("win_rate").alias("rep_win_rate"),
        F.col("performance_tier")
    )

    # Join all data
    enriched = open_opps.join(
        activity_metrics, "opportunity_id", "left"
    ).join(
        rep_win_rates, "owner_id", "left"
    ).join(
        benchmarks_df.select(
            "stage_name",
            F.col("target_days").alias("stage_benchmark"),
            F.col("historical_conversion").alias("stage_conversion_rate")
        ),
        "stage_name",
        "left"
    )

    # Calculate feature scores (each 0-100, weighted)
    scored = enriched.withColumn(
        "days_since_activity",
        F.datediff(SCENARIO_DATE, F.col("last_activity_date"))
    ).withColumn(
        # Feature 1: Health Score (25% weight)
        "health_factor",
        F.col("health_score")
    ).withColumn(
        # Feature 2: Activity Recency (20% weight)
        "activity_recency_factor",
        F.when(F.col("days_since_activity") <= 3, 100)
        .when(F.col("days_since_activity") <= 7, 80)
        .when(F.col("days_since_activity") <= 14, 50)
        .when(F.col("days_since_activity") <= 21, 30)
        .otherwise(10)
    ).withColumn(
        # Feature 3: Activity Volume (10% weight)
        "activity_volume_factor",
        F.least(F.coalesce(F.col("total_activities"), F.lit(0)) * 10, F.lit(100))
    ).withColumn(
        # Feature 4: Stakeholder Engagement (15% weight)
        "stakeholder_factor",
        F.when(F.col("stakeholder_count") >= 5, 100)
        .when(F.col("stakeholder_count") >= 4, 90)
        .when(F.col("stakeholder_count") >= 3, 75)
        .when(F.col("stakeholder_count") >= 2, 50)
        .otherwise(20)
    ).withColumn(
        # Feature 5: Stage Velocity (10% weight)
        "velocity_factor",
        F.when(
            F.col("days_in_current_stage") <= F.coalesce(F.col("stage_benchmark"), F.lit(14)),
            100
        ).when(
            F.col("days_in_current_stage") <= F.coalesce(F.col("stage_benchmark"), F.lit(14)) * 1.5,
            70
        ).when(
            F.col("days_in_current_stage") <= F.coalesce(F.col("stage_benchmark"), F.lit(14)) * 2,
            40
        ).otherwise(15)
    ).withColumn(
        # Feature 6: Rep Track Record (10% weight)
        "rep_factor",
        F.coalesce(F.col("rep_win_rate"), F.lit(0.25)) * 100 * 2  # Scale to 0-100
    ).withColumn(
        # Feature 7: Competitive Risk (5% weight, negative)
        "competitive_factor",
        F.when(F.col("is_competitive") == True, 60).otherwise(100)
    ).withColumn(
        # Feature 8: Discount Risk (5% weight, negative)
        "discount_factor",
        F.when(F.coalesce(F.col("discount_percent"), F.lit(0)) > 30, 40)
        .when(F.coalesce(F.col("discount_percent"), F.lit(0)) > 20, 60)
        .when(F.coalesce(F.col("discount_percent"), F.lit(0)) > 10, 80)
        .otherwise(100)
    )

    # Calculate weighted win probability
    with_probability = scored.withColumn(
        "win_probability_raw",
        (F.col("health_factor") * 0.25) +
        (F.col("activity_recency_factor") * 0.20) +
        (F.col("activity_volume_factor") * 0.10) +
        (F.col("stakeholder_factor") * 0.15) +
        (F.col("velocity_factor") * 0.10) +
        (F.col("rep_factor") * 0.10) +
        (F.col("competitive_factor") * 0.05) +
        (F.col("discount_factor") * 0.05)
    ).withColumn(
        # Apply stage-based ceiling (can't have 90% prob in early stages)
        "stage_ceiling",
        F.when(F.col("stage_name") == "Prospecting", 40)
        .when(F.col("stage_name") == "Discovery", 55)
        .when(F.col("stage_name") == "Solution Design", 70)
        .when(F.col("stage_name") == "Proposal", 85)
        .when(F.col("stage_name") == "Negotiation", 95)
        .when(F.col("stage_name") == "Verbal Commit", 98)
        .otherwise(50)
    ).withColumn(
        "win_probability",
        F.least(F.col("win_probability_raw"), F.col("stage_ceiling"))
    ).withColumn(
        "win_probability_tier",
        F.when(F.col("win_probability") >= 75, "High")
        .when(F.col("win_probability") >= 50, "Medium")
        .when(F.col("win_probability") >= 25, "Low")
        .otherwise("Very Low")
    ).withColumn(
        # Confidence in the prediction (based on data completeness)
        "prediction_confidence",
        F.when(
            F.col("total_activities").isNotNull() &
            F.col("stakeholder_count").isNotNull() &
            F.col("rep_win_rate").isNotNull(),
            "High"
        ).when(
            F.col("total_activities").isNotNull() | F.col("stakeholder_count").isNotNull(),
            "Medium"
        ).otherwise("Low")
    )

    # Select output columns
    result = with_probability.select(
        "opportunity_id",
        "opportunity_name",
        "account_name",
        "owner_id",
        "owner_name",
        "amount",
        "stage_name",
        "close_date",
        "health_score",
        F.round(F.col("win_probability"), 1).alias("win_probability"),
        "win_probability_tier",
        "prediction_confidence",

        # Feature breakdown for explainability
        F.round(F.col("health_factor"), 1).alias("health_factor"),
        F.round(F.col("activity_recency_factor"), 1).alias("activity_recency_factor"),
        F.round(F.col("stakeholder_factor"), 1).alias("stakeholder_factor"),
        F.round(F.col("velocity_factor"), 1).alias("velocity_factor"),
        F.round(F.col("rep_factor"), 1).alias("rep_factor"),
        F.round(F.col("competitive_factor"), 1).alias("competitive_factor"),

        # Context
        F.coalesce(F.col("days_since_activity"), F.lit(0)).alias("days_since_activity"),
        F.coalesce(F.col("stakeholder_count"), F.lit(1)).alias("stakeholder_count"),
        F.coalesce(F.col("total_activities"), F.lit(0)).alias("total_activities"),

        # Weighted amount for forecasting
        F.round(F.col("amount") * F.col("win_probability") / 100, 0).alias("weighted_amount"),

        SCENARIO_DATE.alias("scored_date")
    ).orderBy(F.col("win_probability").desc())

    output.write_dataframe(result)


@transform(
    win_scores=Input("/RevOps/Analytics/win_probability_scores"),
    output=Output("/RevOps/Analytics/slip_risk_scores")
)
def compute_slip_risk(ctx, win_scores, output):
    """
    Calculate probability that a deal will slip to next quarter.

    Slip risk factors:
    - Close date proximity vs stage
    - Activity momentum (accelerating vs decelerating)
    - Forecast category vs health alignment
    - Historical slip patterns
    """

    df = win_scores.dataframe()
    SCENARIO_DATE = F.to_date(F.lit("2024-11-15"))
    QUARTER_END = F.to_date(F.lit("2024-12-31"))

    with_slip_risk = df.withColumn(
        "days_until_close",
        F.datediff(F.col("close_date"), SCENARIO_DATE)
    ).withColumn(
        "days_until_quarter_end",
        F.datediff(QUARTER_END, SCENARIO_DATE)
    ).withColumn(
        # Factor 1: Time pressure (close date vs remaining time)
        "time_pressure_risk",
        F.when(F.col("days_until_close") < 0, 95)  # Already past close date
        .when(F.col("days_until_close") < 7, 70)
        .when(F.col("days_until_close") < 14, 50)
        .when(F.col("days_until_close") < 30, 30)
        .otherwise(15)
    ).withColumn(
        # Factor 2: Stage alignment (is deal far enough along?)
        "stage_alignment_risk",
        F.when(
            (F.col("days_until_close") < 30) &
            (F.col("stage_name").isin("Prospecting", "Discovery", "Solution Design")),
            85  # Early stage with close date soon = high slip risk
        ).when(
            (F.col("days_until_close") < 14) &
            (F.col("stage_name").isin("Proposal")),
            60
        ).when(
            (F.col("days_until_close") < 7) &
            (~F.col("stage_name").isin("Verbal Commit")),
            75
        ).otherwise(20)
    ).withColumn(
        # Factor 3: Activity momentum
        "momentum_risk",
        F.when(F.col("days_since_activity") > 14, 80)
        .when(F.col("days_since_activity") > 7, 50)
        .when(F.col("days_since_activity") > 3, 25)
        .otherwise(10)
    ).withColumn(
        # Factor 4: Win probability inverse
        "win_prob_risk",
        100 - F.col("win_probability")
    ).withColumn(
        # Combined slip risk score
        "slip_risk_score",
        (F.col("time_pressure_risk") * 0.30) +
        (F.col("stage_alignment_risk") * 0.30) +
        (F.col("momentum_risk") * 0.20) +
        (F.col("win_prob_risk") * 0.20)
    ).withColumn(
        "slip_risk_tier",
        F.when(F.col("slip_risk_score") >= 70, "High")
        .when(F.col("slip_risk_score") >= 45, "Medium")
        .otherwise("Low")
    ).withColumn(
        "slip_risk_reason",
        F.when(F.col("days_until_close") < 0, "Close date already passed")
        .when(
            (F.col("stage_alignment_risk") >= 60),
            F.concat(F.lit("Still in "), F.col("stage_name"), F.lit(" with only "), F.col("days_until_close"), F.lit(" days left"))
        ).when(
            F.col("momentum_risk") >= 50,
            F.concat(F.lit("No activity in "), F.col("days_since_activity"), F.lit(" days"))
        ).when(
            F.col("win_prob_risk") >= 50,
            "Low win probability"
        ).otherwise("On track")
    )

    result = with_slip_risk.select(
        "opportunity_id",
        "opportunity_name",
        "account_name",
        "owner_id",
        "owner_name",
        "amount",
        "stage_name",
        "close_date",
        "days_until_close",
        "win_probability",
        F.round(F.col("slip_risk_score"), 1).alias("slip_risk_score"),
        "slip_risk_tier",
        "slip_risk_reason",
        F.round(F.col("time_pressure_risk"), 1).alias("time_pressure_factor"),
        F.round(F.col("stage_alignment_risk"), 1).alias("stage_alignment_factor"),
        F.round(F.col("momentum_risk"), 1).alias("momentum_factor"),
        # Amount at risk of slipping
        F.when(
            F.col("slip_risk_tier") == "High",
            F.col("amount")
        ).when(
            F.col("slip_risk_tier") == "Medium",
            F.col("amount") * 0.5
        ).otherwise(0).alias("slip_risk_amount"),
        F.to_date(F.lit("2024-11-15")).alias("scored_date")
    ).orderBy(F.col("slip_risk_score").desc())

    output.write_dataframe(result)


@transform(
    win_scores=Input("/RevOps/Analytics/win_probability_scores"),
    slip_scores=Input("/RevOps/Analytics/slip_risk_scores"),
    output=Output("/RevOps/Analytics/leading_indicators_summary")
)
def compute_leading_indicators_summary(ctx, win_scores, slip_scores, output):
    """
    Aggregate leading indicators into a summary for forecasting.
    """

    win_df = win_scores.dataframe()
    slip_df = slip_scores.dataframe()

    # Win probability summary
    win_summary = win_df.agg(
        F.count("*").alias("total_opportunities"),
        F.sum("amount").alias("total_pipeline"),
        F.sum("weighted_amount").alias("probability_weighted_pipeline"),
        F.round(F.avg("win_probability"), 1).alias("avg_win_probability"),

        # By tier
        F.sum(F.when(F.col("win_probability_tier") == "High", 1).otherwise(0)).alias("high_prob_count"),
        F.sum(F.when(F.col("win_probability_tier") == "High", F.col("amount")).otherwise(0)).alias("high_prob_amount"),
        F.sum(F.when(F.col("win_probability_tier") == "Medium", 1).otherwise(0)).alias("medium_prob_count"),
        F.sum(F.when(F.col("win_probability_tier") == "Medium", F.col("amount")).otherwise(0)).alias("medium_prob_amount"),
        F.sum(F.when(F.col("win_probability_tier") == "Low", 1).otherwise(0)).alias("low_prob_count"),
        F.sum(F.when(F.col("win_probability_tier") == "Low", F.col("amount")).otherwise(0)).alias("low_prob_amount"),

        # Feature insights
        F.round(F.avg("activity_recency_factor"), 1).alias("avg_activity_recency"),
        F.round(F.avg("stakeholder_factor"), 1).alias("avg_stakeholder_engagement"),
        F.round(F.avg("velocity_factor"), 1).alias("avg_velocity_score")
    )

    # Slip risk summary
    slip_summary = slip_df.agg(
        F.sum(F.when(F.col("slip_risk_tier") == "High", 1).otherwise(0)).alias("high_slip_risk_count"),
        F.sum(F.when(F.col("slip_risk_tier") == "High", F.col("amount")).otherwise(0)).alias("high_slip_risk_amount"),
        F.sum(F.when(F.col("slip_risk_tier") == "Medium", 1).otherwise(0)).alias("medium_slip_risk_count"),
        F.sum(F.when(F.col("slip_risk_tier") == "Medium", F.col("amount")).otherwise(0)).alias("medium_slip_risk_amount"),
        F.sum("slip_risk_amount").alias("total_slip_risk_amount"),
        F.round(F.avg("slip_risk_score"), 1).alias("avg_slip_risk")
    )

    # Cross join to combine (both are single-row)
    combined = win_summary.crossJoin(slip_summary).withColumn(
        "ai_forecast_adjustment",
        # Recommended adjustment to forecast based on leading indicators
        F.col("probability_weighted_pipeline") - F.col("total_slip_risk_amount")
    ).withColumn(
        "forecast_confidence",
        # Overall confidence in the forecast
        F.when(
            (F.col("avg_win_probability") >= 60) & (F.col("avg_slip_risk") < 40),
            "High"
        ).when(
            (F.col("avg_win_probability") >= 45) & (F.col("avg_slip_risk") < 55),
            "Medium"
        ).otherwise("Low")
    ).withColumn(
        "primary_risk_factor",
        F.when(F.col("avg_activity_recency") < 50, "Activity engagement declining")
        .when(F.col("avg_stakeholder_engagement") < 50, "Insufficient stakeholder coverage")
        .when(F.col("avg_velocity_score") < 50, "Deals progressing slowly")
        .when(F.col("avg_slip_risk") >= 50, "High slip risk concentration")
        .otherwise("No major concerns")
    ).withColumn(
        "scored_date",
        F.to_date(F.lit("2024-11-15"))
    )

    output.write_dataframe(combined)


@transform(
    win_scores=Input("/RevOps/Analytics/win_probability_scores"),
    slip_scores=Input("/RevOps/Analytics/slip_risk_scores"),
    output=Output("/RevOps/Analytics/deal_predictions")
)
def compute_deal_predictions(ctx, win_scores, slip_scores, output):
    """
    Combine win probability and slip risk into unified deal predictions.
    This powers the AI predictions panel in the webapp.
    """

    win_df = win_scores.dataframe()
    slip_df = slip_scores.dataframe().select(
        "opportunity_id",
        "slip_risk_score",
        "slip_risk_tier",
        "slip_risk_reason",
        "slip_risk_amount"
    )

    combined = win_df.join(slip_df, "opportunity_id", "left")

    # Generate AI-style prediction narrative
    with_narrative = combined.withColumn(
        "prediction_summary",
        F.when(
            (F.col("win_probability") >= 70) & (F.col("slip_risk_score") < 40),
            "Strong likelihood to close this quarter"
        ).when(
            (F.col("win_probability") >= 50) & (F.col("slip_risk_score") < 50),
            "Good probability with manageable risk"
        ).when(
            (F.col("win_probability") >= 50) & (F.col("slip_risk_score") >= 50),
            "Good potential but slip risk is elevated"
        ).when(
            (F.col("win_probability") < 50) & (F.col("slip_risk_score") >= 60),
            "At risk - needs intervention to stay on track"
        ).otherwise("Uncertain - requires close monitoring")
    ).withColumn(
        "recommended_focus",
        F.when(F.col("activity_recency_factor") < 50, "Re-engage - activity has dropped off")
        .when(F.col("stakeholder_factor") < 50, "Multi-thread - need more stakeholders")
        .when(F.col("velocity_factor") < 50, "Accelerate - deal is stalling")
        .when(F.col("slip_risk_score") >= 60, "Close date at risk - validate timeline")
        .otherwise("Continue current approach")
    ).withColumn(
        "expected_outcome",
        F.when(F.col("win_probability") >= 70, "WIN")
        .when(F.col("win_probability") >= 40, "TOSS-UP")
        .otherwise("UNLIKELY")
    )

    result = with_narrative.select(
        "opportunity_id",
        "opportunity_name",
        "account_name",
        "owner_id",
        "owner_name",
        "amount",
        "stage_name",
        "close_date",
        "health_score",
        F.col("win_probability"),
        "win_probability_tier",
        F.coalesce(F.col("slip_risk_score"), F.lit(0)).alias("slip_risk_score"),
        F.coalesce(F.col("slip_risk_tier"), F.lit("Low")).alias("slip_risk_tier"),
        "prediction_summary",
        "recommended_focus",
        "expected_outcome",
        "weighted_amount",
        F.coalesce(F.col("slip_risk_amount"), F.lit(0)).alias("slip_risk_amount"),

        # Feature breakdown
        "health_factor",
        "activity_recency_factor",
        "stakeholder_factor",
        "velocity_factor",
        "prediction_confidence",

        "scored_date"
    ).orderBy(
        F.when(F.col("expected_outcome") == "TOSS-UP", 1)  # Focus on toss-ups first
        .when(F.col("expected_outcome") == "WIN", 2)
        .otherwise(3),
        F.col("amount").desc()
    )

    output.write_dataframe(result)
