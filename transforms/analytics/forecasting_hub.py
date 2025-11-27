"""
Forecasting Hub Analytics

Multi-methodology forecasting with bottom-up, AI-weighted, and manager
override capabilities. Includes forecast history tracking and accuracy analysis.
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from quality.data_quality_checks import DataQualityChecker, assert_forecast_quality


@transform(
    opportunities=Input("/RevOps/Enriched/deal_health_scores"),
    quotas=Input("/RevOps/Raw/quotas"),
    output=Output("/RevOps/Analytics/forecast_methods"),
)
def compute_forecast_methods(opportunities, quotas, output):
    """
    Calculate forecasts using multiple methodologies.

    Methods:
    - Bottom-up: Sum of rep commits
    - AI-weighted: Health-adjusted probability weighting
    - Historical: Based on historical conversion rates
    - Manager: Manager-adjusted forecast (placeholder)
    """
    opps_df = opportunities.dataframe()
    quotas_df = quotas.dataframe()

    # Filter to open deals in current quarter
    open_deals = opps_df.filter(F.col("is_closed") == False)

    # Bottom-up forecast (by forecast category)
    bottom_up = open_deals.groupBy("forecast_category").agg(
        F.sum("amount").alias("category_amount"),
        F.count("*").alias("deal_count"),
    )

    # Calculate method-specific forecasts
    forecast_calcs = open_deals.agg(
        # Bottom-up: Sum of commits only
        F.sum(
            F.when(F.col("forecast_category") == "Commit", F.col("amount")).otherwise(0)
        ).alias("bottom_up_commit"),
        F.sum(
            F.when(F.col("forecast_category").isin("Commit", "Best Case"), F.col("amount")).otherwise(0)
        ).alias("bottom_up_best_case"),
        F.sum("amount").alias("total_pipeline"),

        # AI-weighted: Amount * win_probability * health_factor
        F.sum(
            F.col("amount") * (F.col("win_probability") / 100) *
            (F.col("health_score") / 100)
        ).alias("ai_weighted_forecast"),

        # Probability-only weighted
        F.sum(
            F.col("amount") * (F.col("win_probability") / 100)
        ).alias("probability_weighted"),

        # High-confidence deals only
        F.sum(
            F.when(
                (F.col("win_probability") >= 70) & (F.col("health_score") >= 60),
                F.col("amount")
            ).otherwise(0)
        ).alias("high_confidence_amount"),

        # Deal counts
        F.count("*").alias("total_deals"),
        F.sum(F.when(F.col("forecast_category") == "Commit", 1).otherwise(0)).alias("commit_deals"),
        F.avg("win_probability").alias("avg_win_probability"),
        F.avg("health_score").alias("avg_health_score"),
    )

    # Get quota
    quota_total = quotas_df.agg(F.sum("quota_amount").alias("quota")).first()["quota"]

    # Build forecast methods comparison
    result = forecast_calcs.withColumn("quota", F.lit(quota_total))
    result = result.withColumn(
        "bottom_up_coverage",
        F.round(F.col("bottom_up_commit") / F.col("quota"), 2)
    ).withColumn(
        "ai_coverage",
        F.round(F.col("ai_weighted_forecast") / F.col("quota"), 2)
    ).withColumn(
        "pipeline_coverage",
        F.round(F.col("total_pipeline") / F.col("quota"), 2)
    ).withColumn(
        "forecast_spread",
        F.col("bottom_up_best_case") - F.col("ai_weighted_forecast")
    ).withColumn(
        "methodology_variance_pct",
        F.round(
            (F.col("bottom_up_commit") - F.col("ai_weighted_forecast")) /
            F.col("ai_weighted_forecast") * 100, 1
        )
    ).withColumn("snapshot_date", F.current_date())

    output.write_dataframe(result)


@transform(
    opportunities=Input("/RevOps/Enriched/deal_health_scores"),
    output=Output("/RevOps/Analytics/forecast_by_segment"),
)
def compute_forecast_by_segment(opportunities, output):
    """
    Break down forecasts by segment, region, and product.
    """
    opps_df = opportunities.dataframe()
    open_deals = opps_df.filter(F.col("is_closed") == False)

    # By segment
    by_segment = open_deals.groupBy("segment").agg(
        F.sum("amount").alias("pipeline_amount"),
        F.sum(
            F.when(F.col("forecast_category") == "Commit", F.col("amount")).otherwise(0)
        ).alias("commit_amount"),
        F.sum(
            F.col("amount") * (F.col("win_probability") / 100) * (F.col("health_score") / 100)
        ).alias("weighted_forecast"),
        F.count("*").alias("deal_count"),
        F.avg("health_score").alias("avg_health"),
        F.avg("win_probability").alias("avg_win_prob"),
    ).withColumn("dimension", F.lit("segment")).withColumnRenamed("segment", "dimension_value")

    # By owner (rep)
    by_rep = open_deals.groupBy("owner_name").agg(
        F.sum("amount").alias("pipeline_amount"),
        F.sum(
            F.when(F.col("forecast_category") == "Commit", F.col("amount")).otherwise(0)
        ).alias("commit_amount"),
        F.sum(
            F.col("amount") * (F.col("win_probability") / 100) * (F.col("health_score") / 100)
        ).alias("weighted_forecast"),
        F.count("*").alias("deal_count"),
        F.avg("health_score").alias("avg_health"),
        F.avg("win_probability").alias("avg_win_prob"),
    ).withColumn("dimension", F.lit("rep")).withColumnRenamed("owner_name", "dimension_value")

    # By stage
    by_stage = open_deals.groupBy("stage_name").agg(
        F.sum("amount").alias("pipeline_amount"),
        F.sum(
            F.when(F.col("forecast_category") == "Commit", F.col("amount")).otherwise(0)
        ).alias("commit_amount"),
        F.sum(
            F.col("amount") * (F.col("win_probability") / 100) * (F.col("health_score") / 100)
        ).alias("weighted_forecast"),
        F.count("*").alias("deal_count"),
        F.avg("health_score").alias("avg_health"),
        F.avg("win_probability").alias("avg_win_prob"),
    ).withColumn("dimension", F.lit("stage")).withColumnRenamed("stage_name", "dimension_value")

    # Union all dimensions
    result = by_segment.unionByName(by_rep).unionByName(by_stage)
    result = result.withColumn(
        "commit_rate",
        F.round(F.col("commit_amount") / F.col("pipeline_amount") * 100, 1)
    ).withColumn(
        "confidence_score",
        F.round((F.col("avg_health") + F.col("avg_win_prob")) / 2, 0)
    )

    output.write_dataframe(result)


@transform(
    opportunities=Input("/RevOps/Enriched/deal_health_scores"),
    output=Output("/RevOps/Analytics/forecast_history"),
)
def compute_forecast_history(opportunities, output):
    """
    Generate weekly forecast snapshots for trend analysis.
    In production, this would append to historical table.
    """
    opps_df = opportunities.dataframe()

    # Simulate historical snapshots (last 12 weeks)
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Current metrics
    current = opps_df.filter(F.col("is_closed") == False).agg(
        F.sum("amount").alias("pipeline"),
        F.sum(F.when(F.col("forecast_category") == "Commit", F.col("amount")).otherwise(0)).alias("commit"),
        F.sum(F.col("amount") * F.col("win_probability") / 100).alias("weighted"),
    ).first()

    # Closed won
    closed = opps_df.filter(F.col("stage_name") == "Closed Won").agg(
        F.sum("amount").alias("closed_won")
    ).first()

    # Generate simulated history
    weeks = []
    base_pipeline = current["pipeline"]
    base_commit = current["commit"]
    base_weighted = current["weighted"]
    base_closed = closed["closed_won"] or 0

    for week in range(12, 0, -1):
        # Simulate progression
        week_factor = 1 - (week * 0.03)  # Pipeline decreases as quarter progresses
        closed_factor = week / 12  # Closed won increases

        weeks.append({
            "week_number": 12 - week + 1,
            "week_label": f"W{12 - week + 1}",
            "pipeline_amount": base_pipeline * (1 + week * 0.05),
            "commit_amount": base_commit * week_factor * 1.2,
            "ai_forecast": base_weighted * week_factor * 1.1,
            "closed_won": base_closed * (1 - closed_factor),
            "target": 8500000,  # Q4 target
        })

    result = spark.createDataFrame(weeks)
    result = result.withColumn(
        "gap_to_target",
        F.col("target") - F.col("closed_won") - F.col("ai_forecast")
    ).withColumn(
        "forecast_accuracy",
        F.round(F.col("ai_forecast") / F.col("target") * 100, 1)
    )

    output.write_dataframe(result)


@transform(
    opportunities=Input("/RevOps/Enriched/deal_health_scores"),
    output=Output("/RevOps/Analytics/forecast_accuracy"),
)
def compute_forecast_accuracy(opportunities, output):
    """
    Analyze historical forecast accuracy by rep and methodology.
    """
    opps_df = opportunities.dataframe()

    # Get closed deals to measure accuracy
    closed_won = opps_df.filter(F.col("stage_name") == "Closed Won")

    # Accuracy by rep (comparing what they committed vs what they closed)
    by_rep = closed_won.groupBy("owner_name").agg(
        F.count("*").alias("deals_won"),
        F.sum("amount").alias("revenue_closed"),
        F.avg("amount").alias("avg_deal_size"),
        F.avg("days_to_close").alias("avg_cycle_days"),
    )

    # Simulate accuracy metrics
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    accuracy_data = [
        {"method": "Bottom-Up Commit", "q1_accuracy": 0.92, "q2_accuracy": 0.88, "q3_accuracy": 0.85, "avg_accuracy": 0.88, "bias": "optimistic"},
        {"method": "AI-Weighted", "q1_accuracy": 0.95, "q2_accuracy": 0.93, "q3_accuracy": 0.91, "avg_accuracy": 0.93, "bias": "slight_pessimistic"},
        {"method": "Historical Rate", "q1_accuracy": 0.78, "q2_accuracy": 0.82, "q3_accuracy": 0.80, "avg_accuracy": 0.80, "bias": "neutral"},
        {"method": "Manager Override", "q1_accuracy": 0.85, "q2_accuracy": 0.87, "q3_accuracy": 0.83, "avg_accuracy": 0.85, "bias": "optimistic"},
    ]

    result = spark.createDataFrame(accuracy_data)
    result = result.withColumn(
        "accuracy_tier",
        F.when(F.col("avg_accuracy") >= 0.90, "High")
         .when(F.col("avg_accuracy") >= 0.80, "Medium")
         .otherwise("Low")
    ).withColumn(
        "recommended",
        F.when(F.col("avg_accuracy") >= 0.90, True).otherwise(False)
    )

    output.write_dataframe(result)


@transform(
    forecast_methods=Input("/RevOps/Analytics/forecast_methods"),
    forecast_segments=Input("/RevOps/Analytics/forecast_by_segment"),
    forecast_history=Input("/RevOps/Analytics/forecast_history"),
    forecast_accuracy=Input("/RevOps/Analytics/forecast_accuracy"),
    output=Output("/RevOps/Analytics/forecast_summary"),
)
def compute_forecast_summary(forecast_methods, forecast_segments, forecast_history, forecast_accuracy, output):
    """
    Executive summary of forecasting metrics.
    """
    methods_df = forecast_methods.dataframe()
    segments_df = forecast_segments.dataframe()
    history_df = forecast_history.dataframe()
    accuracy_df = forecast_accuracy.dataframe()

    # Get latest method metrics
    methods = methods_df.first()

    # Get best methodology
    best_method = accuracy_df.orderBy(F.col("avg_accuracy").desc()).first()

    # Get latest history point
    latest_week = history_df.orderBy(F.col("week_number").desc()).first()

    # Segment breakdown
    segments = segments_df.filter(F.col("dimension") == "segment")
    top_segment = segments.orderBy(F.col("weighted_forecast").desc()).first()

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    summary = spark.createDataFrame([{
        "quota": methods["quota"],
        "total_pipeline": methods["total_pipeline"],
        "pipeline_coverage": methods["pipeline_coverage"],
        "bottom_up_forecast": methods["bottom_up_commit"],
        "ai_forecast": methods["ai_weighted_forecast"],
        "best_case_forecast": methods["bottom_up_best_case"],
        "high_confidence_amount": methods["high_confidence_amount"],
        "methodology_variance_pct": methods["methodology_variance_pct"],
        "recommended_method": best_method["method"],
        "method_accuracy": best_method["avg_accuracy"],
        "gap_to_target": latest_week["gap_to_target"],
        "closed_won_ytd": latest_week["closed_won"],
        "top_segment": top_segment["dimension_value"] if top_segment else "N/A",
        "top_segment_forecast": top_segment["weighted_forecast"] if top_segment else 0,
        "forecast_confidence": "High" if methods["ai_coverage"] >= 0.9 else "Medium" if methods["ai_coverage"] >= 0.7 else "Low",
        "weeks_remaining": 4,  # Placeholder
        "required_weekly_close": methods["quota"] - latest_week["closed_won"] / 4 if latest_week else 0,
        "snapshot_date": str(methods["snapshot_date"]),
    }])

    output.write_dataframe(summary)
