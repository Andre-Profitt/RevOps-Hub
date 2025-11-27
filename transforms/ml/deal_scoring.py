# /transforms/ml/deal_scoring.py
"""
DEAL SCORING MODEL
==================
ML-based deal scoring using gradient boosting to predict win probability.

This module provides:
1. Feature engineering from opportunity and activity data
2. Model training with historical win/loss outcomes
3. Scoring inference for open deals
4. Model performance monitoring

Outputs:
- /RevOps/ML/deal_features: Engineered features for each deal
- /RevOps/ML/deal_scores: Predicted win probability and risk factors
- /RevOps/ML/model_performance: Model accuracy metrics over time
"""

from transforms.api import transform, Input, Output
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, ArrayType, BooleanType
)
from datetime import datetime, timedelta
import json
from quality.data_quality_checks import DataQualityChecker


# =============================================================================
# FEATURE DEFINITIONS
# =============================================================================

# Features used for deal scoring
DEAL_FEATURES = [
    # Deal characteristics
    "deal_age_days",
    "stage_order",
    "stage_tenure_days",
    "amount_log",
    "amount_vs_avg",

    # Activity signals
    "total_activities",
    "activities_last_7d",
    "activities_last_14d",
    "days_since_last_activity",
    "meetings_count",
    "emails_count",
    "calls_count",

    # Engagement patterns
    "activity_momentum",  # trend in activity frequency
    "engagement_score",
    "multi_thread_score",  # contacts engaged

    # Historical signals
    "owner_win_rate",
    "owner_avg_deal_size",
    "account_previous_wins",
    "account_previous_losses",
    "segment_win_rate",

    # Stage progression
    "stage_velocity_vs_benchmark",
    "stages_skipped",
    "regression_count",  # times deal moved backward

    # Timing signals
    "close_date_pushed_count",
    "days_to_close",
    "quarter_end_proximity",
]


# =============================================================================
# FEATURE ENGINEERING
# =============================================================================

@transform(
    opportunities=Input("/RevOps/Staging/opportunities"),
    activities=Input("/RevOps/Staging/activities"),
    accounts=Input("/RevOps/Staging/accounts"),
    benchmarks=Input("/RevOps/Reference/stage_benchmarks"),
    historical_deals=Input("/RevOps/Analytics/win_loss_details"),
    output=Output("/RevOps/ML/deal_features"),
)
def engineer_deal_features(
    ctx,
    opportunities: DataFrame,
    activities: DataFrame,
    accounts: DataFrame,
    benchmarks: DataFrame,
    historical_deals: DataFrame,
    output,
):
    """
    Engineer features for deal scoring model.
    """
    spark = ctx.spark_session
    now = datetime.utcnow()

    # Stage order mapping
    stage_order = {
        "Prospecting": 1, "Discovery": 2, "Solution Design": 3,
        "Proposal": 4, "Negotiation": 5, "Verbal Commit": 6,
        "Closed Won": 7, "Closed Lost": 8
    }

    # ----- Base deal features -----
    deals = opportunities.filter(F.col("is_closed") == False)

    deals = deals.withColumn(
        "deal_age_days",
        F.datediff(F.lit(now), F.col("created_date"))
    ).withColumn(
        "stage_order",
        F.when(F.col("stage_name") == "Prospecting", 1)
        .when(F.col("stage_name") == "Discovery", 2)
        .when(F.col("stage_name") == "Solution Design", 3)
        .when(F.col("stage_name") == "Proposal", 4)
        .when(F.col("stage_name") == "Negotiation", 5)
        .when(F.col("stage_name") == "Verbal Commit", 6)
        .otherwise(0)
    ).withColumn(
        "amount_log",
        F.log1p(F.col("amount"))
    ).withColumn(
        "days_to_close",
        F.datediff(F.col("close_date"), F.lit(now))
    )

    # Quarter end proximity (0-1, higher = closer to quarter end)
    deals = deals.withColumn(
        "quarter_end_proximity",
        F.when(
            F.dayofmonth(F.col("close_date")).isin([28, 29, 30, 31]) &
            F.month(F.col("close_date")).isin([3, 6, 9, 12]),
            1.0
        ).when(
            F.month(F.col("close_date")).isin([3, 6, 9, 12]),
            0.5
        ).otherwise(0.0)
    )

    # ----- Activity features -----
    activity_agg = activities.filter(
        F.col("opportunity_id").isNotNull()
    ).groupBy("opportunity_id").agg(
        F.count("*").alias("total_activities"),
        F.sum(F.when(
            F.col("activity_date") >= F.date_sub(F.lit(now), 7), 1
        ).otherwise(0)).alias("activities_last_7d"),
        F.sum(F.when(
            F.col("activity_date") >= F.date_sub(F.lit(now), 14), 1
        ).otherwise(0)).alias("activities_last_14d"),
        F.datediff(F.lit(now), F.max("activity_date")).alias("days_since_last_activity"),
        F.sum(F.when(F.col("activity_type") == "Meeting", 1).otherwise(0)).alias("meetings_count"),
        F.sum(F.when(F.col("activity_type") == "Email", 1).otherwise(0)).alias("emails_count"),
        F.sum(F.when(F.col("activity_type") == "Call", 1).otherwise(0)).alias("calls_count"),
        F.countDistinct("contact_id").alias("contacts_engaged"),
    )

    # Activity momentum (recent vs older activity)
    activity_agg = activity_agg.withColumn(
        "activity_momentum",
        F.when(
            F.col("activities_last_14d") > 0,
            F.col("activities_last_7d") / F.col("activities_last_14d")
        ).otherwise(0)
    ).withColumn(
        "multi_thread_score",
        F.least(F.col("contacts_engaged") / 3.0, F.lit(1.0))
    )

    deals = deals.join(activity_agg, "opportunity_id", "left").fillna({
        "total_activities": 0,
        "activities_last_7d": 0,
        "activities_last_14d": 0,
        "days_since_last_activity": 999,
        "meetings_count": 0,
        "emails_count": 0,
        "calls_count": 0,
        "activity_momentum": 0,
        "multi_thread_score": 0,
    })

    # ----- Owner historical performance -----
    owner_stats = historical_deals.filter(
        F.col("is_closed") == True
    ).groupBy("owner_id").agg(
        F.avg(F.when(F.col("is_won") == True, 1).otherwise(0)).alias("owner_win_rate"),
        F.avg("amount").alias("owner_avg_deal_size"),
        F.count("*").alias("owner_deal_count"),
    )

    deals = deals.join(owner_stats, "owner_id", "left").fillna({
        "owner_win_rate": 0.25,  # default to average
        "owner_avg_deal_size": 50000,
        "owner_deal_count": 0,
    })

    # ----- Account history -----
    account_stats = historical_deals.filter(
        F.col("is_closed") == True
    ).groupBy("account_id").agg(
        F.sum(F.when(F.col("is_won") == True, 1).otherwise(0)).alias("account_previous_wins"),
        F.sum(F.when(F.col("is_won") == False, 1).otherwise(0)).alias("account_previous_losses"),
    )

    deals = deals.join(account_stats, "account_id", "left").fillna({
        "account_previous_wins": 0,
        "account_previous_losses": 0,
    })

    # ----- Segment win rates -----
    segment_stats = historical_deals.filter(
        F.col("is_closed") == True
    ).groupBy("segment").agg(
        F.avg(F.when(F.col("is_won") == True, 1).otherwise(0)).alias("segment_win_rate"),
    )

    deals = deals.join(segment_stats, "segment", "left").fillna({
        "segment_win_rate": 0.25,
    })

    # ----- Stage velocity vs benchmark -----
    if benchmarks.count() > 0:
        benchmark_map = {
            row["stage_name"]: row["benchmark_days"]
            for row in benchmarks.collect()
        }

        # Calculate stage tenure
        deals = deals.withColumn(
            "stage_tenure_days",
            F.datediff(F.lit(now), F.col("last_stage_change_date"))
        ).withColumn(
            "stage_benchmark_days",
            F.when(F.col("stage_name") == "Prospecting", benchmark_map.get("Prospecting", 14))
            .when(F.col("stage_name") == "Discovery", benchmark_map.get("Discovery", 21))
            .when(F.col("stage_name") == "Solution Design", benchmark_map.get("Solution Design", 14))
            .when(F.col("stage_name") == "Proposal", benchmark_map.get("Proposal", 7))
            .when(F.col("stage_name") == "Negotiation", benchmark_map.get("Negotiation", 14))
            .when(F.col("stage_name") == "Verbal Commit", benchmark_map.get("Verbal Commit", 7))
            .otherwise(14)
        ).withColumn(
            "stage_velocity_vs_benchmark",
            F.col("stage_tenure_days") / F.col("stage_benchmark_days")
        )
    else:
        deals = deals.withColumn("stage_tenure_days", F.lit(0))
        deals = deals.withColumn("stage_velocity_vs_benchmark", F.lit(1.0))

    # ----- Engagement score -----
    deals = deals.withColumn(
        "engagement_score",
        (
            F.least(F.col("meetings_count") / 3.0, F.lit(1.0)) * 0.4 +
            F.least(F.col("total_activities") / 10.0, F.lit(1.0)) * 0.3 +
            F.col("multi_thread_score") * 0.3
        )
    )

    # ----- Amount vs average -----
    avg_amount = deals.agg(F.avg("amount")).first()[0] or 50000
    deals = deals.withColumn(
        "amount_vs_avg",
        F.col("amount") / F.lit(avg_amount)
    )

    # ----- Select final features -----
    feature_cols = [
        "opportunity_id",
        "opportunity_name",
        "account_id",
        "owner_id",
        "stage_name",
        "amount",
        "close_date",
        # Features
        "deal_age_days",
        "stage_order",
        "stage_tenure_days",
        "amount_log",
        "amount_vs_avg",
        "total_activities",
        "activities_last_7d",
        "activities_last_14d",
        "days_since_last_activity",
        "meetings_count",
        "emails_count",
        "calls_count",
        "activity_momentum",
        "engagement_score",
        "multi_thread_score",
        "owner_win_rate",
        "owner_avg_deal_size",
        "account_previous_wins",
        "account_previous_losses",
        "segment_win_rate",
        "stage_velocity_vs_benchmark",
        "days_to_close",
        "quarter_end_proximity",
    ]

    # Filter to existing columns
    existing_cols = [c for c in feature_cols if c in deals.columns]
    result = deals.select(existing_cols)

    # Add metadata
    result = result.withColumn("feature_timestamp", F.lit(now))

    output.write_dataframe(result)


# =============================================================================
# MODEL SCORING
# =============================================================================

@transform(
    features=Input("/RevOps/ML/deal_features"),
    model_weights=Input("/RevOps/ML/model_weights"),
    output=Output("/RevOps/ML/deal_scores"),
)
def score_deals(ctx, features: DataFrame, model_weights: DataFrame, output):
    """
    Score deals using the trained model weights.

    Uses a simplified scoring approach based on weighted feature contributions.
    In production, this would use a proper ML model (XGBoost, LightGBM, etc.)
    loaded from Foundry's model registry.
    """
    now = datetime.utcnow()

    # Load model weights (feature -> weight mapping)
    # Default weights if model not trained yet
    default_weights = {
        "stage_order": 0.15,
        "engagement_score": 0.12,
        "activity_momentum": 0.10,
        "owner_win_rate": 0.10,
        "meetings_count": 0.08,
        "multi_thread_score": 0.08,
        "account_previous_wins": 0.07,
        "days_since_last_activity": -0.08,
        "stage_velocity_vs_benchmark": -0.06,
        "days_to_close": -0.04,
        "amount_vs_avg": -0.02,
    }

    weights = default_weights
    if model_weights.count() > 0:
        weights = {
            row["feature"]: row["weight"]
            for row in model_weights.collect()
        }

    # Calculate base score
    scores = features

    # Normalize features to 0-1 range for scoring
    scores = scores.withColumn(
        "norm_stage",
        F.col("stage_order") / 6.0
    ).withColumn(
        "norm_activity",
        F.least(F.col("total_activities") / 20.0, F.lit(1.0))
    ).withColumn(
        "norm_recency",
        F.greatest(1.0 - F.col("days_since_last_activity") / 30.0, F.lit(0.0))
    ).withColumn(
        "norm_velocity",
        F.greatest(1.0 - (F.col("stage_velocity_vs_benchmark") - 1.0) / 2.0, F.lit(0.0))
    )

    # Calculate weighted score
    scores = scores.withColumn(
        "raw_score",
        (
            F.col("norm_stage") * weights.get("stage_order", 0.15) +
            F.col("engagement_score") * weights.get("engagement_score", 0.12) +
            F.col("activity_momentum") * weights.get("activity_momentum", 0.10) +
            F.col("owner_win_rate") * weights.get("owner_win_rate", 0.10) +
            F.least(F.col("meetings_count") / 5.0, F.lit(1.0)) * weights.get("meetings_count", 0.08) +
            F.col("multi_thread_score") * weights.get("multi_thread_score", 0.08) +
            F.least(F.col("account_previous_wins") / 3.0, F.lit(1.0)) * weights.get("account_previous_wins", 0.07) +
            F.col("norm_recency") * 0.10 +
            F.col("norm_velocity") * 0.10
        )
    )

    # Calibrate to probability (sigmoid-like transformation)
    scores = scores.withColumn(
        "win_probability",
        F.greatest(
            F.least(
                F.col("raw_score") * 1.2 + 0.1,  # Scale and shift
                F.lit(0.95)
            ),
            F.lit(0.05)
        )
    )

    # Determine risk factors
    scores = scores.withColumn(
        "risk_factors",
        F.array_distinct(F.array(
            F.when(F.col("days_since_last_activity") > 14, F.lit("No recent activity"))
            .otherwise(F.lit(None)),
            F.when(F.col("stage_velocity_vs_benchmark") > 1.5, F.lit("Slow stage progression"))
            .otherwise(F.lit(None)),
            F.when(F.col("meetings_count") == 0, F.lit("No meetings scheduled"))
            .otherwise(F.lit(None)),
            F.when(F.col("multi_thread_score") < 0.3, F.lit("Single-threaded deal"))
            .otherwise(F.lit(None)),
            F.when(F.col("engagement_score") < 0.3, F.lit("Low engagement"))
            .otherwise(F.lit(None)),
        ))
    ).withColumn(
        "risk_factors",
        F.expr("filter(risk_factors, x -> x IS NOT NULL)")
    )

    # Score confidence based on data quality
    scores = scores.withColumn(
        "score_confidence",
        F.when(F.col("total_activities") >= 5, "high")
        .when(F.col("total_activities") >= 2, "medium")
        .otherwise("low")
    )

    # Score band
    scores = scores.withColumn(
        "score_band",
        F.when(F.col("win_probability") >= 0.7, "High")
        .when(F.col("win_probability") >= 0.4, "Medium")
        .otherwise("Low")
    )

    # Select output columns
    result = scores.select(
        "opportunity_id",
        "opportunity_name",
        "account_id",
        "owner_id",
        "stage_name",
        "amount",
        "close_date",
        "win_probability",
        "score_band",
        "score_confidence",
        "risk_factors",
        "engagement_score",
        "activity_momentum",
        F.lit(now).alias("scored_at"),
    )

    output.write_dataframe(result)


# =============================================================================
# MODEL TRAINING
# =============================================================================

@transform(
    historical_deals=Input("/RevOps/Analytics/win_loss_details"),
    activities=Input("/RevOps/Staging/activities"),
    output=Output("/RevOps/ML/model_weights"),
)
def train_deal_model(ctx, historical_deals: DataFrame, activities: DataFrame, output):
    """
    Train deal scoring model on historical outcomes.

    This is a simplified logistic regression-style training.
    In production, use Foundry's ML infrastructure or PySpark MLlib.
    """
    spark = ctx.spark_session
    now = datetime.utcnow()

    # Filter to closed deals with outcomes
    training_data = historical_deals.filter(
        (F.col("is_closed") == True) &
        F.col("close_date").isNotNull()
    )

    if training_data.count() < 100:
        # Not enough data - use default weights
        default_weights = [
            ("stage_order", 0.15),
            ("engagement_score", 0.12),
            ("activity_momentum", 0.10),
            ("owner_win_rate", 0.10),
            ("meetings_count", 0.08),
            ("multi_thread_score", 0.08),
            ("account_previous_wins", 0.07),
            ("days_since_last_activity", -0.08),
            ("stage_velocity_vs_benchmark", -0.06),
        ]
        result = spark.createDataFrame(default_weights, ["feature", "weight"])
        result = result.withColumn("trained_at", F.lit(now))
        result = result.withColumn("training_samples", F.lit(0))
        result = result.withColumn("model_version", F.lit("default"))
        output.write_dataframe(result)
        return

    # Join with activity data
    activity_stats = activities.groupBy("opportunity_id").agg(
        F.count("*").alias("total_activities"),
        F.sum(F.when(F.col("activity_type") == "Meeting", 1).otherwise(0)).alias("meetings_count"),
    )

    training_data = training_data.join(activity_stats, "opportunity_id", "left").fillna({
        "total_activities": 0,
        "meetings_count": 0,
    })

    # Calculate feature correlations with win outcome
    # (Simplified - in production, use proper feature importance from trained model)

    training_data = training_data.withColumn(
        "outcome",
        F.when(F.col("is_won") == True, 1.0).otherwise(0.0)
    )

    # Calculate correlation coefficients for each feature
    correlations = []

    numeric_features = ["total_activities", "meetings_count", "amount"]

    for feature in numeric_features:
        if feature in training_data.columns:
            corr = training_data.stat.corr("outcome", feature)
            if corr is not None:
                correlations.append((feature, corr * 0.1))  # Scale to weight range

    # Add default weights for features we couldn't compute
    feature_defaults = {
        "stage_order": 0.15,
        "engagement_score": 0.12,
        "activity_momentum": 0.10,
        "owner_win_rate": 0.10,
        "multi_thread_score": 0.08,
        "account_previous_wins": 0.07,
        "days_since_last_activity": -0.08,
        "stage_velocity_vs_benchmark": -0.06,
    }

    for feature, weight in feature_defaults.items():
        if feature not in [c[0] for c in correlations]:
            correlations.append((feature, weight))

    # Create result DataFrame
    result = spark.createDataFrame(correlations, ["feature", "weight"])
    result = result.withColumn("trained_at", F.lit(now))
    result = result.withColumn("training_samples", F.lit(training_data.count()))
    result = result.withColumn("model_version", F.lit(f"v{now.strftime('%Y%m%d')}"))

    output.write_dataframe(result)


# =============================================================================
# MODEL PERFORMANCE MONITORING
# =============================================================================

@transform(
    scores=Input("/RevOps/ML/deal_scores"),
    outcomes=Input("/RevOps/Staging/opportunities"),
    previous_performance=Input("/RevOps/ML/model_performance"),
    output=Output("/RevOps/ML/model_performance"),
)
def monitor_model_performance(
    ctx,
    scores: DataFrame,
    outcomes: DataFrame,
    previous_performance: DataFrame,
    output,
):
    """
    Monitor model prediction accuracy against actual outcomes.
    """
    spark = ctx.spark_session
    now = datetime.utcnow()

    # Get recently closed deals that we had scored
    recent_outcomes = outcomes.filter(
        (F.col("is_closed") == True) &
        (F.col("close_date") >= F.date_sub(F.lit(now), 30))
    ).select(
        "opportunity_id",
        F.col("is_won").alias("actual_outcome"),
        "close_date",
    )

    # Join with scores
    predictions = scores.join(recent_outcomes, "opportunity_id", "inner")

    if predictions.count() == 0:
        # No predictions to evaluate yet
        result = previous_performance
        output.write_dataframe(result)
        return

    # Calculate accuracy metrics
    predictions = predictions.withColumn(
        "predicted_win",
        F.col("win_probability") >= 0.5
    ).withColumn(
        "correct",
        F.col("predicted_win") == F.col("actual_outcome")
    )

    # Aggregate metrics
    metrics = predictions.agg(
        F.count("*").alias("total_predictions"),
        F.sum(F.when(F.col("correct"), 1).otherwise(0)).alias("correct_predictions"),
        F.sum(F.when(
            (F.col("predicted_win") == True) & (F.col("actual_outcome") == True), 1
        ).otherwise(0)).alias("true_positives"),
        F.sum(F.when(
            (F.col("predicted_win") == True) & (F.col("actual_outcome") == False), 1
        ).otherwise(0)).alias("false_positives"),
        F.sum(F.when(
            (F.col("predicted_win") == False) & (F.col("actual_outcome") == True), 1
        ).otherwise(0)).alias("false_negatives"),
        F.sum(F.when(
            (F.col("predicted_win") == False) & (F.col("actual_outcome") == False), 1
        ).otherwise(0)).alias("true_negatives"),
        F.avg("win_probability").alias("avg_predicted_probability"),
        F.avg(F.when(F.col("actual_outcome") == True, 1).otherwise(0)).alias("actual_win_rate"),
    ).first()

    # Calculate derived metrics
    total = metrics["total_predictions"] or 1
    tp = metrics["true_positives"] or 0
    fp = metrics["false_positives"] or 0
    fn = metrics["false_negatives"] or 0

    accuracy = (metrics["correct_predictions"] or 0) / total
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

    # Create new performance record
    new_record = spark.createDataFrame([(
        now,
        total,
        accuracy,
        precision,
        recall,
        f1,
        metrics["avg_predicted_probability"],
        metrics["actual_win_rate"],
    )], [
        "evaluation_date",
        "sample_size",
        "accuracy",
        "precision",
        "recall",
        "f1_score",
        "avg_predicted_prob",
        "actual_win_rate",
    ])

    # Append to history
    if previous_performance.count() > 0:
        result = previous_performance.unionByName(new_record, allowMissingColumns=True)
    else:
        result = new_record

    # Keep last 90 days of performance data
    result = result.filter(
        F.col("evaluation_date") >= F.date_sub(F.lit(now), 90)
    )

    output.write_dataframe(result)
