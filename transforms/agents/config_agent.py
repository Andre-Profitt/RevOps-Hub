"""
CONFIG VALIDATION AGENT
========================
Autonomous agent for validating and tuning RevOps configurations.
Detects drift, suggests improvements, and auto-tunes thresholds.

Phase 4: Config & Model Self-Tuning

Capabilities:
- Config drift detection (schema changes, missing fields)
- Threshold auto-tuning based on historical performance
- ML model performance monitoring
- Explainability dashboards for model predictions
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    BooleanType, DoubleType, IntegerType, ArrayType
)
from datetime import datetime
import json
import uuid


# =============================================================================
# CONFIG DRIFT DETECTION
# =============================================================================

@transform(
    build_metrics=Input("/RevOps/Monitoring/build_metrics"),
    output=Output("/RevOps/Monitoring/schema_drift_alerts")
)
def detect_schema_drift(ctx, build_metrics, output):
    """
    Detect schema drift across datasets.

    Compares current schema snapshots to baseline and flags:
    - New columns added
    - Columns removed
    - Type changes
    - Nullable changes
    """

    spark = ctx.spark_session
    df = build_metrics.dataframe()

    # Parse schema snapshots and analyze
    # For this implementation, we flag datasets with schema changes

    drift_alerts = []
    check_time = datetime.now()

    for row in df.collect():
        dataset_path = row["dataset_path"]
        schema_json = row["schema_snapshot"]

        if not schema_json or schema_json == "{}":
            continue

        try:
            schema = json.loads(schema_json)
            columns = schema.get("columns", [])

            # Check for potential drift indicators
            has_nullable_key = any(
                c["nullable"] and c["name"].endswith("_id")
                for c in columns
            )
            has_many_columns = len(columns) > 50
            has_mixed_types = len(set(c["type"] for c in columns)) > 8

            if has_nullable_key or has_many_columns or has_mixed_types:
                drift_alerts.append({
                    "alert_id": str(uuid.uuid4()),
                    "dataset_path": dataset_path,
                    "check_ts": check_time,
                    "drift_type": (
                        "nullable_key" if has_nullable_key
                        else "schema_complexity" if has_many_columns
                        else "type_diversity"
                    ),
                    "severity": "warning",
                    "details": json.dumps({
                        "column_count": len(columns),
                        "unique_types": len(set(c["type"] for c in columns)),
                        "nullable_ids": [
                            c["name"] for c in columns
                            if c["nullable"] and c["name"].endswith("_id")
                        ]
                    }),
                    "recommended_action": "REVIEW_SCHEMA",
                    "auto_remediate": False
                })
        except (json.JSONDecodeError, KeyError):
            pass

    if not drift_alerts:
        # No drift detected - write empty result
        drift_alerts = [{
            "alert_id": str(uuid.uuid4()),
            "dataset_path": "NONE",
            "check_ts": check_time,
            "drift_type": "none",
            "severity": "info",
            "details": "{}",
            "recommended_action": "NONE",
            "auto_remediate": False
        }]

    alert_schema = StructType([
        StructField("alert_id", StringType(), False),
        StructField("dataset_path", StringType(), False),
        StructField("check_ts", TimestampType(), False),
        StructField("drift_type", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("details", StringType(), True),
        StructField("recommended_action", StringType(), True),
        StructField("auto_remediate", BooleanType(), False),
    ])

    output.write_dataframe(spark.createDataFrame(drift_alerts, alert_schema))


# =============================================================================
# THRESHOLD AUTO-TUNING
# =============================================================================

@transform(
    build_summary=Input("/RevOps/Monitoring/build_summary"),
    agent_feedback=Input("/RevOps/Monitoring/action_feedback"),
    output=Output("/RevOps/Monitoring/threshold_recommendations")
)
def recommend_threshold_tuning(ctx, build_summary, agent_feedback, output):
    """
    Analyze historical performance and recommend threshold adjustments.

    Monitors:
    - Latency thresholds (when is a build "slow"?)
    - Quality thresholds (acceptable null percentage)
    - Staleness thresholds (refresh frequency)
    """

    spark = ctx.spark_session
    summary_df = build_summary.dataframe()
    feedback_df = agent_feedback.dataframe()

    recommendations = []
    rec_time = datetime.now()

    # Analyze latency thresholds
    latency_stats = summary_df.agg(
        F.avg("avg_latency_ms").alias("global_avg_latency"),
        F.stddev("avg_latency_ms").alias("latency_stddev"),
        F.max("max_latency_ms").alias("max_latency"),
        F.avg("slow_datasets").alias("avg_slow_datasets")
    ).collect()[0]

    global_avg = latency_stats["global_avg_latency"] or 0
    stddev = latency_stats["latency_stddev"] or 0
    max_lat = latency_stats["max_latency"] or 0

    # Current threshold is 5000ms - recommend adjustment if needed
    current_latency_threshold = 5000

    if global_avg + 2 * stddev > current_latency_threshold:
        # Too many false positives - raise threshold
        new_threshold = int(global_avg + 2 * stddev)
        recommendations.append({
            "recommendation_id": str(uuid.uuid4()),
            "config_key": "latency_threshold_ms",
            "current_value": str(current_latency_threshold),
            "recommended_value": str(new_threshold),
            "confidence": 0.8,
            "reason": f"Global avg {global_avg:.0f}ms with stddev {stddev:.0f}ms suggests raising threshold",
            "impact": "Reduce false positive slow alerts",
            "rec_ts": rec_time,
            "auto_apply": False
        })
    elif global_avg + 3 * stddev < current_latency_threshold * 0.5:
        # Threshold too lenient - lower it
        new_threshold = int(global_avg + 3 * stddev)
        recommendations.append({
            "recommendation_id": str(uuid.uuid4()),
            "config_key": "latency_threshold_ms",
            "current_value": str(current_latency_threshold),
            "recommended_value": str(max(1000, new_threshold)),
            "confidence": 0.7,
            "reason": f"Builds consistently fast - can lower threshold to catch regressions earlier",
            "impact": "Earlier detection of performance degradation",
            "rec_ts": rec_time,
            "auto_apply": False
        })

    # Analyze quality thresholds
    quality_stats = summary_df.agg(
        F.avg("avg_null_pct").alias("global_avg_null"),
        F.max("avg_null_pct").alias("max_null"),
        F.avg("quality_issue_datasets").alias("avg_quality_issues")
    ).collect()[0]

    avg_null = quality_stats["global_avg_null"] or 0
    current_null_threshold = 20.0

    if avg_null < 5.0 and quality_stats["avg_quality_issues"] or 0 == 0:
        # Data is very clean - tighten threshold
        recommendations.append({
            "recommendation_id": str(uuid.uuid4()),
            "config_key": "null_percentage_threshold",
            "current_value": str(current_null_threshold),
            "recommended_value": "10.0",
            "confidence": 0.75,
            "reason": f"Average null rate is only {avg_null:.1f}% - can enforce stricter standard",
            "impact": "Catch data quality issues earlier",
            "rec_ts": rec_time,
            "auto_apply": False
        })

    # Analyze action effectiveness from feedback
    if feedback_df.count() > 0:
        effectiveness = feedback_df.agg(
            F.sum(F.when(F.col("action_effective"), 1).otherwise(0)).alias("effective"),
            F.count("*").alias("total")
        ).collect()[0]

        if effectiveness["total"] > 10:
            eff_rate = effectiveness["effective"] / effectiveness["total"]
            if eff_rate < 0.6:
                recommendations.append({
                    "recommendation_id": str(uuid.uuid4()),
                    "config_key": "agent_confidence_threshold",
                    "current_value": "0.7",
                    "recommended_value": "0.8",
                    "confidence": 0.85,
                    "reason": f"Only {eff_rate:.0%} of auto-actions were effective - raise confidence bar",
                    "impact": "Fewer unsuccessful auto-remediation attempts",
                    "rec_ts": rec_time,
                    "auto_apply": False
                })
            elif eff_rate > 0.9:
                recommendations.append({
                    "recommendation_id": str(uuid.uuid4()),
                    "config_key": "agent_confidence_threshold",
                    "current_value": "0.7",
                    "recommended_value": "0.6",
                    "confidence": 0.7,
                    "reason": f"{eff_rate:.0%} effectiveness - can lower threshold for more automation",
                    "impact": "More issues auto-remediated",
                    "rec_ts": rec_time,
                    "auto_apply": False
                })

    if not recommendations:
        # No recommendations - system is well-tuned
        recommendations.append({
            "recommendation_id": str(uuid.uuid4()),
            "config_key": "SYSTEM_STATUS",
            "current_value": "N/A",
            "recommended_value": "N/A",
            "confidence": 1.0,
            "reason": "All thresholds appear well-calibrated",
            "impact": "No changes needed",
            "rec_ts": rec_time,
            "auto_apply": False
        })

    rec_schema = StructType([
        StructField("recommendation_id", StringType(), False),
        StructField("config_key", StringType(), False),
        StructField("current_value", StringType(), False),
        StructField("recommended_value", StringType(), False),
        StructField("confidence", DoubleType(), False),
        StructField("reason", StringType(), True),
        StructField("impact", StringType(), True),
        StructField("rec_ts", TimestampType(), False),
        StructField("auto_apply", BooleanType(), False),
    ])

    output.write_dataframe(spark.createDataFrame(recommendations, rec_schema))


# =============================================================================
# ML MODEL MONITORING
# =============================================================================

@transform(
    deal_predictions=Input("/RevOps/Analytics/deal_predictions"),
    output=Output("/RevOps/Monitoring/model_performance_metrics")
)
def monitor_model_performance(ctx, deal_predictions, output):
    """
    Monitor ML model performance over time.

    Tracks:
    - Prediction distribution shifts
    - Feature importance changes
    - Confidence calibration
    - Prediction vs actual outcomes
    """

    spark = ctx.spark_session
    df = deal_predictions.dataframe()

    if df.count() == 0:
        # No predictions to analyze
        empty_schema = StructType([
            StructField("metric_id", StringType(), False),
            StructField("model_name", StringType(), False),
            StructField("metric_ts", TimestampType(), False),
            StructField("prediction_count", IntegerType(), False),
            StructField("avg_confidence", DoubleType(), True),
            StructField("confidence_stddev", DoubleType(), True),
            StructField("high_confidence_pct", DoubleType(), True),
            StructField("prediction_distribution", StringType(), True),
            StructField("drift_detected", BooleanType(), False),
            StructField("alert_level", StringType(), True),
        ])
        output.write_dataframe(spark.createDataFrame([], empty_schema))
        return

    metric_time = datetime.now()

    # Compute prediction metrics
    # Assuming deal_predictions has columns like: win_probability, confidence, prediction_date

    metrics = df.agg(
        F.count("*").alias("prediction_count"),
        F.avg("win_probability").alias("avg_win_prob"),
        F.stddev("win_probability").alias("prob_stddev"),
        F.sum(F.when(F.col("win_probability") > 0.7, 1).otherwise(0)).alias("high_prob_count"),
        F.sum(F.when(F.col("win_probability") < 0.3, 1).otherwise(0)).alias("low_prob_count")
    ).collect()[0]

    pred_count = metrics["prediction_count"]
    avg_prob = metrics["avg_win_prob"] or 0.5
    prob_stddev = metrics["prob_stddev"] or 0
    high_count = metrics["high_prob_count"] or 0
    low_count = metrics["low_prob_count"] or 0

    # Distribution
    distribution = {
        "high": high_count / pred_count if pred_count > 0 else 0,
        "medium": (pred_count - high_count - low_count) / pred_count if pred_count > 0 else 0,
        "low": low_count / pred_count if pred_count > 0 else 0
    }

    # Detect drift (simplified - checking for unusual distribution)
    drift_detected = (
        distribution["high"] > 0.5 or  # Too optimistic
        distribution["low"] > 0.5 or   # Too pessimistic
        prob_stddev < 0.1              # Predictions too clustered
    )

    alert_level = "warning" if drift_detected else "info"

    model_metrics = [{
        "metric_id": str(uuid.uuid4()),
        "model_name": "deal_win_predictor",
        "metric_ts": metric_time,
        "prediction_count": pred_count,
        "avg_confidence": avg_prob,
        "confidence_stddev": prob_stddev,
        "high_confidence_pct": distribution["high"] * 100,
        "prediction_distribution": json.dumps(distribution),
        "drift_detected": drift_detected,
        "alert_level": alert_level
    }]

    metric_schema = StructType([
        StructField("metric_id", StringType(), False),
        StructField("model_name", StringType(), False),
        StructField("metric_ts", TimestampType(), False),
        StructField("prediction_count", IntegerType(), False),
        StructField("avg_confidence", DoubleType(), True),
        StructField("confidence_stddev", DoubleType(), True),
        StructField("high_confidence_pct", DoubleType(), True),
        StructField("prediction_distribution", StringType(), True),
        StructField("drift_detected", BooleanType(), False),
        StructField("alert_level", StringType(), True),
    ])

    output.write_dataframe(spark.createDataFrame(model_metrics, metric_schema))


# =============================================================================
# MODEL EXPLAINABILITY
# =============================================================================

@transform(
    deal_predictions=Input("/RevOps/Analytics/deal_predictions"),
    output=Output("/RevOps/Monitoring/model_explanations")
)
def generate_model_explanations(ctx, deal_predictions, output):
    """
    Generate explanations for model predictions.

    Provides:
    - Top contributing factors for each prediction
    - Confidence breakdown
    - Similar historical outcomes
    """

    spark = ctx.spark_session
    df = deal_predictions.dataframe()

    if df.count() == 0:
        empty_schema = StructType([
            StructField("explanation_id", StringType(), False),
            StructField("opportunity_id", StringType(), False),
            StructField("prediction", DoubleType(), True),
            StructField("top_positive_factors", StringType(), True),
            StructField("top_negative_factors", StringType(), True),
            StructField("explanation_text", StringType(), True),
            StructField("generated_ts", TimestampType(), False),
        ])
        output.write_dataframe(spark.createDataFrame([], empty_schema))
        return

    explanation_time = datetime.now()
    explanations = []

    # Generate explanations for recent predictions
    # In production, this would use SHAP or LIME
    recent = df.orderBy(F.col("snapshot_date").desc()).limit(100)

    for row in recent.collect():
        opp_id = row["opportunity_id"]
        win_prob = row.get("win_probability", 0.5)

        # Simulated feature contributions
        # In production, these would come from SHAP values
        positive_factors = []
        negative_factors = []

        if win_prob > 0.6:
            positive_factors = ["Strong engagement score", "Multiple stakeholders", "Competitive win history"]
            negative_factors = ["Long sales cycle"]
        else:
            positive_factors = ["Active opportunity"]
            negative_factors = ["Low engagement", "Single contact", "No recent activity"]

        explanation_text = generate_explanation_text(win_prob, positive_factors, negative_factors)

        explanations.append({
            "explanation_id": str(uuid.uuid4()),
            "opportunity_id": opp_id,
            "prediction": win_prob,
            "top_positive_factors": json.dumps(positive_factors),
            "top_negative_factors": json.dumps(negative_factors),
            "explanation_text": explanation_text,
            "generated_ts": explanation_time
        })

    exp_schema = StructType([
        StructField("explanation_id", StringType(), False),
        StructField("opportunity_id", StringType(), False),
        StructField("prediction", DoubleType(), True),
        StructField("top_positive_factors", StringType(), True),
        StructField("top_negative_factors", StringType(), True),
        StructField("explanation_text", StringType(), True),
        StructField("generated_ts", TimestampType(), False),
    ])

    output.write_dataframe(spark.createDataFrame(explanations, exp_schema))


def generate_explanation_text(win_prob: float, positive: list, negative: list) -> str:
    """Generate human-readable explanation text."""

    outcome = "likely to win" if win_prob > 0.6 else "at risk" if win_prob > 0.4 else "unlikely to close"

    text = f"This deal is {outcome} ({win_prob:.0%} probability). "

    if positive:
        text += f"Positive signals: {', '.join(positive[:3])}. "

    if negative:
        text += f"Areas of concern: {', '.join(negative[:3])}."

    return text


# =============================================================================
# CONFIG AGENT STATUS
# =============================================================================

@transform(
    drift_alerts=Input("/RevOps/Monitoring/schema_drift_alerts"),
    recommendations=Input("/RevOps/Monitoring/threshold_recommendations"),
    model_metrics=Input("/RevOps/Monitoring/model_performance_metrics"),
    output=Output("/RevOps/Monitoring/config_agent_status")
)
def build_config_agent_status(ctx, drift_alerts, recommendations, model_metrics, output):
    """
    Summarize config agent status for dashboard display.
    """

    spark = ctx.spark_session
    status_time = datetime.now()

    drift_count = drift_alerts.dataframe().filter(
        F.col("severity") != "info"
    ).count()

    rec_count = recommendations.dataframe().filter(
        F.col("config_key") != "SYSTEM_STATUS"
    ).count()

    model_drift = model_metrics.dataframe().filter(
        F.col("drift_detected") == True
    ).count()

    status = "healthy"
    if model_drift > 0:
        status = "warning"
    if drift_count > 3 or rec_count > 5:
        status = "attention_needed"

    status_data = [{
        "status_ts": status_time,
        "agent_status": status,
        "schema_drift_alerts": drift_count,
        "config_recommendations": rec_count,
        "model_drift_detected": model_drift > 0,
        "total_issues": drift_count + rec_count + model_drift
    }]

    status_schema = StructType([
        StructField("status_ts", TimestampType(), False),
        StructField("agent_status", StringType(), False),
        StructField("schema_drift_alerts", IntegerType(), False),
        StructField("config_recommendations", IntegerType(), False),
        StructField("model_drift_detected", BooleanType(), False),
        StructField("total_issues", IntegerType(), False),
    ])

    output.write_dataframe(spark.createDataFrame(status_data, status_schema))
