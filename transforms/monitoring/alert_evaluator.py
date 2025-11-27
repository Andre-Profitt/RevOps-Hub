# /transforms/monitoring/alert_evaluator.py
"""
ALERT EVALUATOR TRANSFORM
=========================
Evaluates alert conditions and records triggered alerts.

Outputs:
- /RevOps/Monitoring/alert_history: Full history of triggered alerts
- /RevOps/Monitoring/active_alerts: Currently active (unresolved) alerts

This transform runs after analytics builds complete and:
1. Reads latest metrics from monitoring datasets
2. Evaluates alert rules per customer
3. Sends notifications via configured channels
4. Records alert history
"""

from transforms.api import transform, Input, Output
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DoubleType, BooleanType, MapType
)
from datetime import datetime, timedelta


# =============================================================================
# SCHEMAS
# =============================================================================

ALERT_HISTORY_SCHEMA = StructType([
    StructField("alert_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("alert_type", StringType(), False),
    StructField("severity", StringType(), False),
    StructField("title", StringType(), False),
    StructField("message", StringType(), True),
    StructField("metric_name", StringType(), True),
    StructField("metric_value", DoubleType(), True),
    StructField("threshold", DoubleType(), True),
    StructField("triggered_at", TimestampType(), False),
    StructField("resolved_at", TimestampType(), True),
    StructField("is_resolved", BooleanType(), False),
    StructField("notification_sent", BooleanType(), False),
    StructField("notification_channels", StringType(), True),  # JSON array
    StructField("context", StringType(), True),  # JSON object
])


# =============================================================================
# ALERT EVALUATION
# =============================================================================

@transform(
    build_metrics=Input("/RevOps/Monitoring/build_metrics"),
    pipeline_health=Input("/RevOps/Analytics/pipeline_health_summary"),
    data_quality=Input("/RevOps/DataQuality/summary"),
    sync_status=Input("/RevOps/DataQuality/sync_status"),
    customer_config=Input("/RevOps/Config/customer_settings"),
    previous_alerts=Input("/RevOps/Monitoring/alert_history"),
    output=Output("/RevOps/Monitoring/alert_history"),
)
def evaluate_alerts(
    ctx,
    build_metrics: DataFrame,
    pipeline_health: DataFrame,
    data_quality: DataFrame,
    sync_status: DataFrame,
    customer_config: DataFrame,
    previous_alerts: DataFrame,
    output,
):
    """
    Evaluate alert conditions and record triggered alerts.
    """
    spark = ctx.spark_session
    now = datetime.utcnow()

    # Load customer configs
    configs = customer_config.collect()
    customer_thresholds = {}
    for row in configs:
        customer_thresholds[row["customer_id"]] = {
            "stale_pct_threshold": row.get("stale_pct_threshold", 0.10),
            "data_quality_threshold": row.get("data_quality_threshold", 0.90),
            "sync_delay_threshold": row.get("sync_delay_threshold", 60),
            "critical_deal_threshold": row.get("critical_deal_threshold", 5),
        }

    # Get default thresholds if no customer-specific ones
    default_thresholds = {
        "stale_pct_threshold": 0.10,
        "data_quality_threshold": 0.90,
        "sync_delay_threshold": 60,
        "critical_deal_threshold": 5,
    }

    new_alerts = []

    # ----- Build Failure Alerts -----
    failed_builds = build_metrics.filter(
        (F.col("status") == "FAILED") &
        (F.col("build_timestamp") > F.lit(now - timedelta(hours=1)))
    )

    for row in failed_builds.collect():
        alert = {
            "alert_id": f"build-{row['build_id'][:8]}-{int(now.timestamp())}",
            "customer_id": row.get("customer_id", "default"),
            "alert_type": "build_failure",
            "severity": "critical",
            "title": "Build Failure",
            "message": f"Build failed for {row.get('dataset_path', 'unknown')}: {row.get('error_message', 'Unknown error')}",
            "metric_name": "build_status",
            "metric_value": None,
            "threshold": None,
            "triggered_at": now,
            "resolved_at": None,
            "is_resolved": False,
            "notification_sent": False,
            "notification_channels": "[]",
            "context": f'{{"build_id": "{row["build_id"]}", "dataset": "{row.get("dataset_path", "")}"}}',
        }
        new_alerts.append(alert)

    # ----- Pipeline Staleness Alerts -----
    stale_metrics = pipeline_health.groupBy("customer_id").agg(
        F.sum(F.when(F.col("days_since_activity") > 14, 1).otherwise(0)).alias("stale_count"),
        F.count("*").alias("total_count"),
        F.sum(F.when(F.col("health_status") == "Critical", F.col("amount")).otherwise(0)).alias("at_risk_amount"),
    )

    for row in stale_metrics.collect():
        customer_id = row["customer_id"] or "default"
        thresholds = customer_thresholds.get(customer_id, default_thresholds)

        total = row["total_count"] or 1
        stale_pct = (row["stale_count"] or 0) / total

        if stale_pct > thresholds["stale_pct_threshold"]:
            alert = {
                "alert_id": f"stale-{customer_id[:8]}-{int(now.timestamp())}",
                "customer_id": customer_id,
                "alert_type": "stale_pipeline",
                "severity": "warning",
                "title": "Stale Pipeline Alert",
                "message": f"Pipeline staleness at {stale_pct:.1%}, exceeds {thresholds['stale_pct_threshold']:.1%} threshold. {row['stale_count']} deals need attention.",
                "metric_name": "stale_deal_pct",
                "metric_value": stale_pct,
                "threshold": thresholds["stale_pct_threshold"],
                "triggered_at": now,
                "resolved_at": None,
                "is_resolved": False,
                "notification_sent": False,
                "notification_channels": "[]",
                "context": f'{{"stale_count": {row["stale_count"]}, "total_count": {row["total_count"]}}}',
            }
            new_alerts.append(alert)

    # ----- Data Quality Alerts -----
    quality_metrics = data_quality.groupBy("customer_id").agg(
        F.avg("overall_score").alias("avg_quality"),
    )

    for row in quality_metrics.collect():
        customer_id = row["customer_id"] or "default"
        thresholds = customer_thresholds.get(customer_id, default_thresholds)
        quality = row["avg_quality"] or 1.0

        if quality < thresholds["data_quality_threshold"]:
            alert = {
                "alert_id": f"quality-{customer_id[:8]}-{int(now.timestamp())}",
                "customer_id": customer_id,
                "alert_type": "data_quality_drop",
                "severity": "warning",
                "title": "Data Quality Alert",
                "message": f"Data quality at {quality:.1%}, below {thresholds['data_quality_threshold']:.1%} threshold.",
                "metric_name": "data_quality_score",
                "metric_value": quality,
                "threshold": thresholds["data_quality_threshold"],
                "triggered_at": now,
                "resolved_at": None,
                "is_resolved": False,
                "notification_sent": False,
                "notification_channels": "[]",
                "context": "{}",
            }
            new_alerts.append(alert)

    # ----- Sync Delay Alerts -----
    if sync_status.count() > 0:
        latest_sync = sync_status.groupBy("customer_id", "source_system").agg(
            F.max("last_sync_time").alias("last_sync"),
        )

        for row in latest_sync.collect():
            if row["last_sync"] is None:
                continue

            customer_id = row["customer_id"] or "default"
            thresholds = customer_thresholds.get(customer_id, default_thresholds)

            delay_minutes = (now - row["last_sync"]).total_seconds() / 60

            if delay_minutes > thresholds["sync_delay_threshold"]:
                alert = {
                    "alert_id": f"sync-{customer_id[:8]}-{row['source_system'][:4]}-{int(now.timestamp())}",
                    "customer_id": customer_id,
                    "alert_type": "sync_delay",
                    "severity": "warning",
                    "title": "CRM Sync Delay",
                    "message": f"{row['source_system']} sync delayed by {delay_minutes:.0f} minutes (threshold: {thresholds['sync_delay_threshold']}min).",
                    "metric_name": "sync_delay_minutes",
                    "metric_value": delay_minutes,
                    "threshold": float(thresholds["sync_delay_threshold"]),
                    "triggered_at": now,
                    "resolved_at": None,
                    "is_resolved": False,
                    "notification_sent": False,
                    "notification_channels": "[]",
                    "context": f'{{"source_system": "{row["source_system"]}"}}',
                }
                new_alerts.append(alert)

    # Create DataFrame from new alerts
    if new_alerts:
        new_alerts_df = spark.createDataFrame(new_alerts, ALERT_HISTORY_SCHEMA)
    else:
        new_alerts_df = spark.createDataFrame([], ALERT_HISTORY_SCHEMA)

    # Apply cooldown - don't create duplicate alerts within 1 hour
    if previous_alerts.count() > 0:
        recent_alerts = previous_alerts.filter(
            F.col("triggered_at") > F.lit(now - timedelta(hours=1))
        ).select("alert_type", "customer_id").distinct()

        # Anti-join to remove duplicates
        new_alerts_df = new_alerts_df.join(
            recent_alerts,
            ["alert_type", "customer_id"],
            "left_anti"
        )

    # Combine with previous alerts
    result = previous_alerts.unionByName(new_alerts_df, allowMissingColumns=True)

    # Auto-resolve old alerts (> 24 hours without recurrence)
    result = result.withColumn(
        "is_resolved",
        F.when(
            (F.col("is_resolved") == False) &
            (F.col("triggered_at") < F.lit(now - timedelta(hours=24))),
            True
        ).otherwise(F.col("is_resolved"))
    ).withColumn(
        "resolved_at",
        F.when(
            (F.col("is_resolved") == True) & F.col("resolved_at").isNull(),
            F.lit(now)
        ).otherwise(F.col("resolved_at"))
    )

    output.write_dataframe(result)


# =============================================================================
# ACTIVE ALERTS VIEW
# =============================================================================

@transform(
    alert_history=Input("/RevOps/Monitoring/alert_history"),
    output=Output("/RevOps/Monitoring/active_alerts"),
)
def compute_active_alerts(ctx, alert_history: DataFrame, output):
    """
    Filter to currently active (unresolved) alerts.
    """
    active = alert_history.filter(F.col("is_resolved") == False)

    # Add age and urgency
    now = datetime.utcnow()
    result = active.withColumn(
        "age_hours",
        (F.lit(now).cast("long") - F.col("triggered_at").cast("long")) / 3600
    ).withColumn(
        "urgency",
        F.when(F.col("severity") == "critical", 1)
        .when(F.col("age_hours") > 12, 2)
        .when(F.col("severity") == "warning", 3)
        .otherwise(4)
    ).orderBy("urgency", "triggered_at")

    output.write_dataframe(result)


# =============================================================================
# ALERT SUMMARY
# =============================================================================

@transform(
    alert_history=Input("/RevOps/Monitoring/alert_history"),
    output=Output("/RevOps/Monitoring/alert_summary"),
)
def compute_alert_summary(ctx, alert_history: DataFrame, output):
    """
    Compute alert summary statistics.
    """
    now = datetime.utcnow()

    # Time windows
    last_24h = now - timedelta(hours=24)
    last_7d = now - timedelta(days=7)
    last_30d = now - timedelta(days=30)

    summary = alert_history.groupBy("customer_id").agg(
        # Active alerts
        F.sum(F.when(F.col("is_resolved") == False, 1).otherwise(0)).alias("active_count"),
        F.sum(F.when(
            (F.col("is_resolved") == False) & (F.col("severity") == "critical"), 1
        ).otherwise(0)).alias("critical_active"),

        # Last 24 hours
        F.sum(F.when(F.col("triggered_at") > F.lit(last_24h), 1).otherwise(0)).alias("alerts_24h"),

        # Last 7 days
        F.sum(F.when(F.col("triggered_at") > F.lit(last_7d), 1).otherwise(0)).alias("alerts_7d"),

        # Last 30 days
        F.sum(F.when(F.col("triggered_at") > F.lit(last_30d), 1).otherwise(0)).alias("alerts_30d"),

        # By type (last 7d)
        F.sum(F.when(
            (F.col("alert_type") == "build_failure") & (F.col("triggered_at") > F.lit(last_7d)), 1
        ).otherwise(0)).alias("build_failures_7d"),
        F.sum(F.when(
            (F.col("alert_type") == "stale_pipeline") & (F.col("triggered_at") > F.lit(last_7d)), 1
        ).otherwise(0)).alias("stale_alerts_7d"),
        F.sum(F.when(
            (F.col("alert_type") == "data_quality_drop") & (F.col("triggered_at") > F.lit(last_7d)), 1
        ).otherwise(0)).alias("quality_alerts_7d"),
        F.sum(F.when(
            (F.col("alert_type") == "sync_delay") & (F.col("triggered_at") > F.lit(last_7d)), 1
        ).otherwise(0)).alias("sync_alerts_7d"),

        # Resolution stats
        F.avg(F.when(
            F.col("is_resolved") == True,
            (F.col("resolved_at").cast("long") - F.col("triggered_at").cast("long")) / 3600
        )).alias("avg_resolution_hours"),
    )

    # Add snapshot timestamp
    result = summary.withColumn("snapshot_time", F.lit(now))

    output.write_dataframe(result)
