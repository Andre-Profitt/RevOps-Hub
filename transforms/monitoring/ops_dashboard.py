# /transforms/monitoring/ops_dashboard.py
"""
OPS DASHBOARD TRANSFORMS
========================
Provides operational visibility into system health, build performance,
and data freshness.

Outputs:
- /RevOps/Ops/system_health: Overall system health status
- /RevOps/Ops/build_performance: Build latency and success rates
- /RevOps/Ops/data_freshness: Data staleness tracking
- /RevOps/Ops/usage_metrics: Platform usage statistics
"""

from transforms.api import transform, Input, Output
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DoubleType, IntegerType, BooleanType
)
from datetime import datetime, timedelta


# =============================================================================
# SYSTEM HEALTH
# =============================================================================

@transform(
    build_metrics=Input("/RevOps/Monitoring/build_metrics"),
    sync_status=Input("/RevOps/DataQuality/sync_status"),
    data_quality=Input("/RevOps/DataQuality/summary"),
    active_alerts=Input("/RevOps/Monitoring/active_alerts"),
    output=Output("/RevOps/Ops/system_health"),
)
def compute_system_health(
    ctx,
    build_metrics: DataFrame,
    sync_status: DataFrame,
    data_quality: DataFrame,
    active_alerts: DataFrame,
    output,
):
    """
    Compute overall system health status per customer.
    """
    spark = ctx.spark_session
    now = datetime.utcnow()
    last_24h = now - timedelta(hours=24)

    # Build health (last 24h)
    build_health = build_metrics.filter(
        F.col("build_timestamp") > F.lit(last_24h)
    ).groupBy("customer_id").agg(
        F.count("*").alias("total_builds"),
        F.sum(F.when(F.col("status") == "SUCCEEDED", 1).otherwise(0)).alias("successful_builds"),
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_builds"),
        F.avg("duration_seconds").alias("avg_build_duration"),
        F.max("build_timestamp").alias("last_build_time"),
    ).withColumn(
        "build_success_rate",
        F.col("successful_builds") / F.col("total_builds")
    )

    # Sync health
    sync_health = sync_status.groupBy("customer_id").agg(
        F.max("last_sync_time").alias("last_sync_time"),
        F.avg("records_synced").alias("avg_records_synced"),
        F.sum(F.when(F.col("sync_status") == "success", 1).otherwise(0)).alias("successful_syncs"),
        F.count("*").alias("total_syncs"),
    ).withColumn(
        "sync_success_rate",
        F.col("successful_syncs") / F.col("total_syncs")
    ).withColumn(
        "sync_age_hours",
        (F.lit(now).cast("long") - F.col("last_sync_time").cast("long")) / 3600
    )

    # Data quality health
    quality_health = data_quality.groupBy("customer_id").agg(
        F.avg("overall_score").alias("avg_data_quality"),
        F.min("overall_score").alias("min_data_quality"),
    )

    # Alert health
    alert_health = active_alerts.groupBy("customer_id").agg(
        F.count("*").alias("active_alert_count"),
        F.sum(F.when(F.col("severity") == "critical", 1).otherwise(0)).alias("critical_alerts"),
        F.sum(F.when(F.col("severity") == "warning", 1).otherwise(0)).alias("warning_alerts"),
    )

    # Join all health metrics
    health = build_health.join(sync_health, "customer_id", "outer") \
        .join(quality_health, "customer_id", "outer") \
        .join(alert_health, "customer_id", "outer")

    # Fill nulls
    health = health.fillna({
        "build_success_rate": 1.0,
        "sync_success_rate": 1.0,
        "avg_data_quality": 1.0,
        "active_alert_count": 0,
        "critical_alerts": 0,
        "warning_alerts": 0,
        "sync_age_hours": 0,
    })

    # Calculate overall health score (0-100)
    health = health.withColumn(
        "health_score",
        (
            F.col("build_success_rate") * 25 +
            F.col("sync_success_rate") * 25 +
            F.col("avg_data_quality") * 25 +
            F.greatest(F.lit(0), F.lit(25) - F.col("critical_alerts") * 10 - F.col("warning_alerts") * 2)
        )
    )

    # Determine health status
    health = health.withColumn(
        "health_status",
        F.when(F.col("critical_alerts") > 0, "Critical")
        .when(F.col("health_score") < 60, "Degraded")
        .when(F.col("health_score") < 80, "Warning")
        .otherwise("Healthy")
    )

    # Add timestamp
    result = health.withColumn("snapshot_time", F.lit(now))

    output.write_dataframe(result)


# =============================================================================
# BUILD PERFORMANCE
# =============================================================================

@transform(
    build_metrics=Input("/RevOps/Monitoring/build_metrics"),
    output=Output("/RevOps/Ops/build_performance"),
)
def compute_build_performance(ctx, build_metrics: DataFrame, output):
    """
    Compute build performance metrics over various time windows.
    """
    now = datetime.utcnow()

    # Time windows
    windows = {
        "1h": timedelta(hours=1),
        "24h": timedelta(hours=24),
        "7d": timedelta(days=7),
        "30d": timedelta(days=30),
    }

    # Calculate metrics per window
    metrics_list = []

    for window_name, delta in windows.items():
        cutoff = now - delta

        window_metrics = build_metrics.filter(
            F.col("build_timestamp") > F.lit(cutoff)
        ).groupBy("customer_id", "dataset_path").agg(
            F.count("*").alias("build_count"),
            F.sum(F.when(F.col("status") == "SUCCEEDED", 1).otherwise(0)).alias("success_count"),
            F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failure_count"),
            F.avg("duration_seconds").alias("avg_duration_seconds"),
            F.min("duration_seconds").alias("min_duration_seconds"),
            F.max("duration_seconds").alias("max_duration_seconds"),
            F.percentile_approx("duration_seconds", 0.5).alias("p50_duration"),
            F.percentile_approx("duration_seconds", 0.95).alias("p95_duration"),
            F.percentile_approx("duration_seconds", 0.99).alias("p99_duration"),
        ).withColumn("time_window", F.lit(window_name))

        metrics_list.append(window_metrics)

    # Union all windows
    result = metrics_list[0]
    for df in metrics_list[1:]:
        result = result.unionByName(df, allowMissingColumns=True)

    # Calculate derived metrics
    result = result.withColumn(
        "success_rate",
        F.col("success_count") / F.col("build_count")
    ).withColumn(
        "failure_rate",
        F.col("failure_count") / F.col("build_count")
    )

    # Add timestamp
    result = result.withColumn("snapshot_time", F.lit(now))

    output.write_dataframe(result)


# =============================================================================
# DATA FRESHNESS
# =============================================================================

@transform(
    build_metrics=Input("/RevOps/Monitoring/build_metrics"),
    sync_status=Input("/RevOps/DataQuality/sync_status"),
    output=Output("/RevOps/Ops/data_freshness"),
)
def compute_data_freshness(ctx, build_metrics: DataFrame, sync_status: DataFrame, output):
    """
    Track data freshness for each dataset.
    """
    now = datetime.utcnow()

    # Get latest successful build per dataset
    latest_builds = build_metrics.filter(
        F.col("status") == "SUCCEEDED"
    ).groupBy("customer_id", "dataset_path").agg(
        F.max("build_timestamp").alias("last_successful_build"),
        F.count("*").alias("total_builds"),
    )

    # Calculate staleness
    freshness = latest_builds.withColumn(
        "hours_since_build",
        (F.lit(now).cast("long") - F.col("last_successful_build").cast("long")) / 3600
    ).withColumn(
        "days_since_build",
        F.col("hours_since_build") / 24
    )

    # Determine freshness status
    freshness = freshness.withColumn(
        "freshness_status",
        F.when(F.col("hours_since_build") <= 1, "Fresh")
        .when(F.col("hours_since_build") <= 6, "Recent")
        .when(F.col("hours_since_build") <= 24, "Stale")
        .otherwise("Critical")
    )

    # Add expected freshness based on dataset type
    freshness = freshness.withColumn(
        "expected_freshness_hours",
        F.when(F.col("dataset_path").contains("Staging"), 1)
        .when(F.col("dataset_path").contains("Enriched"), 4)
        .when(F.col("dataset_path").contains("Analytics"), 12)
        .when(F.col("dataset_path").contains("Dashboard"), 24)
        .otherwise(24)
    ).withColumn(
        "is_overdue",
        F.col("hours_since_build") > F.col("expected_freshness_hours")
    )

    # Join with sync status for source data freshness
    if sync_status.count() > 0:
        sync_freshness = sync_status.groupBy("customer_id", "source_system").agg(
            F.max("last_sync_time").alias("last_sync_time"),
            F.sum("records_synced").alias("total_records_synced"),
        ).withColumn(
            "hours_since_sync",
            (F.lit(now).cast("long") - F.col("last_sync_time").cast("long")) / 3600
        )

        # Add sync data
        freshness = freshness.join(
            sync_freshness.select("customer_id", "hours_since_sync", "last_sync_time"),
            "customer_id",
            "left"
        )

    # Add timestamp
    result = freshness.withColumn("snapshot_time", F.lit(now))

    output.write_dataframe(result)


# =============================================================================
# USAGE METRICS
# =============================================================================

@transform(
    build_metrics=Input("/RevOps/Monitoring/build_metrics"),
    customer_config=Input("/RevOps/Config/customer_settings"),
    output=Output("/RevOps/Ops/usage_metrics"),
)
def compute_usage_metrics(ctx, build_metrics: DataFrame, customer_config: DataFrame, output):
    """
    Compute platform usage statistics per customer.
    """
    now = datetime.utcnow()
    last_30d = now - timedelta(days=30)

    # Build activity
    build_usage = build_metrics.filter(
        F.col("build_timestamp") > F.lit(last_30d)
    ).groupBy("customer_id").agg(
        F.count("*").alias("builds_30d"),
        F.countDistinct("dataset_path").alias("datasets_built"),
        F.sum("duration_seconds").alias("total_compute_seconds"),
        F.countDistinct(F.date_trunc("day", F.col("build_timestamp"))).alias("active_days"),
    )

    # Calculate daily averages
    build_usage = build_usage.withColumn(
        "avg_builds_per_day",
        F.col("builds_30d") / 30
    ).withColumn(
        "avg_compute_minutes_per_day",
        (F.col("total_compute_seconds") / 30) / 60
    )

    # Join with customer tier info
    if customer_config.count() > 0:
        tier_info = customer_config.select(
            "customer_id",
            F.col("tier").alias("subscription_tier"),
            F.col("max_users").alias("user_limit"),
            F.col("max_opportunities").alias("opportunity_limit"),
        )
        build_usage = build_usage.join(tier_info, "customer_id", "left")

    # Calculate utilization scores
    build_usage = build_usage.withColumn(
        "activity_score",
        F.least(F.lit(100), F.col("active_days") / 30 * 100)
    )

    # Add timestamp
    result = build_usage.withColumn("snapshot_time", F.lit(now))

    output.write_dataframe(result)


# =============================================================================
# SLA TRACKING
# =============================================================================

@transform(
    build_metrics=Input("/RevOps/Monitoring/build_metrics"),
    data_freshness=Input("/RevOps/Ops/data_freshness"),
    output=Output("/RevOps/Ops/sla_tracking"),
)
def compute_sla_tracking(ctx, build_metrics: DataFrame, data_freshness: DataFrame, output):
    """
    Track SLA compliance for data freshness and build reliability.
    """
    now = datetime.utcnow()
    last_30d = now - timedelta(days=30)

    # Define SLAs
    # - Staging data: updated within 1 hour (99% SLA)
    # - Analytics data: updated within 24 hours (99.5% SLA)
    # - Build success rate: 99%

    # Build SLA
    build_sla = build_metrics.filter(
        F.col("build_timestamp") > F.lit(last_30d)
    ).groupBy("customer_id").agg(
        F.count("*").alias("total_builds"),
        F.sum(F.when(F.col("status") == "SUCCEEDED", 1).otherwise(0)).alias("successful_builds"),
    ).withColumn(
        "build_success_rate",
        F.col("successful_builds") / F.col("total_builds")
    ).withColumn(
        "build_sla_target", F.lit(0.99)
    ).withColumn(
        "build_sla_met",
        F.col("build_success_rate") >= F.col("build_sla_target")
    )

    # Freshness SLA
    freshness_sla = data_freshness.groupBy("customer_id").agg(
        F.count("*").alias("total_datasets"),
        F.sum(F.when(F.col("is_overdue") == False, 1).otherwise(0)).alias("fresh_datasets"),
        F.sum(F.when(F.col("is_overdue") == True, 1).otherwise(0)).alias("overdue_datasets"),
    ).withColumn(
        "freshness_rate",
        F.col("fresh_datasets") / F.col("total_datasets")
    ).withColumn(
        "freshness_sla_target", F.lit(0.95)
    ).withColumn(
        "freshness_sla_met",
        F.col("freshness_rate") >= F.col("freshness_sla_target")
    )

    # Combine SLAs
    sla = build_sla.join(freshness_sla, "customer_id", "outer")

    # Overall SLA status
    sla = sla.withColumn(
        "overall_sla_met",
        (F.coalesce(F.col("build_sla_met"), F.lit(True))) &
        (F.coalesce(F.col("freshness_sla_met"), F.lit(True)))
    ).withColumn(
        "sla_status",
        F.when(F.col("overall_sla_met") == True, "Compliant")
        .otherwise("Breach")
    )

    # Add timestamp
    result = sla.withColumn("snapshot_time", F.lit(now))

    output.write_dataframe(result)
