# /transforms/commercial/tenant_health_metrics.py
"""
TENANT HEALTH METRICS
=====================
Computes health scores for customer success monitoring.

Health dimensions:
- Data Freshness: How current is the synced data
- Adoption: Feature usage and active users
- AI Accuracy: Agent recommendation acceptance rate
- Build Health: Pipeline reliability

Outputs:
- /RevOps/Commercial/tenant_health_scores: Current health per tenant
- /RevOps/Commercial/tenant_health_trends: Weekly health trends
"""

from transforms.api import transform, Input, Output
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from datetime import datetime, timedelta


# =============================================================================
# HEALTH SCORING WEIGHTS
# =============================================================================

HEALTH_WEIGHTS = {
    "data_freshness": 0.25,
    "adoption": 0.30,
    "ai_accuracy": 0.25,
    "build_health": 0.20,
}

# Thresholds for health categories
HEALTH_THRESHOLDS = {
    "critical": 40,
    "at_risk": 60,
    "monitor": 80,
    "healthy": 80,
}


# =============================================================================
# TENANT HEALTH SCORES
# =============================================================================

@transform(
    customer_config=Input("/RevOps/Config/customer_settings"),
    sync_status=Input("/RevOps/Monitoring/sync_status"),
    feature_usage=Input("/RevOps/Telemetry/feature_usage"),
    agent_feedback=Input("/RevOps/Agents/action_feedback"),
    build_metrics=Input("/RevOps/Monitoring/build_metrics"),
    usage_daily=Input("/RevOps/Commercial/usage_daily"),
    output=Output("/RevOps/Commercial/tenant_health_scores"),
)
def compute_tenant_health_scores(
    ctx,
    customer_config: DataFrame,
    sync_status: DataFrame,
    feature_usage: DataFrame,
    agent_feedback: DataFrame,
    build_metrics: DataFrame,
    usage_daily: DataFrame,
    output,
):
    """
    Compute composite health scores per tenant.
    """
    spark = ctx.spark_session
    now = datetime.utcnow()
    week_ago = now - timedelta(days=7)

    # Get customer list
    customers = customer_config.select(
        "customer_id",
        "customer_name",
        "tier",
    )

    # ----- DATA FRESHNESS SCORE -----
    if sync_status.count() > 0:
        freshness = sync_status.filter(
            F.col("sync_type") == "crm"
        ).groupBy("customer_id").agg(
            F.max("last_sync_at").alias("last_sync_at"),
        ).withColumn(
            "hours_since_sync",
            (F.unix_timestamp(F.lit(now)) - F.unix_timestamp(F.col("last_sync_at"))) / 3600
        ).withColumn(
            "data_freshness_score",
            F.when(F.col("hours_since_sync") <= 1, 100.0)
            .when(F.col("hours_since_sync") <= 4, 90.0)
            .when(F.col("hours_since_sync") <= 12, 75.0)
            .when(F.col("hours_since_sync") <= 24, 60.0)
            .when(F.col("hours_since_sync") <= 48, 40.0)
            .otherwise(20.0)
        )
    else:
        freshness = spark.createDataFrame([], StructType([
            StructField("customer_id", StringType(), True),
            StructField("last_sync_at", TimestampType(), True),
            StructField("hours_since_sync", DoubleType(), True),
            StructField("data_freshness_score", DoubleType(), True),
        ]))

    # ----- ADOPTION SCORE -----
    if usage_daily.count() > 0:
        # Recent usage (last 7 days)
        recent_usage = usage_daily.filter(
            F.col("usage_date") >= F.lit(week_ago.date())
        ).groupBy("customer_id").agg(
            F.avg("active_users_today").alias("avg_daily_users"),
            F.countDistinct("usage_date").alias("active_days"),
            F.sum("feature_events_today").alias("total_feature_events"),
        )

        # Join with tier limits for percentage
        adoption = recent_usage.join(
            customer_config.select("customer_id", "max_users"),
            "customer_id",
            "left"
        ).withColumn(
            "user_engagement_pct",
            F.col("avg_daily_users") / F.col("max_users") * 100
        ).withColumn(
            "adoption_score",
            F.when(F.col("active_days") >= 6, 100.0)  # 6+ days active
            .when(F.col("active_days") >= 4, 80.0)   # 4-5 days
            .when(F.col("active_days") >= 2, 60.0)   # 2-3 days
            .when(F.col("active_days") >= 1, 40.0)   # 1 day
            .otherwise(20.0)                          # No activity
            * (0.5 + 0.5 * F.least(F.col("user_engagement_pct") / 50, F.lit(1.0)))
        )
    else:
        adoption = spark.createDataFrame([], StructType([
            StructField("customer_id", StringType(), True),
            StructField("avg_daily_users", DoubleType(), True),
            StructField("active_days", IntegerType(), True),
            StructField("total_feature_events", IntegerType(), True),
            StructField("adoption_score", DoubleType(), True),
        ]))

    # ----- AI ACCURACY SCORE -----
    if agent_feedback.count() > 0:
        ai_metrics = agent_feedback.filter(
            F.col("feedback_timestamp") >= F.lit(week_ago)
        ).groupBy("customer_id").agg(
            F.count("*").alias("total_recommendations"),
            F.sum(F.when(F.col("feedback") == "accepted", 1).otherwise(0)).alias("accepted_count"),
            F.sum(F.when(F.col("feedback") == "rejected", 1).otherwise(0)).alias("rejected_count"),
        ).withColumn(
            "acceptance_rate",
            F.when(F.col("total_recommendations") > 0,
                   F.col("accepted_count") / F.col("total_recommendations"))
            .otherwise(0.5)  # Default 50% if no data
        ).withColumn(
            "ai_accuracy_score",
            F.col("acceptance_rate") * 100
        )
    else:
        ai_metrics = spark.createDataFrame([], StructType([
            StructField("customer_id", StringType(), True),
            StructField("total_recommendations", IntegerType(), True),
            StructField("acceptance_rate", DoubleType(), True),
            StructField("ai_accuracy_score", DoubleType(), True),
        ]))

    # ----- BUILD HEALTH SCORE -----
    if build_metrics.count() > 0:
        build_health = build_metrics.filter(
            F.col("build_timestamp") >= F.lit(week_ago)
        ).groupBy("customer_id").agg(
            F.count("*").alias("total_builds"),
            F.sum(F.when(F.col("status") == "success", 1).otherwise(0)).alias("successful_builds"),
            F.avg("duration_seconds").alias("avg_build_duration"),
        ).withColumn(
            "build_success_rate",
            F.when(F.col("total_builds") > 0,
                   F.col("successful_builds") / F.col("total_builds"))
            .otherwise(1.0)
        ).withColumn(
            "build_health_score",
            F.col("build_success_rate") * 100
        )
    else:
        build_health = spark.createDataFrame([], StructType([
            StructField("customer_id", StringType(), True),
            StructField("total_builds", IntegerType(), True),
            StructField("build_success_rate", DoubleType(), True),
            StructField("build_health_score", DoubleType(), True),
        ]))

    # ----- COMBINE ALL SCORES -----
    health = customers \
        .join(freshness.select("customer_id", "data_freshness_score", "hours_since_sync"), "customer_id", "left") \
        .join(adoption.select("customer_id", "adoption_score", "active_days", "avg_daily_users"), "customer_id", "left") \
        .join(ai_metrics.select("customer_id", "ai_accuracy_score", "acceptance_rate"), "customer_id", "left") \
        .join(build_health.select("customer_id", "build_health_score", "build_success_rate"), "customer_id", "left")

    # Fill nulls with neutral scores
    health = health.fillna({
        "data_freshness_score": 50.0,
        "adoption_score": 50.0,
        "ai_accuracy_score": 50.0,
        "build_health_score": 100.0,
        "hours_since_sync": 24.0,
        "active_days": 0,
        "avg_daily_users": 0.0,
        "acceptance_rate": 0.5,
        "build_success_rate": 1.0,
    })

    # Calculate composite score
    health = health.withColumn(
        "composite_score",
        F.col("data_freshness_score") * HEALTH_WEIGHTS["data_freshness"] +
        F.col("adoption_score") * HEALTH_WEIGHTS["adoption"] +
        F.col("ai_accuracy_score") * HEALTH_WEIGHTS["ai_accuracy"] +
        F.col("build_health_score") * HEALTH_WEIGHTS["build_health"]
    )

    # Determine health status
    health = health.withColumn(
        "health_status",
        F.when(F.col("composite_score") < HEALTH_THRESHOLDS["critical"], "critical")
        .when(F.col("composite_score") < HEALTH_THRESHOLDS["at_risk"], "at_risk")
        .when(F.col("composite_score") < HEALTH_THRESHOLDS["monitor"], "monitor")
        .otherwise("healthy")
    )

    # Add CS recommendations
    health = health.withColumn(
        "cs_recommendation",
        F.when(F.col("health_status") == "critical",
               "Immediate intervention required. Schedule call within 24 hours.")
        .when(F.col("health_status") == "at_risk",
               "Proactive outreach recommended. Check adoption blockers.")
        .when(F.col("health_status") == "monitor",
               "Monitor weekly. Consider training refresh.")
        .otherwise("Healthy engagement. Quarterly check-in sufficient.")
    )

    # Add timestamp
    health = health.withColumn("scored_at", F.lit(now))

    output.write_dataframe(health)


# =============================================================================
# HEALTH TRENDS
# =============================================================================

@transform(
    current_health=Input("/RevOps/Commercial/tenant_health_scores"),
    previous_trends=Input("/RevOps/Commercial/tenant_health_trends"),
    output=Output("/RevOps/Commercial/tenant_health_trends"),
)
def compute_health_trends(ctx, current_health: DataFrame, previous_trends: DataFrame, output):
    """
    Track weekly health trends for trend analysis.
    """
    spark = ctx.spark_session
    now = datetime.utcnow()
    week_num = now.isocalendar()[1]
    year = now.year

    # Create weekly snapshot
    snapshot = current_health.select(
        "customer_id",
        "composite_score",
        "data_freshness_score",
        "adoption_score",
        "ai_accuracy_score",
        "build_health_score",
        "health_status",
    ).withColumn(
        "week_number", F.lit(week_num)
    ).withColumn(
        "year", F.lit(year)
    ).withColumn(
        "snapshot_date", F.lit(now.date())
    )

    # Append to history
    if previous_trends.count() > 0:
        # Dedup by customer_id + week + year
        result = previous_trends.unionByName(snapshot, allowMissingColumns=True)
        result = result.dropDuplicates(["customer_id", "year", "week_number"])
    else:
        result = snapshot

    # Calculate week-over-week change
    window = Window.partitionBy("customer_id").orderBy("year", "week_number")
    result = result.withColumn(
        "prev_score",
        F.lag("composite_score").over(window)
    ).withColumn(
        "score_change",
        F.col("composite_score") - F.coalesce(F.col("prev_score"), F.col("composite_score"))
    ).withColumn(
        "trend_direction",
        F.when(F.col("score_change") > 5, "improving")
        .when(F.col("score_change") < -5, "declining")
        .otherwise("stable")
    )

    output.write_dataframe(result)
