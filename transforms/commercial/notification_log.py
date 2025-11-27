# /transforms/commercial/notification_log.py
"""
NOTIFICATION LOG
================
Tracks all notifications sent across channels.

Channels:
- Email (transactional)
- Slack (webhooks)
- In-app (toast/banner)
- PagerDuty (critical alerts)

Outputs:
- /RevOps/Commercial/notification_log: Full notification history
- /RevOps/Commercial/notification_summary: Daily aggregates
"""

from transforms.api import transform, Input, Output
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    TimestampType, BooleanType
)
from datetime import datetime, timedelta


# =============================================================================
# NOTIFICATION TYPES
# =============================================================================

NOTIFICATION_TYPES = [
    # System
    "build_complete",
    "build_failed",
    "sync_complete",
    "sync_failed",
    # Alerts
    "alert_triggered",
    "escalation_created",
    "escalation_resolved",
    # Commercial
    "usage_warning",
    "usage_limit_reached",
    "license_expiring",
    "upgrade_available",
    # Implementation
    "stage_completed",
    "go_live_approved",
    "cs_handoff",
    # Agent
    "agent_action_pending",
    "agent_action_executed",
    "agent_recommendation",
]

NOTIFICATION_CHANNELS = ["email", "slack", "in_app", "pagerduty", "webhook"]


# =============================================================================
# NOTIFICATION LOG
# =============================================================================

@transform(
    raw_notifications=Input("/RevOps/Audit/notification_events"),
    customer_config=Input("/RevOps/Config/customer_settings"),
    output=Output("/RevOps/Commercial/notification_log"),
)
def track_notifications(ctx, raw_notifications: DataFrame, customer_config: DataFrame, output):
    """
    Process and enrich notification events.
    """
    spark = ctx.spark_session
    now = datetime.utcnow()

    notification_schema = StructType([
        StructField("notification_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("notification_type", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("recipient", StringType(), True),
        StructField("subject", StringType(), True),
        StructField("body_preview", StringType(), True),
        StructField("status", StringType(), True),
        StructField("sent_at", TimestampType(), True),
        StructField("delivered_at", TimestampType(), True),
        StructField("read_at", TimestampType(), True),
        StructField("clicked_at", TimestampType(), True),
        StructField("error_message", StringType(), True),
        StructField("metadata", StringType(), True),
    ])

    if raw_notifications.count() > 0:
        # Enrich with customer info
        notifications = raw_notifications.join(
            customer_config.select("customer_id", "customer_name", "tier"),
            "customer_id",
            "left"
        )

        # Determine delivery status
        notifications = notifications.withColumn(
            "status",
            F.when(F.col("error_message").isNotNull(), "failed")
            .when(F.col("read_at").isNotNull(), "read")
            .when(F.col("delivered_at").isNotNull(), "delivered")
            .when(F.col("sent_at").isNotNull(), "sent")
            .otherwise("pending")
        )

        # Truncate body for preview
        notifications = notifications.withColumn(
            "body_preview",
            F.when(
                F.length(F.col("body")) > 200,
                F.concat(F.substring(F.col("body"), 1, 197), F.lit("..."))
            ).otherwise(F.col("body"))
        )

        result = notifications.select(
            "notification_id",
            "customer_id",
            "notification_type",
            "channel",
            "recipient",
            "subject",
            "body_preview",
            "status",
            "sent_at",
            "delivered_at",
            "read_at",
            "clicked_at",
            "error_message",
            "metadata",
        )
    else:
        result = spark.createDataFrame([], notification_schema)

    output.write_dataframe(result)


# =============================================================================
# NOTIFICATION SUMMARY
# =============================================================================

@transform(
    notification_log=Input("/RevOps/Commercial/notification_log"),
    output=Output("/RevOps/Commercial/notification_summary"),
)
def compute_notification_summary(ctx, notification_log: DataFrame, output):
    """
    Compute daily notification statistics.
    """
    spark = ctx.spark_session
    now = datetime.utcnow()
    today = now.date()

    if notification_log.count() == 0:
        output.write_dataframe(spark.createDataFrame([], StructType([
            StructField("summary_date", TimestampType(), True),
            StructField("customer_id", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("total_sent", IntegerType(), True),
            StructField("total_delivered", IntegerType(), True),
            StructField("total_read", IntegerType(), True),
            StructField("total_failed", IntegerType(), True),
            StructField("delivery_rate_pct", IntegerType(), True),
            StructField("read_rate_pct", IntegerType(), True),
        ])))
        return

    # Filter to today
    today_notifications = notification_log.filter(
        F.to_date(F.col("sent_at")) == F.lit(today)
    )

    # Aggregate by customer and channel
    summary = today_notifications.groupBy(
        "customer_id", "channel"
    ).agg(
        F.count("*").alias("total_sent"),
        F.sum(F.when(F.col("status").isin(["delivered", "read"]), 1).otherwise(0)).alias("total_delivered"),
        F.sum(F.when(F.col("status") == "read", 1).otherwise(0)).alias("total_read"),
        F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)).alias("total_failed"),
    ).withColumn(
        "delivery_rate_pct",
        F.when(
            F.col("total_sent") > 0,
            (F.col("total_delivered") / F.col("total_sent") * 100).cast(IntegerType())
        ).otherwise(0)
    ).withColumn(
        "read_rate_pct",
        F.when(
            F.col("total_delivered") > 0,
            (F.col("total_read") / F.col("total_delivered") * 100).cast(IntegerType())
        ).otherwise(0)
    ).withColumn(
        "summary_date", F.lit(today)
    )

    output.write_dataframe(summary)


# =============================================================================
# NOTIFICATION PREFERENCES
# =============================================================================

@transform(
    user_preferences=Input("/RevOps/Config/user_notification_preferences"),
    customer_config=Input("/RevOps/Config/customer_settings"),
    output=Output("/RevOps/Commercial/notification_preferences"),
)
def compute_notification_preferences(ctx, user_preferences: DataFrame, customer_config: DataFrame, output):
    """
    Compile effective notification preferences per user.
    """
    spark = ctx.spark_session
    now = datetime.utcnow()

    preference_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("email_enabled", BooleanType(), True),
        StructField("slack_enabled", BooleanType(), True),
        StructField("in_app_enabled", BooleanType(), True),
        StructField("digest_frequency", StringType(), True),
        StructField("quiet_hours_start", IntegerType(), True),
        StructField("quiet_hours_end", IntegerType(), True),
        StructField("alert_types_enabled", StringType(), True),
        StructField("updated_at", TimestampType(), True),
    ])

    if user_preferences.count() > 0:
        # Merge user prefs with customer defaults
        prefs = user_preferences.join(
            customer_config.select(
                "customer_id",
                F.col("alerts.enabled").alias("alerts_enabled"),
                F.col("alerts.default_channels").alias("default_channels"),
            ),
            "customer_id",
            "left"
        )

        # Apply customer-level settings if user hasn't overridden
        prefs = prefs.withColumn(
            "email_enabled",
            F.coalesce(F.col("email_enabled"), F.lit(True))
        ).withColumn(
            "slack_enabled",
            F.coalesce(F.col("slack_enabled"), F.lit(False))
        ).withColumn(
            "in_app_enabled",
            F.coalesce(F.col("in_app_enabled"), F.lit(True))
        ).withColumn(
            "digest_frequency",
            F.coalesce(F.col("digest_frequency"), F.lit("daily"))
        ).withColumn(
            "updated_at", F.lit(now)
        )

        result = prefs.select(
            "user_id",
            "customer_id",
            "email_enabled",
            "slack_enabled",
            "in_app_enabled",
            "digest_frequency",
            "quiet_hours_start",
            "quiet_hours_end",
            "alert_types_enabled",
            "updated_at",
        )
    else:
        result = spark.createDataFrame([], preference_schema)

    output.write_dataframe(result)
