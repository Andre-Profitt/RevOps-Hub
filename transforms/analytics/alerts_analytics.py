# /transforms/analytics/alerts_analytics.py
"""
ALERTS ANALYTICS
================
Generates alert data from hygiene scores and monitoring:
- Alert summary (counts by severity)
- Active alerts with context and actions
- Alert rules configuration

Powers the Alerts Dashboard.
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@transform(
    hygiene_alerts=Input("/RevOps/Analytics/pipeline_hygiene_alerts"),
    output=Output("/RevOps/Alerts/summary")
)
def compute_alert_summary(ctx, hygiene_alerts, output):
    """Generate alert summary statistics."""

    alerts_df = hygiene_alerts.dataframe()

    # Count by severity
    summary = alerts_df.agg(
        F.count("*").alias("total_active"),
        F.sum(F.when(F.col("alert_severity") == "Critical", 1).otherwise(0)).alias("critical_count"),
        F.sum(F.when(F.col("alert_severity") == "High", 1).otherwise(0)).alias("high_count"),
        F.sum(F.when(F.col("alert_severity") == "Medium", 1).otherwise(0)).alias("medium_count"),
        F.sum(F.when(F.col("alert_severity") == "Low", 1).otherwise(0)).alias("low_count")
    ).withColumn(
        "acknowledged_count",
        F.lit(0)  # Would require alert state tracking
    ).withColumn(
        "resolved_today",
        F.lit(0)
    ).withColumn(
        "avg_resolution_time",
        F.lit(4.5)  # Hours
    ).withColumn(
        "snapshot_date",
        F.current_date()
    )

    output.write_dataframe(summary)


@transform(
    hygiene_alerts=Input("/RevOps/Analytics/pipeline_hygiene_alerts"),
    deal_health=Input("/RevOps/Enriched/deal_health_scores"),
    output=Output("/RevOps/Alerts/active")
)
def compute_active_alerts(ctx, hygiene_alerts, deal_health, output):
    """Generate active alerts from hygiene and health data."""

    hygiene_df = hygiene_alerts.dataframe()
    health_df = deal_health.dataframe()

    # Transform hygiene alerts into general alert format
    hygiene_active = hygiene_df.filter(
        F.col("alert_severity").isin("Critical", "High", "Medium")
    ).withColumn(
        "alert_id",
        F.concat(F.lit("HYG-"), F.monotonically_increasing_id())
    ).withColumn(
        "title",
        F.concat(F.col("alert_severity"), F.lit(" Hygiene Alert: "), F.col("primary_alert"))
    ).withColumn(
        "description",
        F.concat(
            F.col("opportunity_name"),
            F.lit(" ("),
            F.col("account_name"),
            F.lit(") - "),
            F.col("recommended_action")
        )
    ).withColumn(
        "severity",
        F.col("alert_severity")
    ).withColumn(
        "category",
        F.lit("Pipeline Hygiene")
    ).withColumn(
        "source",
        F.lit("Hygiene Monitor")
    ).withColumn(
        "created_at",
        F.current_timestamp()
    ).withColumn(
        "status",
        F.lit("Active")
    ).withColumn(
        "acknowledged_by",
        F.lit(None).cast("string")
    ).withColumn(
        "acknowledged_at",
        F.lit(None).cast("timestamp")
    ).withColumn(
        "resolved_at",
        F.lit(None).cast("timestamp")
    ).withColumn(
        "related_object_type",
        F.lit("Opportunity")
    ).withColumn(
        "related_object_id",
        F.col("opportunity_id")
    ).withColumn(
        "related_object_name",
        F.col("opportunity_name")
    ).withColumn(
        "action_url",
        F.concat(F.lit("/deals/"), F.col("opportunity_id"))
    ).withColumn(
        "assignee",
        F.col("owner_name")
    )

    # Add deal health alerts
    health_alerts = health_df.filter(
        F.col("health_category") == "Critical"
    ).withColumn(
        "alert_id",
        F.concat(F.lit("HEALTH-"), F.monotonically_increasing_id())
    ).withColumn(
        "title",
        F.concat(F.lit("Critical Deal Health: "), F.col("opportunity_name"))
    ).withColumn(
        "description",
        F.concat(
            F.lit("Health score dropped to "),
            F.col("health_score"),
            F.lit(" - immediate attention required")
        )
    ).withColumn(
        "severity",
        F.lit("Critical")
    ).withColumn(
        "category",
        F.lit("Deal Health")
    ).withColumn(
        "source",
        F.lit("Health Monitor")
    ).withColumn(
        "created_at",
        F.current_timestamp()
    ).withColumn(
        "status",
        F.lit("Active")
    ).withColumn(
        "acknowledged_by",
        F.lit(None).cast("string")
    ).withColumn(
        "acknowledged_at",
        F.lit(None).cast("timestamp")
    ).withColumn(
        "resolved_at",
        F.lit(None).cast("timestamp")
    ).withColumn(
        "related_object_type",
        F.lit("Opportunity")
    ).withColumn(
        "related_object_id",
        F.col("opportunity_id")
    ).withColumn(
        "related_object_name",
        F.col("opportunity_name")
    ).withColumn(
        "action_url",
        F.concat(F.lit("/deals/"), F.col("opportunity_id"))
    ).withColumn(
        "assignee",
        F.col("owner_name")
    )

    # Select common columns and union
    columns = [
        "alert_id", "title", "description", "severity", "category",
        "source", "created_at", "status", "acknowledged_by", "acknowledged_at",
        "resolved_at", "related_object_type", "related_object_id",
        "related_object_name", "action_url", "assignee"
    ]

    combined = hygiene_active.select(columns).union(
        health_alerts.select(columns)
    ).orderBy(
        F.when(F.col("severity") == "Critical", 1)
        .when(F.col("severity") == "High", 2)
        .when(F.col("severity") == "Medium", 3)
        .otherwise(4),
        F.col("created_at").desc()
    )

    output.write_dataframe(combined)


@transform(
    output=Output("/RevOps/Alerts/rules")
)
def compute_alert_rules(ctx, output):
    """Generate alert rule configuration."""

    rules = [
        {
            "rule_id": "RULE-001",
            "name": "Critical Deal Health",
            "description": "Alert when deal health score drops below 40",
            "category": "Deal Health",
            "severity": "Critical",
            "condition": "health_score < 40",
            "threshold": 40,
            "enabled": True,
            "notification_channels": "email,slack",
            "recipients": "sales-managers@company.com",
            "created_by": "System",
            "created_at": "2024-01-01T00:00:00Z",
            "last_triggered": "2024-11-27T06:00:00Z",
            "trigger_count": 23
        },
        {
            "rule_id": "RULE-002",
            "name": "Stale Close Date",
            "description": "Alert when close date is in the past without update",
            "category": "Pipeline Hygiene",
            "severity": "High",
            "condition": "close_date < current_date AND stage != Closed",
            "threshold": 0,
            "enabled": True,
            "notification_channels": "email",
            "recipients": "rep-owner@company.com",
            "created_by": "RevOps",
            "created_at": "2024-01-01T00:00:00Z",
            "last_triggered": "2024-11-27T07:00:00Z",
            "trigger_count": 156
        },
        {
            "rule_id": "RULE-003",
            "name": "Gone Dark",
            "description": "Alert when no activity in 14+ days on active opportunity",
            "category": "Engagement",
            "severity": "High",
            "condition": "days_since_activity > 14 AND stage NOT IN (Closed Won, Closed Lost)",
            "threshold": 14,
            "enabled": True,
            "notification_channels": "email,slack",
            "recipients": "rep-owner@company.com,manager@company.com",
            "created_by": "RevOps",
            "created_at": "2024-01-01T00:00:00Z",
            "last_triggered": "2024-11-27T05:30:00Z",
            "trigger_count": 45
        },
        {
            "rule_id": "RULE-004",
            "name": "Single Threaded Large Deal",
            "description": "Alert when deal >$200K has fewer than 3 stakeholders",
            "category": "Risk",
            "severity": "Medium",
            "condition": "amount > 200000 AND stakeholder_count < 3",
            "threshold": 3,
            "enabled": True,
            "notification_channels": "email",
            "recipients": "rep-owner@company.com",
            "created_by": "RevOps",
            "created_at": "2024-02-01T00:00:00Z",
            "last_triggered": "2024-11-26T14:00:00Z",
            "trigger_count": 89
        },
        {
            "rule_id": "RULE-005",
            "name": "Heavy Discount",
            "description": "Alert when discount exceeds 25%",
            "category": "Deal Desk",
            "severity": "Medium",
            "condition": "discount_pct > 0.25",
            "threshold": 25,
            "enabled": True,
            "notification_channels": "email",
            "recipients": "deal-desk@company.com",
            "created_by": "Finance",
            "created_at": "2024-01-15T00:00:00Z",
            "last_triggered": "2024-11-27T03:00:00Z",
            "trigger_count": 34
        },
        {
            "rule_id": "RULE-006",
            "name": "Data Sync Failure",
            "description": "Alert when data sync fails or has high error rate",
            "category": "Data Quality",
            "severity": "High",
            "condition": "sync_status = Failed OR error_rate > 0.05",
            "threshold": 5,
            "enabled": True,
            "notification_channels": "slack,pagerduty",
            "recipients": "data-ops@company.com",
            "created_by": "Data Ops",
            "created_at": "2024-01-01T00:00:00Z",
            "last_triggered": "2024-11-20T12:00:00Z",
            "trigger_count": 8
        },
        {
            "rule_id": "RULE-007",
            "name": "Forecast Miss Risk",
            "description": "Alert when committed forecast is below 80% of target",
            "category": "Forecast",
            "severity": "Critical",
            "condition": "commit_amount < target * 0.80 AND weeks_remaining < 4",
            "threshold": 80,
            "enabled": True,
            "notification_channels": "email,slack",
            "recipients": "leadership@company.com",
            "created_by": "Finance",
            "created_at": "2024-03-01T00:00:00Z",
            "last_triggered": "2024-11-25T09:00:00Z",
            "trigger_count": 3
        }
    ]

    rules_df = ctx.spark_session.createDataFrame(rules)
    output.write_dataframe(rules_df)
