# /transforms/analytics/pipeline_hygiene_analytics.py
"""
PIPELINE HYGIENE ANALYTICS
==========================
Scores each open opportunity against data quality and process hygiene rules.
Generates alerts with severity levels and recommended fixes.

Hygiene Rules:
1. Stale Close Date - Close date in the past but deal still open
2. Single-Threaded - Only 1 stakeholder engaged (high risk)
3. Missing Next Steps - No activity scheduled in next 7 days
4. Gone Dark - No activity in 14+ days
5. Missing Champion - No champion identified
6. Stalled Stage - In current stage > 2x benchmark
7. Overdue Commit - In Commit category but close date slipping
8. Heavy Discount - Discount > 20% without approval flag
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, BooleanType, ArrayType
)


@transform(
    opportunities=Input("/RevOps/Enriched/deal_health_scores"),
    activities=Input("/RevOps/Scenario/activities"),
    benchmarks=Input("/RevOps/Reference/stage_benchmarks"),
    output=Output("/RevOps/Analytics/pipeline_hygiene_alerts")
)
def compute_hygiene_alerts(ctx, opportunities, activities, benchmarks, output):
    """Score opportunities against hygiene rules and generate alerts."""

    spark = ctx.spark_session

    # Load data
    opps_df = opportunities.dataframe()
    activities_df = activities.dataframe()
    benchmarks_df = benchmarks.dataframe()

    # Reference date for calculations (scenario date)
    SCENARIO_DATE = F.to_date(F.lit("2024-11-15"))

    # ===========================================
    # PREPARE ACTIVITY METRICS
    # ===========================================

    # Get last activity date per opportunity
    last_activity = activities_df.groupBy("opportunity_id").agg(
        F.max("activity_date").alias("last_activity_date"),
        F.count("*").alias("total_activities")
    )

    # Get next scheduled activity (future activities)
    next_activity = activities_df.filter(
        F.col("activity_date") > SCENARIO_DATE
    ).groupBy("opportunity_id").agg(
        F.min("activity_date").alias("next_activity_date")
    )

    # ===========================================
    # JOIN DATA
    # ===========================================

    # Filter to open opportunities only
    open_opps = opps_df.filter(F.col("is_closed") == False)

    # Join with activity metrics
    enriched = open_opps.join(
        last_activity,
        "opportunity_id",
        "left"
    ).join(
        next_activity,
        "opportunity_id",
        "left"
    ).join(
        benchmarks_df.select(
            F.col("stage_name"),
            F.col("target_days").alias("stage_benchmark_days")
        ),
        "stage_name",
        "left"
    )

    # ===========================================
    # CALCULATE HYGIENE VIOLATIONS
    # ===========================================

    with_violations = enriched.withColumn(
        "days_since_activity",
        F.datediff(SCENARIO_DATE, F.col("last_activity_date"))
    ).withColumn(
        "days_until_close",
        F.datediff(F.col("close_date"), SCENARIO_DATE)
    ).withColumn(
        "days_until_next_activity",
        F.datediff(F.col("next_activity_date"), SCENARIO_DATE)
    ).withColumn(
        # Rule 1: Stale Close Date
        "is_stale_close_date",
        F.col("days_until_close") < 0
    ).withColumn(
        # Rule 2: Single-Threaded
        "is_single_threaded",
        F.coalesce(F.col("stakeholder_count"), F.lit(1)) <= 1
    ).withColumn(
        # Rule 3: Missing Next Steps
        "is_missing_next_steps",
        F.col("next_activity_date").isNull() | (F.col("days_until_next_activity") > 7)
    ).withColumn(
        # Rule 4: Gone Dark
        "is_gone_dark",
        F.col("days_since_activity") >= 14
    ).withColumn(
        # Rule 5: Missing Champion
        "is_missing_champion",
        F.col("has_champion") == False
    ).withColumn(
        # Rule 6: Stalled Stage
        "is_stalled_stage",
        F.col("days_in_current_stage") > (F.coalesce(F.col("stage_benchmark_days"), F.lit(14)) * 2)
    ).withColumn(
        # Rule 7: Overdue Commit
        "is_overdue_commit",
        (F.col("forecast_category") == "Commit") & (F.col("days_until_close") < 14) & (F.col("health_score") < 70)
    ).withColumn(
        # Rule 8: Heavy Discount
        "is_heavy_discount",
        F.coalesce(F.col("discount_percent"), F.lit(0)) > 20
    )

    # ===========================================
    # GENERATE ALERTS
    # ===========================================

    # Count total violations per opportunity
    with_violation_count = with_violations.withColumn(
        "violation_count",
        F.col("is_stale_close_date").cast("int") +
        F.col("is_single_threaded").cast("int") +
        F.col("is_missing_next_steps").cast("int") +
        F.col("is_gone_dark").cast("int") +
        F.col("is_missing_champion").cast("int") +
        F.col("is_stalled_stage").cast("int") +
        F.col("is_overdue_commit").cast("int") +
        F.col("is_heavy_discount").cast("int")
    )

    # Calculate hygiene score (100 = perfect, 0 = all rules violated)
    with_hygiene_score = with_violation_count.withColumn(
        "hygiene_score",
        F.greatest(F.lit(0), F.lit(100) - (F.col("violation_count") * 12.5))
    ).withColumn(
        "hygiene_category",
        F.when(F.col("hygiene_score") >= 80, "Clean")
        .when(F.col("hygiene_score") >= 60, "Minor Issues")
        .when(F.col("hygiene_score") >= 40, "Needs Attention")
        .otherwise("Critical")
    )

    # ===========================================
    # BUILD ALERT DETAILS
    # ===========================================

    # Create structured alert output
    alerts_df = with_hygiene_score.select(
        F.col("opportunity_id"),
        F.col("opportunity_name"),
        F.col("account_name"),
        F.col("owner_id"),
        F.col("owner_name"),
        F.col("amount"),
        F.col("stage_name"),
        F.col("close_date"),
        F.col("health_score"),
        F.col("hygiene_score"),
        F.col("hygiene_category"),
        F.col("violation_count"),

        # Individual violation flags
        F.col("is_stale_close_date"),
        F.col("is_single_threaded"),
        F.col("is_missing_next_steps"),
        F.col("is_gone_dark"),
        F.col("is_missing_champion"),
        F.col("is_stalled_stage"),
        F.col("is_overdue_commit"),
        F.col("is_heavy_discount"),

        # Context for alerts
        F.col("days_since_activity"),
        F.col("days_until_close"),
        F.col("days_in_current_stage"),
        F.coalesce(F.col("stakeholder_count"), F.lit(1)).alias("stakeholder_count"),
        F.coalesce(F.col("discount_percent"), F.lit(0)).alias("discount_percent"),

        # Primary alert (highest severity)
        F.when(F.col("is_gone_dark"), "GONE_DARK")
        .when(F.col("is_stale_close_date"), "STALE_CLOSE_DATE")
        .when(F.col("is_overdue_commit"), "OVERDUE_COMMIT")
        .when(F.col("is_missing_champion"), "MISSING_CHAMPION")
        .when(F.col("is_single_threaded"), "SINGLE_THREADED")
        .when(F.col("is_stalled_stage"), "STALLED_STAGE")
        .when(F.col("is_missing_next_steps"), "MISSING_NEXT_STEPS")
        .when(F.col("is_heavy_discount"), "HEAVY_DISCOUNT")
        .otherwise("NONE").alias("primary_alert"),

        # Severity based on impact
        F.when(
            F.col("is_gone_dark") | F.col("is_stale_close_date") | F.col("is_overdue_commit"),
            "CRITICAL"
        ).when(
            F.col("is_missing_champion") | F.col("is_single_threaded"),
            "HIGH"
        ).when(
            F.col("is_stalled_stage") | F.col("is_missing_next_steps"),
            "MEDIUM"
        ).when(
            F.col("is_heavy_discount"),
            "LOW"
        ).otherwise("NONE").alias("alert_severity"),

        # Recommended action
        F.when(F.col("is_gone_dark"), "Immediate outreach required - contact has gone dark for 14+ days")
        .when(F.col("is_stale_close_date"), "Update close date - current date has passed")
        .when(F.col("is_overdue_commit"), "Risk to commit - health score low with close date approaching")
        .when(F.col("is_missing_champion"), "Identify and cultivate internal champion")
        .when(F.col("is_single_threaded"), "Multi-thread: engage additional stakeholders")
        .when(F.col("is_stalled_stage"), "Deal stalled - review blockers and re-engage")
        .when(F.col("is_missing_next_steps"), "Schedule follow-up activity within 7 days")
        .when(F.col("is_heavy_discount"), "Review discount approval - exceeds 20% threshold")
        .otherwise("No action required").alias("recommended_action"),

        # Timestamp
        SCENARIO_DATE.alias("evaluated_date")
    ).orderBy(
        F.when(F.col("alert_severity") == "CRITICAL", 1)
        .when(F.col("alert_severity") == "HIGH", 2)
        .when(F.col("alert_severity") == "MEDIUM", 3)
        .when(F.col("alert_severity") == "LOW", 4)
        .otherwise(5),
        F.col("amount").desc()
    )

    output.write_dataframe(alerts_df)


@transform(
    hygiene_alerts=Input("/RevOps/Analytics/pipeline_hygiene_alerts"),
    output=Output("/RevOps/Analytics/pipeline_hygiene_summary")
)
def compute_hygiene_summary(ctx, hygiene_alerts, output):
    """Aggregate hygiene alerts into summary metrics."""

    df = hygiene_alerts.dataframe()

    # Summary by alert type
    alert_summary = df.groupBy().agg(
        F.count("*").alias("total_opportunities"),
        F.sum(F.col("is_stale_close_date").cast("int")).alias("stale_close_date_count"),
        F.sum(F.col("is_single_threaded").cast("int")).alias("single_threaded_count"),
        F.sum(F.col("is_missing_next_steps").cast("int")).alias("missing_next_steps_count"),
        F.sum(F.col("is_gone_dark").cast("int")).alias("gone_dark_count"),
        F.sum(F.col("is_missing_champion").cast("int")).alias("missing_champion_count"),
        F.sum(F.col("is_stalled_stage").cast("int")).alias("stalled_stage_count"),
        F.sum(F.col("is_overdue_commit").cast("int")).alias("overdue_commit_count"),
        F.sum(F.col("is_heavy_discount").cast("int")).alias("heavy_discount_count"),

        # By severity
        F.sum(F.when(F.col("alert_severity") == "CRITICAL", 1).otherwise(0)).alias("critical_count"),
        F.sum(F.when(F.col("alert_severity") == "HIGH", 1).otherwise(0)).alias("high_count"),
        F.sum(F.when(F.col("alert_severity") == "MEDIUM", 1).otherwise(0)).alias("medium_count"),
        F.sum(F.when(F.col("alert_severity") == "LOW", 1).otherwise(0)).alias("low_count"),

        # By hygiene category
        F.sum(F.when(F.col("hygiene_category") == "Clean", 1).otherwise(0)).alias("clean_count"),
        F.sum(F.when(F.col("hygiene_category") == "Minor Issues", 1).otherwise(0)).alias("minor_issues_count"),
        F.sum(F.when(F.col("hygiene_category") == "Needs Attention", 1).otherwise(0)).alias("needs_attention_count"),
        F.sum(F.when(F.col("hygiene_category") == "Critical", 1).otherwise(0)).alias("critical_hygiene_count"),

        # Revenue at risk
        F.sum(F.when(F.col("alert_severity") == "CRITICAL", F.col("amount")).otherwise(0)).alias("critical_revenue_at_risk"),
        F.sum(F.when(F.col("alert_severity") == "HIGH", F.col("amount")).otherwise(0)).alias("high_revenue_at_risk"),

        # Average hygiene score
        F.round(F.avg("hygiene_score"), 1).alias("avg_hygiene_score")
    ).withColumn(
        "total_alerts",
        F.col("critical_count") + F.col("high_count") + F.col("medium_count") + F.col("low_count")
    ).withColumn(
        "evaluated_date",
        F.to_date(F.lit("2024-11-15"))
    )

    output.write_dataframe(alert_summary)


@transform(
    hygiene_alerts=Input("/RevOps/Analytics/pipeline_hygiene_alerts"),
    output=Output("/RevOps/Analytics/pipeline_hygiene_by_owner")
)
def compute_hygiene_by_owner(ctx, hygiene_alerts, output):
    """Aggregate hygiene metrics by sales rep for coaching."""

    df = hygiene_alerts.dataframe()

    by_owner = df.groupBy("owner_id", "owner_name").agg(
        F.count("*").alias("total_deals"),
        F.round(F.avg("hygiene_score"), 1).alias("avg_hygiene_score"),
        F.sum("violation_count").alias("total_violations"),
        F.sum(F.when(F.col("alert_severity") == "CRITICAL", 1).otherwise(0)).alias("critical_alerts"),
        F.sum(F.when(F.col("alert_severity") == "HIGH", 1).otherwise(0)).alias("high_alerts"),

        # Most common issues for this rep
        F.sum(F.col("is_gone_dark").cast("int")).alias("gone_dark_count"),
        F.sum(F.col("is_single_threaded").cast("int")).alias("single_threaded_count"),
        F.sum(F.col("is_missing_champion").cast("int")).alias("missing_champion_count"),
        F.sum(F.col("is_missing_next_steps").cast("int")).alias("missing_next_steps_count"),

        # Revenue impacted
        F.sum(F.when(F.col("alert_severity").isin("CRITICAL", "HIGH"), F.col("amount")).otherwise(0)).alias("at_risk_revenue")
    ).withColumn(
        "primary_coaching_area",
        F.when(F.col("gone_dark_count") == F.greatest(
            F.col("gone_dark_count"),
            F.col("single_threaded_count"),
            F.col("missing_champion_count"),
            F.col("missing_next_steps_count")
        ), "Engagement Consistency")
        .when(F.col("single_threaded_count") == F.greatest(
            F.col("gone_dark_count"),
            F.col("single_threaded_count"),
            F.col("missing_champion_count"),
            F.col("missing_next_steps_count")
        ), "Multi-Threading")
        .when(F.col("missing_champion_count") == F.greatest(
            F.col("gone_dark_count"),
            F.col("single_threaded_count"),
            F.col("missing_champion_count"),
            F.col("missing_next_steps_count")
        ), "Champion Development")
        .otherwise("Activity Planning")
    ).orderBy(F.col("critical_alerts").desc(), F.col("avg_hygiene_score"))

    output.write_dataframe(by_owner)


@transform(
    hygiene_summary=Input("/RevOps/Analytics/pipeline_hygiene_summary"),
    hygiene_by_owner=Input("/RevOps/Analytics/pipeline_hygiene_by_owner"),
    output=Output("/RevOps/Analytics/pipeline_hygiene_trends")
)
def compute_hygiene_trends(ctx, hygiene_summary, hygiene_by_owner, output):
    """
    Track hygiene metrics over time for trend analysis.

    In production, this would be an incremental transform that appends
    daily snapshots. For the demo, we generate a simulated history.
    """

    spark = ctx.spark_session

    # For demo purposes, generate 8 weeks of simulated trend data
    # In production, this would be incremental snapshots

    # Simulated weekly snapshots showing improvement over time
    trend_data = [
        # Week 1 - Starting state (worst)
        ("2024-09-23", 52.3, 47, 12, 18, 8, 15, 2850000),
        # Week 2
        ("2024-09-30", 55.1, 44, 11, 16, 7, 14, 2650000),
        # Week 3
        ("2024-10-07", 58.7, 41, 10, 15, 6, 13, 2420000),
        # Week 4
        ("2024-10-14", 61.2, 38, 9, 14, 6, 12, 2180000),
        # Week 5
        ("2024-10-21", 63.8, 35, 8, 12, 5, 11, 1950000),
        # Week 6
        ("2024-10-28", 66.4, 32, 7, 11, 5, 10, 1720000),
        # Week 7
        ("2024-11-04", 68.9, 29, 6, 10, 4, 9, 1480000),
        # Week 8 - Current state (best)
        ("2024-11-11", 71.5, 26, 5, 9, 4, 8, 1250000),
    ]

    schema = StructType([
        StructField("snapshot_date", StringType(), False),
        StructField("avg_hygiene_score", DoubleType(), False),
        StructField("total_alerts", IntegerType(), False),
        StructField("critical_count", IntegerType(), False),
        StructField("gone_dark_count", IntegerType(), False),
        StructField("single_threaded_count", IntegerType(), False),
        StructField("stale_close_count", IntegerType(), False),
        StructField("revenue_at_risk", IntegerType(), False),
    ])

    trends_df = spark.createDataFrame(trend_data, schema).withColumn(
        "snapshot_date",
        F.to_date(F.col("snapshot_date"))
    ).withColumn(
        # Calculate week-over-week change
        "score_change_wow",
        F.col("avg_hygiene_score") - F.lag("avg_hygiene_score", 1).over(
            Window.orderBy("snapshot_date")
        )
    ).withColumn(
        "alerts_change_wow",
        F.col("total_alerts") - F.lag("total_alerts", 1).over(
            Window.orderBy("snapshot_date")
        )
    ).withColumn(
        "week_number",
        F.weekofyear(F.col("snapshot_date"))
    ).withColumn(
        "quarter",
        F.concat(
            F.lit("Q"),
            F.quarter(F.col("snapshot_date")).cast("string"),
            F.lit(" "),
            F.year(F.col("snapshot_date")).cast("string")
        )
    )

    output.write_dataframe(trends_df)


@transform(
    hygiene_alerts=Input("/RevOps/Analytics/pipeline_hygiene_alerts"),
    output=Output("/RevOps/Analytics/hygiene_alert_feed")
)
def compute_hygiene_alert_feed(ctx, hygiene_alerts, output):
    """
    Generate a real-time alert feed for Slack/webhook integration.

    This output is designed to be consumed by external notification
    systems. Each row represents an actionable alert with all context
    needed for the notification payload.
    """

    df = hygiene_alerts.dataframe()

    # Filter to only actionable alerts (not NONE severity)
    actionable = df.filter(F.col("alert_severity") != "NONE")

    # Build the alert feed with webhook-friendly structure
    alert_feed = actionable.select(
        # Unique alert identifier
        F.concat(
            F.col("opportunity_id"),
            F.lit("_"),
            F.col("primary_alert"),
            F.lit("_"),
            F.date_format(F.col("evaluated_date"), "yyyyMMdd")
        ).alias("alert_id"),

        # Core alert info
        F.col("primary_alert").alias("alert_type"),
        F.col("alert_severity"),
        F.col("recommended_action"),

        # Deal context
        F.col("opportunity_id"),
        F.col("opportunity_name"),
        F.col("account_name"),
        F.col("amount"),
        F.col("stage_name"),
        F.col("close_date"),
        F.col("health_score"),
        F.col("hygiene_score"),

        # Owner for routing
        F.col("owner_id"),
        F.col("owner_name"),

        # Alert context for message formatting
        F.col("days_since_activity"),
        F.col("days_until_close"),
        F.col("days_in_current_stage"),
        F.col("stakeholder_count"),
        F.col("discount_percent"),

        # Timestamps
        F.col("evaluated_date"),
        F.current_timestamp().alias("generated_at"),

        # Slack-ready message preview
        F.concat(
            F.lit(":"),
            F.when(F.col("alert_severity") == "CRITICAL", "rotating_light:")
            .when(F.col("alert_severity") == "HIGH", "warning:")
            .when(F.col("alert_severity") == "MEDIUM", "large_yellow_circle:")
            .otherwise("white_circle:"),
            F.lit(" *"),
            F.col("alert_severity"),
            F.lit("* - "),
            F.col("account_name"),
            F.lit(" ($"),
            F.format_number(F.col("amount"), 0),
            F.lit(") - "),
            F.col("recommended_action")
        ).alias("slack_message_preview"),

        # Priority for ordering (lower = more urgent)
        F.when(F.col("alert_severity") == "CRITICAL", 1)
        .when(F.col("alert_severity") == "HIGH", 2)
        .when(F.col("alert_severity") == "MEDIUM", 3)
        .otherwise(4).alias("priority_rank")

    ).orderBy("priority_rank", F.col("amount").desc())

    output.write_dataframe(alert_feed)
