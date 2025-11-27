# /transforms/ingestion/telemetry_feeds.py
"""
TELEMETRY & SUPPORT FEEDS
=========================
Integrates product usage, support tickets, and marketing data
into the RevOps data model for cross-functional analysis.

Input Sources:
- Product Analytics (Snowflake/BigQuery/Amplitude/Mixpanel)
- Support Tickets (Zendesk/Jira/Intercom)
- Marketing Automation (Marketo/Pardot/HubSpot)

Output:
- /RevOps/Telemetry/product_usage - Product engagement signals
- /RevOps/Telemetry/support_tickets - Support health signals
- /RevOps/Telemetry/marketing_touches - Marketing attribution
- /RevOps/Telemetry/unified_timeline - Combined activity timeline
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =============================================================================
# PRODUCT USAGE TELEMETRY
# =============================================================================

@transform(
    product_events=Input("/Analytics/Raw/ProductEvents"),
    account_mapping=Input("/RevOps/Reference/account_domain_mapping"),
    output=Output("/RevOps/Telemetry/product_usage")
)
def ingest_product_usage(ctx, product_events, account_mapping, output):
    """
    Transform product analytics events into usage signals.

    Calculates:
    - Daily/weekly/monthly active users per account
    - Feature adoption scores
    - Usage trends and engagement health
    """

    events = product_events.dataframe()
    mapping = account_mapping.dataframe()

    # Aggregate events by account and time period
    usage_by_account = events.groupBy(
        F.col("account_id"),
        F.date_trunc("day", F.col("event_timestamp")).alias("usage_date")
    ).agg(
        F.countDistinct("user_id").alias("daily_active_users"),
        F.count("*").alias("total_events"),
        F.countDistinct("event_name").alias("features_used"),
        F.sum(F.when(F.col("event_name").like("%core%"), 1).otherwise(0)).alias("core_feature_events"),
        F.sum(F.when(F.col("event_name").like("%premium%"), 1).otherwise(0)).alias("premium_feature_events"),
        F.max("event_timestamp").alias("last_activity")
    )

    # Calculate rolling metrics
    window_7d = Window.partitionBy("account_id").orderBy("usage_date").rowsBetween(-6, 0)
    window_30d = Window.partitionBy("account_id").orderBy("usage_date").rowsBetween(-29, 0)

    with_rolling = usage_by_account.withColumn(
        "wau",
        F.sum("daily_active_users").over(window_7d)
    ).withColumn(
        "mau",
        F.sum("daily_active_users").over(window_30d)
    ).withColumn(
        "engagement_score",
        F.round(
            (F.col("daily_active_users") * 0.3 +
             F.col("features_used") * 0.3 +
             F.least(F.col("total_events") / 100, F.lit(10)) * 0.4),
            1
        )
    )

    # Derive health status
    final = with_rolling.withColumn(
        "usage_health",
        F.when(F.col("engagement_score") >= 7, "Healthy")
        .when(F.col("engagement_score") >= 4, "Monitor")
        .when(F.col("engagement_score") >= 2, "At Risk")
        .otherwise("Critical")
    ).withColumn(
        "usage_trend",
        F.when(F.col("engagement_score") > F.lag("engagement_score", 7).over(
            Window.partitionBy("account_id").orderBy("usage_date")
        ), "increasing")
        .when(F.col("engagement_score") < F.lag("engagement_score", 7).over(
            Window.partitionBy("account_id").orderBy("usage_date")
        ), "decreasing")
        .otherwise("stable")
    ).withColumn(
        "_synced_at",
        F.current_timestamp()
    )

    output.write_dataframe(final)


# =============================================================================
# SUPPORT TICKET DATA
# =============================================================================

@transform(
    zendesk_tickets=Input("/Zendesk/Raw/Tickets"),
    account_mapping=Input("/RevOps/Reference/account_domain_mapping"),
    output=Output("/RevOps/Telemetry/support_tickets")
)
def ingest_support_tickets(ctx, zendesk_tickets, account_mapping, output):
    """
    Transform support ticket data into customer health signals.

    Calculates:
    - Ticket volume and severity trends
    - Resolution times
    - Support health score per account
    """

    tickets = zendesk_tickets.dataframe()
    mapping = account_mapping.dataframe()

    # Map tickets to RevOps accounts via domain/email
    # (In production, use email domain or explicit account linkage)
    ticket_enriched = tickets.withColumn(
        "email_domain",
        F.regexp_extract(F.col("requester_email"), "@(.+)$", 1)
    )

    # Join to get account_id
    with_account = ticket_enriched.join(
        mapping.select("domain", F.col("account_id").alias("_acc_id")),
        ticket_enriched.email_domain == mapping.domain,
        "left"
    ).withColumn(
        "account_id",
        F.coalesce(F.col("_acc_id"), F.col("organization_id"))
    ).drop("_acc_id")

    # Normalize ticket data
    normalized = with_account.select(
        F.col("id").alias("ticket_id"),
        F.col("account_id"),
        F.col("subject"),
        F.col("description"),
        F.col("status"),
        F.when(F.col("priority") == "urgent", "Critical")
        .when(F.col("priority") == "high", "High")
        .when(F.col("priority") == "normal", "Medium")
        .otherwise("Low").alias("severity"),
        F.col("type").alias("ticket_type"),
        F.col("tags"),
        F.col("created_at").alias("created_date"),
        F.col("updated_at").alias("updated_date"),
        F.col("solved_at").alias("resolved_date"),
        F.col("requester_email"),
        F.col("assignee_name"),
        F.col("satisfaction_rating"),
    )

    # Calculate resolution time
    with_metrics = normalized.withColumn(
        "resolution_hours",
        F.when(
            F.col("resolved_date").isNotNull(),
            F.round((F.unix_timestamp("resolved_date") - F.unix_timestamp("created_date")) / 3600, 1)
        ).otherwise(F.lit(None))
    ).withColumn(
        "is_escalated",
        F.col("severity").isin("Critical", "High")
    ).withColumn(
        "is_open",
        ~F.col("status").isin("solved", "closed")
    )

    # Add support health scoring
    final = with_metrics.withColumn(
        "support_impact_score",
        F.when(F.col("severity") == "Critical", 10)
        .when(F.col("severity") == "High", 5)
        .when(F.col("severity") == "Medium", 2)
        .otherwise(1)
    ).withColumn(
        "source_system",
        F.lit("Zendesk")
    ).withColumn(
        "_synced_at",
        F.current_timestamp()
    )

    output.write_dataframe(final)


@transform(
    support_tickets=Input("/RevOps/Telemetry/support_tickets"),
    output=Output("/RevOps/Telemetry/support_health_by_account")
)
def compute_support_health(ctx, support_tickets, output):
    """Aggregate support metrics by account for health scoring."""

    tickets = support_tickets.dataframe()

    # Aggregate per account
    by_account = tickets.groupBy("account_id").agg(
        F.count("*").alias("total_tickets_30d"),
        F.sum(F.when(F.col("is_open"), 1).otherwise(0)).alias("open_tickets"),
        F.sum(F.when(F.col("severity") == "Critical", 1).otherwise(0)).alias("critical_tickets"),
        F.sum(F.when(F.col("severity") == "High", 1).otherwise(0)).alias("high_tickets"),
        F.avg("resolution_hours").alias("avg_resolution_hours"),
        F.avg("satisfaction_rating").alias("avg_csat"),
        F.sum("support_impact_score").alias("total_impact_score"),
        F.max("created_date").alias("last_ticket_date")
    )

    # Calculate support health score
    with_health = by_account.withColumn(
        "support_health_score",
        F.greatest(
            F.lit(0),
            F.lit(100) -
            (F.col("critical_tickets") * 20) -
            (F.col("high_tickets") * 10) -
            (F.col("open_tickets") * 5) -
            F.when(F.col("avg_resolution_hours") > 48, 10).otherwise(0)
        )
    ).withColumn(
        "support_health_status",
        F.when(F.col("support_health_score") >= 80, "Healthy")
        .when(F.col("support_health_score") >= 60, "Monitor")
        .when(F.col("support_health_score") >= 40, "At Risk")
        .otherwise("Critical")
    )

    output.write_dataframe(with_health)


# =============================================================================
# MARKETING ATTRIBUTION
# =============================================================================

@transform(
    marketo_activities=Input("/Marketo/Raw/Activities"),
    account_mapping=Input("/RevOps/Reference/account_domain_mapping"),
    output=Output("/RevOps/Telemetry/marketing_touches")
)
def ingest_marketing_touches(ctx, marketo_activities, account_mapping, output):
    """
    Transform marketing automation data into attribution touchpoints.

    Captures:
    - Email engagement (sends, opens, clicks)
    - Form submissions
    - Content downloads
    - Webinar attendance
    - Ad interactions
    """

    activities = marketo_activities.dataframe()
    mapping = account_mapping.dataframe()

    # Normalize activity types
    normalized = activities.select(
        F.col("id").alias("touch_id"),
        F.col("leadId").alias("contact_id"),
        F.when(F.col("activityTypeId") == 6, "Email Sent")
        .when(F.col("activityTypeId") == 7, "Email Delivered")
        .when(F.col("activityTypeId") == 8, "Email Bounced")
        .when(F.col("activityTypeId") == 9, "Email Unsubscribed")
        .when(F.col("activityTypeId") == 10, "Email Opened")
        .when(F.col("activityTypeId") == 11, "Email Clicked")
        .when(F.col("activityTypeId") == 2, "Form Filled")
        .when(F.col("activityTypeId") == 1, "Page Visit")
        .otherwise("Other").alias("touch_type"),
        F.col("activityDate").alias("touch_date"),
        F.col("campaignId").alias("campaign_id"),
        F.col("primaryAttributeValue").alias("asset_name"),
    )

    # Map to accounts via contact email domain
    # (In production, use lead-to-account matching)
    enriched = normalized.withColumn(
        "is_engagement",
        F.col("touch_type").isin("Email Opened", "Email Clicked", "Form Filled")
    ).withColumn(
        "touch_score",
        F.when(F.col("touch_type") == "Form Filled", 10)
        .when(F.col("touch_type") == "Email Clicked", 5)
        .when(F.col("touch_type") == "Email Opened", 2)
        .when(F.col("touch_type") == "Page Visit", 1)
        .otherwise(0)
    ).withColumn(
        "source_system",
        F.lit("Marketo")
    ).withColumn(
        "_synced_at",
        F.current_timestamp()
    )

    output.write_dataframe(enriched)


# =============================================================================
# UNIFIED TIMELINE
# =============================================================================

@transform(
    sales_activities=Input("/RevOps/Staging/activities"),
    product_usage=Input("/RevOps/Telemetry/product_usage"),
    support_tickets=Input("/RevOps/Telemetry/support_tickets"),
    marketing_touches=Input("/RevOps/Telemetry/marketing_touches"),
    output=Output("/RevOps/Telemetry/unified_timeline")
)
def build_unified_timeline(ctx, sales_activities, product_usage, support_tickets, marketing_touches, output):
    """
    Create unified timeline of all customer touchpoints.

    Combines:
    - Sales activities (calls, emails, meetings)
    - Product usage events
    - Support interactions
    - Marketing engagements
    """

    # Transform each source to common schema
    sales = sales_activities.dataframe().select(
        F.col("account_id"),
        F.col("opportunity_id"),
        F.col("activity_date").alias("event_date"),
        F.col("activity_type").alias("event_type"),
        F.lit("Sales").alias("event_category"),
        F.col("subject").alias("event_description"),
        F.col("owner_name").alias("actor"),
        F.lit(5).alias("engagement_score"),
        F.lit("CRM").alias("source_system"),
    )

    product = product_usage.dataframe().select(
        F.col("account_id"),
        F.lit(None).cast("string").alias("opportunity_id"),
        F.col("usage_date").alias("event_date"),
        F.lit("Product Usage").alias("event_type"),
        F.lit("Product").alias("event_category"),
        F.concat(F.lit("DAU: "), F.col("daily_active_users")).alias("event_description"),
        F.lit("System").alias("actor"),
        F.col("engagement_score"),
        F.lit("Product Analytics").alias("source_system"),
    )

    support = support_tickets.dataframe().select(
        F.col("account_id"),
        F.lit(None).cast("string").alias("opportunity_id"),
        F.col("created_date").alias("event_date"),
        F.concat(F.lit("Support Ticket: "), F.col("severity")).alias("event_type"),
        F.lit("Support").alias("event_category"),
        F.col("subject").alias("event_description"),
        F.col("requester_email").alias("actor"),
        F.col("support_impact_score").alias("engagement_score"),
        F.col("source_system"),
    )

    marketing = marketing_touches.dataframe().filter(F.col("is_engagement")).select(
        F.lit(None).cast("string").alias("account_id"),  # Would need contact-to-account mapping
        F.lit(None).cast("string").alias("opportunity_id"),
        F.col("touch_date").alias("event_date"),
        F.col("touch_type").alias("event_type"),
        F.lit("Marketing").alias("event_category"),
        F.col("asset_name").alias("event_description"),
        F.col("contact_id").alias("actor"),
        F.col("touch_score").alias("engagement_score"),
        F.col("source_system"),
    )

    # Union all sources
    unified = sales.union(product).union(support).union(marketing)

    # Add metadata
    final = unified.withColumn(
        "event_id",
        F.monotonically_increasing_id()
    ).withColumn(
        "_created_at",
        F.current_timestamp()
    ).orderBy(F.col("event_date").desc())

    output.write_dataframe(final)


# =============================================================================
# FEATURE USAGE TELEMETRY
# =============================================================================

@transform(
    product_usage=Input("/RevOps/Telemetry/product_usage"),
    output=Output("/RevOps/Telemetry/feature_usage")
)
def compute_feature_usage(ctx, product_usage, output):
    """
    Aggregate feature usage metrics for usage metering.

    Tracks:
    - Feature adoption by customer
    - Usage patterns and trends
    - Feature stickiness scores
    """

    usage = product_usage.dataframe()

    # Aggregate feature usage by customer
    feature_usage = usage.groupBy("account_id").agg(
        F.sum("features_used").alias("total_features_used"),
        F.sum("core_feature_events").alias("core_feature_usage"),
        F.sum("premium_feature_events").alias("premium_feature_usage"),
        F.avg("engagement_score").alias("avg_engagement_score"),
        F.count("*").alias("active_days_30d"),
        F.max("usage_date").alias("last_active_date"),
        F.min("usage_date").alias("first_active_date"),
    ).withColumn(
        "feature_adoption_score",
        F.round(
            (F.col("total_features_used") / 30) * 0.4 +
            (F.col("active_days_30d") / 30) * 0.3 +
            (F.col("avg_engagement_score") / 10) * 0.3,
            2
        ) * 100
    ).withColumn(
        "adoption_tier",
        F.when(F.col("feature_adoption_score") >= 70, "Power User")
        .when(F.col("feature_adoption_score") >= 40, "Active User")
        .when(F.col("feature_adoption_score") >= 20, "Light User")
        .otherwise("At Risk")
    ).withColumn(
        "premium_usage_pct",
        F.when(
            (F.col("core_feature_usage") + F.col("premium_feature_usage")) > 0,
            F.round(F.col("premium_feature_usage") /
                   (F.col("core_feature_usage") + F.col("premium_feature_usage")) * 100, 1)
        ).otherwise(0)
    ).withColumn(
        "snapshot_date",
        F.current_date()
    ).withColumn(
        "_synced_at",
        F.current_timestamp()
    )

    output.write_dataframe(feature_usage)
