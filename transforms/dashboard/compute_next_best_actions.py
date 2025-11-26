# /transforms/dashboard/compute_next_best_actions.py
"""
NEXT BEST ACTIONS CALCULATOR
============================
Generates prioritized action recommendations for each rep based on:
- Deal health scores
- Pipeline hygiene violations
- Days since last activity
- Deal amount and close date
- Specific risk factors

This powers the "Next Best Actions" widget in the Rep Coaching Dashboard.
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@transform(
    deal_health=Input("/RevOps/Enriched/deal_health_scores"),
    hygiene_alerts=Input("/RevOps/Analytics/pipeline_hygiene_alerts"),
    output=Output("/RevOps/Dashboard/next_best_actions")
)
def compute(ctx, deal_health, hygiene_alerts, output):
    """Generate prioritized next best actions for each rep."""

    df = deal_health.dataframe()
    hygiene_df = hygiene_alerts.dataframe()

    # Filter to open opportunities only
    open_opps = df.filter(F.col("stage_name").isin(
        "Prospecting", "Discovery", "Solution Design",
        "Proposal", "Negotiation", "Verbal Commit"
    ))

    # Join with hygiene alerts to incorporate hygiene violations
    with_hygiene = open_opps.join(
        hygiene_df.select(
            "opportunity_id",
            "hygiene_score",
            "hygiene_category",
            "violation_count",
            "primary_alert",
            "alert_severity",
            "recommended_action",
            "is_stale_close_date",
            "is_single_threaded",
            "is_missing_next_steps",
            "is_gone_dark",
            "is_missing_champion"
        ),
        "opportunity_id",
        "left"
    )

    # Calculate priority score - now including hygiene factors
    # Higher score = more urgent action needed
    scored = with_hygiene.withColumn(
        "priority_score",
        # Health factor (lower health = higher priority)
        (100 - F.col("health_score")) * 2 +
        # Hygiene factor (lower hygiene = higher priority)
        (100 - F.coalesce(F.col("hygiene_score"), F.lit(100))) * 1.5 +
        # Recency factor (more days since activity = higher priority)
        F.least(F.col("days_since_activity") * 5, F.lit(50)) +
        # Amount factor (larger deals = higher priority)
        F.when(F.col("amount") >= 1000000, 30)
        .when(F.col("amount") >= 500000, 20)
        .when(F.col("amount") >= 200000, 10)
        .otherwise(5) +
        # Close date proximity (closer = higher priority)
        F.when(
            F.datediff(F.col("close_date"), F.to_date(F.lit("2024-11-15"))) <= 15,
            25
        ).when(
            F.datediff(F.col("close_date"), F.to_date(F.lit("2024-11-15"))) <= 30,
            15
        ).otherwise(5) +
        # Risk flag bonuses
        F.when(F.col("health_category") == "Critical", 30)
        .when(F.col("health_category") == "At Risk", 15)
        .otherwise(0) +
        # Hygiene severity bonuses
        F.when(F.col("alert_severity") == "CRITICAL", 40)
        .when(F.col("alert_severity") == "HIGH", 25)
        .when(F.col("alert_severity") == "MEDIUM", 10)
        .otherwise(0)
    )

    # Determine action type based on situation (hygiene-aware)
    with_action = scored.withColumn(
        "action_type",
        # Hygiene-based actions take priority
        F.when(F.col("is_gone_dark") == True, "call")
        .when(F.col("is_stale_close_date") == True, "update")
        .when(F.col("is_missing_champion") == True, "meeting")
        .when(F.col("is_single_threaded") == True, "meeting")
        .when(F.col("is_missing_next_steps") == True, "task")
        # Fall back to health-based actions
        .when(
            (F.col("days_since_activity") >= 7) |
            (F.col("health_category").isin("Critical", "At Risk")),
            "call"
        ).when(
            F.col("stage_name") == "Proposal",
            "call"
        ).when(
            F.col("stakeholder_count") < 2,
            "meeting"  # Need to multi-thread
        ).otherwise("email")
    ).withColumn(
        "action_reason",
        # Use hygiene recommended action if available
        F.when(
            F.col("alert_severity").isin("CRITICAL", "HIGH"),
            F.col("recommended_action")
        )
        # Fall back to original logic
        .when(
            F.col("days_since_activity") >= 10,
            F.concat(F.lit("Re-engage - "), F.col("days_since_activity"), F.lit(" days since last contact"))
        ).when(
            F.col("days_since_activity") >= 7,
            "Follow up - deal going quiet"
        ).when(
            F.col("health_category") == "Critical",
            "Urgent attention - deal at critical health"
        ).when(
            F.col("stakeholder_count") < 2,
            "Expand contacts - currently single-threaded"
        ).when(
            F.col("stage_name") == "Proposal",
            "Follow up on pricing proposal"
        ).when(
            F.col("is_competitive") == True,
            "Competitive deal - maintain momentum"
        ).otherwise("Regular check-in")
    ).withColumn(
        "urgency",
        # Hygiene CRITICAL alerts are always "today"
        F.when(F.col("alert_severity") == "CRITICAL", "today")
        .when(F.col("priority_score") >= 150, "today")
        .when(F.col("alert_severity") == "HIGH", "this_week")
        .when(F.col("priority_score") >= 100, "this_week")
        .otherwise("soon")
    )

    # Rank within each rep
    window = Window.partitionBy("owner_id").orderBy(F.col("priority_score").desc())

    ranked = with_action.withColumn(
        "priority_rank",
        F.row_number().over(window)
    )

    # Select output columns
    result = ranked.select(
        F.concat(F.col("opportunity_id"), F.lit("-action")).alias("action_id"),
        F.col("owner_id").alias("rep_id"),
        "opportunity_id",
        "account_name",
        "opportunity_name",
        # Contact would come from a contacts table in production
        F.lit("Primary Contact").alias("contact_name"),
        F.lit("Champion").alias("contact_role"),
        "action_type",
        "action_reason",
        "priority_score",
        "priority_rank",
        # Best time (simplified - would use calendar/activity patterns)
        F.lit("2:00 PM").alias("best_time_start"),
        F.lit("4:00 PM").alias("best_time_end"),
        "urgency",
        F.col("amount").alias("deal_amount"),
        F.col("health_score").alias("deal_health"),
        "days_since_activity",

        # Hygiene fields
        F.coalesce(F.col("hygiene_score"), F.lit(100)).alias("hygiene_score"),
        F.coalesce(F.col("hygiene_category"), F.lit("Clean")).alias("hygiene_category"),
        F.coalesce(F.col("violation_count"), F.lit(0)).alias("violation_count"),
        F.coalesce(F.col("primary_alert"), F.lit("NONE")).alias("primary_alert"),
        F.coalesce(F.col("alert_severity"), F.lit("NONE")).alias("alert_severity"),

        # Risk context - now hygiene-aware
        F.when(
            F.col("alert_severity") == "CRITICAL",
            "Critical hygiene issue - immediate action required"
        ).when(
            F.col("alert_severity") == "HIGH",
            "High-priority hygiene violation"
        ).when(
            F.col("days_since_activity") >= 10,
            "High risk of deal going cold"
        ).when(
            F.col("health_category") == "Critical",
            "Deal may slip quarter without intervention"
        ).otherwise("Maintain deal momentum").alias("risk_if_delayed"),

        # Display helpers - updated with new action types
        F.when(F.col("action_type") == "call", "phone")
        .when(F.col("action_type") == "email", "mail")
        .when(F.col("action_type") == "meeting", "calendar")
        .when(F.col("action_type") == "update", "edit")
        .when(F.col("action_type") == "task", "check-square")
        .otherwise("clipboard").alias("action_icon"),
        F.lit("Today 2-4pm").alias("time_display")
    ).filter(
        F.col("priority_rank") <= 10  # Top 10 actions per rep
    )

    output.write_dataframe(result)
