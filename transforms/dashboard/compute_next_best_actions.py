# /transforms/dashboard/compute_next_best_actions.py
"""
NEXT BEST ACTIONS CALCULATOR
============================
Generates prioritized action recommendations for each rep based on:
- Deal health scores
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
    output=Output("/RevOps/Dashboard/next_best_actions")
)
def compute(ctx, deal_health, output):
    """Generate prioritized next best actions for each rep."""

    df = deal_health.dataframe()

    # Filter to open opportunities only
    open_opps = df.filter(F.col("stage_name").isin(
        "Prospecting", "Discovery", "Solution Design",
        "Proposal", "Negotiation", "Verbal Commit"
    ))

    # Calculate priority score
    # Higher score = more urgent action needed
    scored = open_opps.withColumn(
        "priority_score",
        # Health factor (lower health = higher priority)
        (100 - F.col("health_score")) * 2 +
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
        .otherwise(0)
    )

    # Determine action type based on situation
    with_action = scored.withColumn(
        "action_type",
        F.when(
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
        F.when(
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
        F.when(F.col("priority_score") >= 150, "today")
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
        F.when(
            F.col("days_since_activity") >= 10,
            "High risk of deal going cold"
        ).when(
            F.col("health_category") == "Critical",
            "Deal may slip quarter without intervention"
        ).otherwise("Maintain deal momentum").alias("risk_if_delayed"),
        # Display helpers
        F.when(F.col("action_type") == "call", "phone")
        .when(F.col("action_type") == "email", "mail")
        .when(F.col("action_type") == "meeting", "calendar")
        .otherwise("clipboard").alias("action_icon"),
        F.lit("Today 2-4pm").alias("time_display")
    ).filter(
        F.col("priority_rank") <= 10  # Top 10 actions per rep
    )

    output.write_dataframe(result)
