# /transforms/ml/next_best_action.py
"""
NEXT-BEST-ACTION RECOMMENDATION ENGINE
======================================
AI-powered recommendations for sales reps and managers.

This module provides personalized action recommendations based on:
1. Deal health and stage progression
2. Activity patterns and engagement
3. Historical success patterns
4. Time-sensitive opportunities

Outputs:
- /RevOps/ML/rep_recommendations: Prioritized actions per rep
- /RevOps/ML/deal_recommendations: Specific actions per deal
- /RevOps/ML/account_recommendations: Account-level actions
"""

from transforms.api import transform, Input, Output
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, ArrayType
from datetime import datetime, timedelta


# =============================================================================
# ACTION TYPES AND TEMPLATES
# =============================================================================

ACTION_TYPES = {
    # Deal progression actions
    "SCHEDULE_MEETING": {
        "category": "engagement",
        "priority_base": 80,
        "template": "Schedule discovery meeting with {contact_name}",
        "impact": "high",
    },
    "FOLLOW_UP_CALL": {
        "category": "engagement",
        "priority_base": 70,
        "template": "Follow up on proposal with {contact_name}",
        "impact": "medium",
    },
    "SEND_PROPOSAL": {
        "category": "progression",
        "priority_base": 85,
        "template": "Send proposal to {account_name} - ${amount:,.0f} deal",
        "impact": "high",
    },
    "UPDATE_CLOSE_DATE": {
        "category": "hygiene",
        "priority_base": 60,
        "template": "Update close date for {opportunity_name} (currently past due)",
        "impact": "low",
    },
    "ADD_NEXT_STEP": {
        "category": "hygiene",
        "priority_base": 65,
        "template": "Add next step for {opportunity_name}",
        "impact": "low",
    },
    "MULTI_THREAD": {
        "category": "strategy",
        "priority_base": 75,
        "template": "Engage additional stakeholders at {account_name}",
        "impact": "high",
    },
    "EXECUTIVE_SPONSOR": {
        "category": "strategy",
        "priority_base": 90,
        "template": "Bring in executive sponsor for {account_name} deal",
        "impact": "high",
    },
    "COMPETITIVE_INTEL": {
        "category": "strategy",
        "priority_base": 70,
        "template": "Research competitor presence at {account_name}",
        "impact": "medium",
    },
    "REACTIVATE_STALE": {
        "category": "rescue",
        "priority_base": 85,
        "template": "Re-engage {account_name} - no activity in {days_stale} days",
        "impact": "high",
    },
    "QUARTER_END_PUSH": {
        "category": "timing",
        "priority_base": 95,
        "template": "Quarter-end close push for {opportunity_name}",
        "impact": "critical",
    },
    "RENEWAL_OUTREACH": {
        "category": "retention",
        "priority_base": 85,
        "template": "Initiate renewal conversation with {account_name}",
        "impact": "high",
    },
    "CHURN_INTERVENTION": {
        "category": "retention",
        "priority_base": 95,
        "template": "Urgent: Schedule QBR with at-risk customer {account_name}",
        "impact": "critical",
    },
}


# =============================================================================
# DEAL RECOMMENDATIONS
# =============================================================================

@transform(
    opportunities=Input("/RevOps/Staging/opportunities"),
    activities=Input("/RevOps/Staging/activities"),
    contacts=Input("/RevOps/Staging/contacts"),
    deal_scores=Input("/RevOps/ML/deal_scores"),
    hygiene_alerts=Input("/RevOps/Analytics/pipeline_hygiene_alerts"),
    output=Output("/RevOps/ML/deal_recommendations"),
)
def generate_deal_recommendations(
    ctx,
    opportunities: DataFrame,
    activities: DataFrame,
    contacts: DataFrame,
    deal_scores: DataFrame,
    hygiene_alerts: DataFrame,
    output,
):
    """
    Generate next-best-action recommendations for each open deal.
    """
    spark = ctx.spark_session
    now = datetime.utcnow()

    # Get open deals with scores
    deals = opportunities.filter(F.col("is_closed") == False)
    deals = deals.join(
        deal_scores.select(
            "opportunity_id", "win_probability", "score_band",
            "risk_factors", "engagement_score"
        ),
        "opportunity_id",
        "left"
    )

    # Get recent activity per deal
    recent_activity = activities.filter(
        F.col("opportunity_id").isNotNull()
    ).groupBy("opportunity_id").agg(
        F.max("activity_date").alias("last_activity_date"),
        F.count("*").alias("activity_count"),
        F.sum(F.when(F.col("activity_type") == "Meeting", 1).otherwise(0)).alias("meeting_count"),
    )

    deals = deals.join(recent_activity, "opportunity_id", "left")

    # Get primary contact per deal
    primary_contacts = contacts.filter(
        F.col("is_primary") == True
    ).select(
        "account_id",
        F.col("first_name").alias("contact_first_name"),
        F.col("last_name").alias("contact_last_name"),
        F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")).alias("contact_name"),
    )

    deals = deals.join(primary_contacts, "account_id", "left")

    # Get hygiene issues
    hygiene_issues = hygiene_alerts.groupBy("opportunity_id").agg(
        F.collect_set("alert_type").alias("hygiene_issues"),
    )

    deals = deals.join(hygiene_issues, "opportunity_id", "left")

    # Calculate days since activity
    deals = deals.withColumn(
        "days_since_activity",
        F.coalesce(
            F.datediff(F.lit(now), F.col("last_activity_date")),
            F.lit(999)
        )
    )

    # Calculate days to close
    deals = deals.withColumn(
        "days_to_close",
        F.datediff(F.col("close_date"), F.lit(now))
    )

    # Determine if quarter end
    deals = deals.withColumn(
        "is_quarter_end",
        F.month(F.col("close_date")).isin([3, 6, 9, 12]) &
        (F.col("days_to_close") <= 14)
    )

    # ----- Generate recommendations -----
    recommendations = []

    # Collect deals for processing
    deal_rows = deals.collect()

    for deal in deal_rows:
        deal_recs = []

        # Stale deal - needs reactivation
        if deal["days_since_activity"] and deal["days_since_activity"] > 14:
            priority = 85 + min(deal["days_since_activity"] - 14, 10)
            deal_recs.append({
                "action_type": "REACTIVATE_STALE",
                "priority": priority,
                "reason": f"No activity in {deal['days_since_activity']} days",
            })

        # No meetings scheduled
        if (deal["meeting_count"] or 0) == 0 and deal["stage_name"] not in ["Prospecting", "Closed Won", "Closed Lost"]:
            deal_recs.append({
                "action_type": "SCHEDULE_MEETING",
                "priority": 80,
                "reason": "No meetings recorded for this opportunity",
            })

        # Low engagement but high value
        if (deal["engagement_score"] or 0) < 0.3 and (deal["amount"] or 0) > 50000:
            deal_recs.append({
                "action_type": "MULTI_THREAD",
                "priority": 75,
                "reason": "Low engagement on high-value deal",
            })

        # Quarter end opportunity
        if deal["is_quarter_end"] and deal["stage_name"] in ["Proposal", "Negotiation", "Verbal Commit"]:
            deal_recs.append({
                "action_type": "QUARTER_END_PUSH",
                "priority": 95,
                "reason": "Quarter ending soon with deal in late stage",
            })

        # Large deal needs executive sponsor
        if (deal["amount"] or 0) > 100000 and deal["stage_name"] in ["Solution Design", "Proposal"]:
            deal_recs.append({
                "action_type": "EXECUTIVE_SPONSOR",
                "priority": 90,
                "reason": "Large deal may benefit from executive involvement",
            })

        # Past close date
        if (deal["days_to_close"] or 0) < 0:
            deal_recs.append({
                "action_type": "UPDATE_CLOSE_DATE",
                "priority": 60,
                "reason": f"Close date is {abs(deal['days_to_close'])} days past due",
            })

        # Hygiene issues
        if deal["hygiene_issues"]:
            if "missing_next_step" in deal["hygiene_issues"]:
                deal_recs.append({
                    "action_type": "ADD_NEXT_STEP",
                    "priority": 65,
                    "reason": "No next step defined",
                })

        # Ready to send proposal
        if deal["stage_name"] == "Solution Design" and (deal["meeting_count"] or 0) >= 2:
            deal_recs.append({
                "action_type": "SEND_PROPOSAL",
                "priority": 85,
                "reason": "Deal ready to advance to proposal stage",
            })

        # Add recommendations for this deal
        for rec in deal_recs:
            recommendations.append({
                "opportunity_id": deal["opportunity_id"],
                "opportunity_name": deal["opportunity_name"],
                "account_id": deal["account_id"],
                "account_name": deal["account_name"],
                "owner_id": deal["owner_id"],
                "amount": deal["amount"],
                "stage_name": deal["stage_name"],
                "action_type": rec["action_type"],
                "priority": rec["priority"],
                "reason": rec["reason"],
                "category": ACTION_TYPES.get(rec["action_type"], {}).get("category", "other"),
                "impact": ACTION_TYPES.get(rec["action_type"], {}).get("impact", "medium"),
                "contact_name": deal["contact_name"],
                "generated_at": now,
            })

    # Create DataFrame
    if recommendations:
        result = spark.createDataFrame(recommendations)
    else:
        # Empty result with schema
        schema = StructType([
            StructField("opportunity_id", StringType(), True),
            StructField("opportunity_name", StringType(), True),
            StructField("account_id", StringType(), True),
            StructField("account_name", StringType(), True),
            StructField("owner_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("stage_name", StringType(), True),
            StructField("action_type", StringType(), True),
            StructField("priority", IntegerType(), True),
            StructField("reason", StringType(), True),
            StructField("category", StringType(), True),
            StructField("impact", StringType(), True),
            StructField("contact_name", StringType(), True),
            StructField("generated_at", TimestampType(), True),
        ])
        result = spark.createDataFrame([], schema)

    output.write_dataframe(result)


# =============================================================================
# REP RECOMMENDATIONS
# =============================================================================

@transform(
    deal_recommendations=Input("/RevOps/ML/deal_recommendations"),
    churn_scores=Input("/RevOps/ML/churn_scores"),
    rep_performance=Input("/RevOps/Coaching/rep_performance"),
    output=Output("/RevOps/ML/rep_recommendations"),
)
def generate_rep_recommendations(
    ctx,
    deal_recommendations: DataFrame,
    churn_scores: DataFrame,
    rep_performance: DataFrame,
    output,
):
    """
    Aggregate and prioritize recommendations per rep.
    """
    spark = ctx.spark_session
    now = datetime.utcnow()

    # Get top deal recommendations per rep
    window = Window.partitionBy("owner_id").orderBy(F.desc("priority"))

    top_deal_actions = deal_recommendations.withColumn(
        "rank",
        F.row_number().over(window)
    ).filter(F.col("rank") <= 5)

    # Aggregate by rep
    rep_actions = top_deal_actions.groupBy("owner_id").agg(
        F.count("*").alias("total_actions"),
        F.sum(F.when(F.col("impact") == "critical", 1).otherwise(0)).alias("critical_actions"),
        F.sum(F.when(F.col("impact") == "high", 1).otherwise(0)).alias("high_priority_actions"),
        F.sum("amount").alias("total_amount_at_stake"),
        F.collect_list(
            F.struct(
                "opportunity_name",
                "action_type",
                "priority",
                "reason",
                "amount"
            )
        ).alias("top_actions"),
    )

    # Add churn intervention recommendations
    churn_critical = churn_scores.filter(
        F.col("churn_risk_tier").isin(["Critical", "High"])
    ).groupBy("owner_id").agg(
        F.count("*").alias("at_risk_customers"),
        F.sum("annual_revenue").alias("arr_at_risk"),
        F.collect_list(
            F.struct("account_name", "churn_probability", "recommended_action")
        ).alias("churn_actions"),
    )

    rep_actions = rep_actions.join(churn_critical, "owner_id", "left")

    # Add rep context
    if rep_performance.count() > 0:
        rep_context = rep_performance.select(
            F.col("rep_id").alias("owner_id"),
            "rep_name",
            "quota_attainment",
            "pipeline_coverage",
        )
        rep_actions = rep_actions.join(rep_context, "owner_id", "left")

    # Calculate urgency score
    rep_actions = rep_actions.withColumn(
        "urgency_score",
        F.coalesce(F.col("critical_actions"), F.lit(0)) * 3 +
        F.coalesce(F.col("high_priority_actions"), F.lit(0)) * 2 +
        F.coalesce(F.col("at_risk_customers"), F.lit(0)) * 2
    )

    # Generate summary recommendation
    rep_actions = rep_actions.withColumn(
        "summary",
        F.when(
            F.col("critical_actions") > 0,
            F.concat(
                F.lit("Focus on "),
                F.col("critical_actions"),
                F.lit(" critical deal actions")
            )
        ).when(
            F.col("at_risk_customers") > 0,
            F.concat(
                F.lit("Address "),
                F.col("at_risk_customers"),
                F.lit(" at-risk customer relationships")
            )
        ).when(
            F.col("total_actions") > 0,
            F.concat(
                F.lit("Complete "),
                F.col("total_actions"),
                F.lit(" recommended actions")
            )
        ).otherwise("No urgent actions - maintain current activities")
    )

    # Add timestamp
    result = rep_actions.withColumn("generated_at", F.lit(now))

    output.write_dataframe(result)


# =============================================================================
# ACCOUNT RECOMMENDATIONS
# =============================================================================

@transform(
    accounts=Input("/RevOps/Staging/accounts"),
    opportunities=Input("/RevOps/Staging/opportunities"),
    churn_scores=Input("/RevOps/ML/churn_scores"),
    deal_recommendations=Input("/RevOps/ML/deal_recommendations"),
    output=Output("/RevOps/ML/account_recommendations"),
)
def generate_account_recommendations(
    ctx,
    accounts: DataFrame,
    opportunities: DataFrame,
    churn_scores: DataFrame,
    deal_recommendations: DataFrame,
    output,
):
    """
    Generate account-level strategic recommendations.
    """
    spark = ctx.spark_session
    now = datetime.utcnow()

    # Get account context
    account_opps = opportunities.filter(
        F.col("is_closed") == False
    ).groupBy("account_id").agg(
        F.count("*").alias("open_opportunities"),
        F.sum("amount").alias("open_pipeline"),
        F.max("stage_name").alias("furthest_stage"),
    )

    account_data = accounts.join(account_opps, "account_id", "left")
    account_data = account_data.join(
        churn_scores.select("account_id", "churn_probability", "churn_risk_tier", "risk_factors"),
        "account_id",
        "left"
    )

    # Aggregate deal recommendations per account
    account_actions = deal_recommendations.groupBy("account_id").agg(
        F.count("*").alias("pending_actions"),
        F.max("priority").alias("max_priority"),
        F.collect_list("action_type").alias("action_types"),
    )

    account_data = account_data.join(account_actions, "account_id", "left")

    # Generate account-level recommendations
    account_data = account_data.withColumn(
        "strategic_recommendation",
        F.when(
            F.col("churn_risk_tier") == "Critical",
            "URGENT: Schedule executive business review"
        ).when(
            F.col("churn_risk_tier") == "High",
            "Schedule customer success check-in"
        ).when(
            (F.col("open_opportunities") == 0) & (F.col("segment") == "Enterprise"),
            "Identify expansion opportunities"
        ).when(
            F.col("open_pipeline") > 500000,
            "Assign executive sponsor for strategic account"
        ).when(
            F.col("pending_actions") > 3,
            "Prioritize and consolidate pending actions"
        ).otherwise("Maintain regular engagement cadence")
    )

    # Account priority score
    account_data = account_data.withColumn(
        "priority_score",
        F.coalesce(F.col("churn_probability"), F.lit(0)) * 30 +
        F.least(F.coalesce(F.col("open_pipeline"), F.lit(0)) / 100000, F.lit(50)) +
        F.coalesce(F.col("max_priority"), F.lit(0)) * 0.2
    )

    # Select output
    result = account_data.select(
        "account_id",
        "account_name",
        "segment",
        "owner_id",
        "open_opportunities",
        "open_pipeline",
        "churn_risk_tier",
        "churn_probability",
        "risk_factors",
        "pending_actions",
        "strategic_recommendation",
        "priority_score",
        F.lit(now).alias("generated_at"),
    ).orderBy(F.desc("priority_score"))

    output.write_dataframe(result)
