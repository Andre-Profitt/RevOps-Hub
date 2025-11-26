"""
Cross-Functional Telemetry Analytics

Tracks handoff metrics and conversion rates across Marketing, Sales, and Customer Success.
Provides visibility into funnel performance and team alignment.

Outputs:
- /RevOps/Analytics/funnel_handoff_metrics: MQL→SQL→Opp→Won conversion tracking
- /RevOps/Analytics/lead_response_analytics: Speed-to-lead and response time metrics
- /RevOps/Analytics/cross_team_activity: Activity correlation across functions
- /RevOps/Analytics/funnel_drop_analysis: Where leads fall out, by segment
- /RevOps/Analytics/telemetry_summary: Executive summary of cross-functional health
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DoubleType, IntegerType
from datetime import datetime


# =============================================================================
# 1. FUNNEL HANDOFF METRICS
# =============================================================================

@transform(
    leads=Input("/RevOps/Scenario/leads"),
    opportunities=Input("/RevOps/Scenario/opportunities"),
    accounts=Input("/RevOps/Scenario/accounts"),
    output=Output("/RevOps/Analytics/funnel_handoff_metrics")
)
def compute_funnel_handoffs(ctx, leads, opportunities, accounts, output):
    """
    Track conversion rates at each funnel stage handoff.
    MQL → SQL → Opportunity → Closed Won
    """
    leads_df = leads.dataframe()
    opps_df = opportunities.dataframe()
    accounts_df = accounts.dataframe()

    # Get current date for calculations
    current_date = datetime.now().date()

    # Lead funnel metrics
    lead_metrics = leads_df.groupBy("lead_source", "segment").agg(
        F.count("*").alias("total_leads"),
        F.sum(F.when(F.col("status") == "MQL", 1).otherwise(0)).alias("mql_count"),
        F.sum(F.when(F.col("status") == "SQL", 1).otherwise(0)).alias("sql_count"),
        F.sum(F.when(F.col("status") == "Qualified", 1).otherwise(0)).alias("qualified_count"),
        F.sum(F.when(F.col("status") == "Converted", 1).otherwise(0)).alias("converted_count"),
        F.sum(F.when(F.col("status") == "Disqualified", 1).otherwise(0)).alias("disqualified_count"),
        F.avg("lead_score").alias("avg_lead_score"),
        F.avg(
            F.when(F.col("first_response_date").isNotNull() & F.col("created_date").isNotNull(),
                   F.datediff(F.col("first_response_date"), F.col("created_date")))
        ).alias("avg_response_time_days"),
    )

    # Calculate conversion rates
    handoff_metrics = lead_metrics.withColumn(
        "mql_rate",
        F.when(F.col("total_leads") > 0,
               F.col("mql_count") / F.col("total_leads")).otherwise(0)
    ).withColumn(
        "mql_to_sql_rate",
        F.when(F.col("mql_count") > 0,
               F.col("sql_count") / F.col("mql_count")).otherwise(0)
    ).withColumn(
        "sql_to_qualified_rate",
        F.when(F.col("sql_count") > 0,
               F.col("qualified_count") / F.col("sql_count")).otherwise(0)
    ).withColumn(
        "qualified_to_converted_rate",
        F.when(F.col("qualified_count") > 0,
               F.col("converted_count") / F.col("qualified_count")).otherwise(0)
    ).withColumn(
        "overall_conversion_rate",
        F.when(F.col("total_leads") > 0,
               F.col("converted_count") / F.col("total_leads")).otherwise(0)
    ).withColumn(
        "disqualification_rate",
        F.when(F.col("total_leads") > 0,
               F.col("disqualified_count") / F.col("total_leads")).otherwise(0)
    )

    # Add benchmarks and health indicators
    result = handoff_metrics.withColumn(
        "mql_rate_health",
        F.when(F.col("mql_rate") >= 0.25, "HEALTHY")
         .when(F.col("mql_rate") >= 0.15, "MONITOR")
         .otherwise("AT_RISK")
    ).withColumn(
        "mql_to_sql_health",
        F.when(F.col("mql_to_sql_rate") >= 0.40, "HEALTHY")
         .when(F.col("mql_to_sql_rate") >= 0.25, "MONITOR")
         .otherwise("AT_RISK")
    ).withColumn(
        "response_time_health",
        F.when(F.col("avg_response_time_days") <= 1, "HEALTHY")
         .when(F.col("avg_response_time_days") <= 3, "MONITOR")
         .otherwise("AT_RISK")
    ).withColumn(
        "snapshot_date",
        F.lit(current_date)
    )

    output.write_dataframe(result)


# =============================================================================
# 2. LEAD RESPONSE ANALYTICS
# =============================================================================

@transform(
    leads=Input("/RevOps/Scenario/leads"),
    activities=Input("/RevOps/Scenario/activities"),
    users=Input("/RevOps/Reference/sales_reps"),
    output=Output("/RevOps/Analytics/lead_response_analytics")
)
def compute_lead_response(ctx, leads, activities, users, output):
    """
    Analyze speed-to-lead and first touch response times.
    """
    leads_df = leads.dataframe()
    activities_df = activities.dataframe()
    users_df = users.dataframe()

    # Find first activity for each lead
    lead_window = Window.partitionBy("lead_id").orderBy("activity_date")

    first_touches = activities_df.filter(
        F.col("activity_type").isin(["Call", "Email", "Meeting"])
    ).withColumn(
        "touch_rank",
        F.row_number().over(lead_window)
    ).filter(
        F.col("touch_rank") == 1
    ).select(
        "lead_id",
        F.col("activity_date").alias("first_touch_date"),
        F.col("activity_type").alias("first_touch_type"),
        F.col("user_id").alias("first_touch_rep")
    )

    # Join with leads
    lead_response = leads_df.join(
        first_touches,
        leads_df.lead_id == first_touches.lead_id,
        "left"
    ).drop(first_touches.lead_id)

    # Calculate response metrics
    response_metrics = lead_response.withColumn(
        "response_time_hours",
        F.when(
            F.col("first_touch_date").isNotNull() & F.col("created_date").isNotNull(),
            (F.unix_timestamp(F.col("first_touch_date")) -
             F.unix_timestamp(F.col("created_date"))) / 3600
        ).otherwise(None)
    ).withColumn(
        "response_time_bucket",
        F.when(F.col("response_time_hours").isNull(), "No Response")
         .when(F.col("response_time_hours") <= 1, "Within 1 Hour")
         .when(F.col("response_time_hours") <= 4, "1-4 Hours")
         .when(F.col("response_time_hours") <= 24, "4-24 Hours")
         .when(F.col("response_time_hours") <= 72, "1-3 Days")
         .otherwise("3+ Days")
    )

    # Aggregate by segment and source
    result = response_metrics.groupBy(
        "lead_source", "segment", "response_time_bucket"
    ).agg(
        F.count("*").alias("lead_count"),
        F.avg("response_time_hours").alias("avg_response_hours"),
        F.avg("lead_score").alias("avg_lead_score"),
        F.sum(F.when(F.col("status") == "Converted", 1).otherwise(0)).alias("converted_count"),
    ).withColumn(
        "conversion_rate",
        F.when(F.col("lead_count") > 0,
               F.col("converted_count") / F.col("lead_count")).otherwise(0)
    ).withColumn(
        "speed_impact",
        F.when(F.col("response_time_bucket") == "Within 1 Hour", "HIGH_POSITIVE")
         .when(F.col("response_time_bucket") == "1-4 Hours", "POSITIVE")
         .when(F.col("response_time_bucket") == "4-24 Hours", "NEUTRAL")
         .when(F.col("response_time_bucket") == "1-3 Days", "NEGATIVE")
         .when(F.col("response_time_bucket") == "3+ Days", "HIGH_NEGATIVE")
         .otherwise("UNKNOWN")
    )

    output.write_dataframe(result)


# =============================================================================
# 3. CROSS-TEAM ACTIVITY CORRELATION
# =============================================================================

@transform(
    opportunities=Input("/RevOps/Scenario/opportunities"),
    activities=Input("/RevOps/Scenario/activities"),
    deal_health=Input("/RevOps/Enriched/deal_health_scores"),
    output=Output("/RevOps/Analytics/cross_team_activity")
)
def compute_cross_team_activity(ctx, opportunities, activities, deal_health, output):
    """
    Track activity patterns across Marketing, Sales, and CS teams.
    Identify correlation between multi-team engagement and deal success.
    """
    opps_df = opportunities.dataframe()
    activities_df = activities.dataframe()
    health_df = deal_health.dataframe()

    # Classify activities by team function
    team_activities = activities_df.withColumn(
        "team_function",
        F.when(F.col("activity_type").isin(["Webinar", "Event", "Marketing Email"]), "MARKETING")
         .when(F.col("activity_type").isin(["Call", "Email", "Meeting", "Demo"]), "SALES")
         .when(F.col("activity_type").isin(["Onboarding", "Training", "Support"]), "CUSTOMER_SUCCESS")
         .otherwise("OTHER")
    )

    # Aggregate by opportunity
    opp_activity = team_activities.groupBy("opportunity_id").agg(
        F.sum(F.when(F.col("team_function") == "MARKETING", 1).otherwise(0)).alias("marketing_touches"),
        F.sum(F.when(F.col("team_function") == "SALES", 1).otherwise(0)).alias("sales_touches"),
        F.sum(F.when(F.col("team_function") == "CUSTOMER_SUCCESS", 1).otherwise(0)).alias("cs_touches"),
        F.count("*").alias("total_touches"),
        F.countDistinct("user_id").alias("unique_team_members"),
        F.countDistinct("team_function").alias("teams_engaged"),
    )

    # Join with opportunities and health
    cross_team = opps_df.join(
        opp_activity,
        opps_df.opportunity_id == opp_activity.opportunity_id,
        "left"
    ).join(
        health_df.select("opportunity_id", "health_score_override"),
        opps_df.opportunity_id == health_df.opportunity_id,
        "left"
    ).drop(opp_activity.opportunity_id).drop(health_df.opportunity_id)

    # Calculate engagement patterns
    result = cross_team.withColumn(
        "marketing_to_sales_ratio",
        F.when(F.col("sales_touches") > 0,
               F.col("marketing_touches") / F.col("sales_touches")).otherwise(0)
    ).withColumn(
        "multi_team_engagement",
        F.when(F.col("teams_engaged") >= 3, "FULL")
         .when(F.col("teams_engaged") == 2, "PARTIAL")
         .otherwise("SINGLE")
    ).withColumn(
        "engagement_score",
        (F.coalesce(F.col("marketing_touches"), F.lit(0)) * 0.2 +
         F.coalesce(F.col("sales_touches"), F.lit(0)) * 0.5 +
         F.coalesce(F.col("cs_touches"), F.lit(0)) * 0.3)
    ).withColumn(
        "is_won",
        F.when(F.col("stage_name") == "Closed Won", 1).otherwise(0)
    ).select(
        "opportunity_id",
        "account_name",
        "stage_name",
        "amount",
        F.coalesce(F.col("marketing_touches"), F.lit(0)).alias("marketing_touches"),
        F.coalesce(F.col("sales_touches"), F.lit(0)).alias("sales_touches"),
        F.coalesce(F.col("cs_touches"), F.lit(0)).alias("cs_touches"),
        F.coalesce(F.col("total_touches"), F.lit(0)).alias("total_touches"),
        F.coalesce(F.col("teams_engaged"), F.lit(1)).alias("teams_engaged"),
        "multi_team_engagement",
        "engagement_score",
        F.coalesce(F.col("health_score_override"), F.lit(50)).alias("health_score"),
        "is_won"
    )

    output.write_dataframe(result)


# =============================================================================
# 4. FUNNEL DROP-OFF ANALYSIS
# =============================================================================

@transform(
    leads=Input("/RevOps/Scenario/leads"),
    opportunities=Input("/RevOps/Scenario/opportunities"),
    output=Output("/RevOps/Analytics/funnel_drop_analysis")
)
def compute_funnel_drops(ctx, leads, opportunities, output):
    """
    Identify where leads drop out of the funnel by segment and source.
    """
    leads_df = leads.dataframe()
    opps_df = opportunities.dataframe()

    # Define funnel stages
    funnel_stages = [
        ("Lead Created", "total_leads"),
        ("MQL", "mql_count"),
        ("SQL", "sql_count"),
        ("Opportunity Created", "opp_count"),
        ("Qualified", "qualified_opp_count"),
        ("Proposal", "proposal_count"),
        ("Negotiation", "negotiation_count"),
        ("Closed Won", "won_count"),
    ]

    # Aggregate leads by segment and source
    lead_agg = leads_df.groupBy("segment", "lead_source").agg(
        F.count("*").alias("total_leads"),
        F.sum(F.when(F.col("status") == "MQL", 1).otherwise(0)).alias("mql_count"),
        F.sum(F.when(F.col("status") == "SQL", 1).otherwise(0)).alias("sql_count"),
        F.sum(F.when(F.col("status") == "Converted", 1).otherwise(0)).alias("converted_count"),
    )

    # Aggregate opportunities
    opp_agg = opps_df.groupBy("segment", "lead_source").agg(
        F.count("*").alias("opp_count"),
        F.sum(F.when(F.col("stage_name").isin(
            ["Solution Design", "Proposal", "Negotiation", "Closed Won"]
        ), 1).otherwise(0)).alias("qualified_opp_count"),
        F.sum(F.when(F.col("stage_name").isin(
            ["Proposal", "Negotiation", "Closed Won"]
        ), 1).otherwise(0)).alias("proposal_count"),
        F.sum(F.when(F.col("stage_name").isin(
            ["Negotiation", "Closed Won"]
        ), 1).otherwise(0)).alias("negotiation_count"),
        F.sum(F.when(F.col("stage_name") == "Closed Won", 1).otherwise(0)).alias("won_count"),
        F.sum(F.when(F.col("stage_name") == "Closed Won", F.col("amount")).otherwise(0)).alias("won_amount"),
    )

    # Join and calculate drop-offs
    funnel_data = lead_agg.join(
        opp_agg,
        ["segment", "lead_source"],
        "left"
    ).fillna(0)

    # Calculate stage-over-stage drop-offs
    result = funnel_data.withColumn(
        "drop_lead_to_mql",
        F.col("total_leads") - F.col("mql_count")
    ).withColumn(
        "drop_mql_to_sql",
        F.col("mql_count") - F.col("sql_count")
    ).withColumn(
        "drop_sql_to_opp",
        F.col("sql_count") - F.col("opp_count")
    ).withColumn(
        "drop_opp_to_qualified",
        F.col("opp_count") - F.col("qualified_opp_count")
    ).withColumn(
        "drop_qualified_to_proposal",
        F.col("qualified_opp_count") - F.col("proposal_count")
    ).withColumn(
        "drop_proposal_to_negotiation",
        F.col("proposal_count") - F.col("negotiation_count")
    ).withColumn(
        "drop_negotiation_to_won",
        F.col("negotiation_count") - F.col("won_count")
    ).withColumn(
        "overall_conversion_rate",
        F.when(F.col("total_leads") > 0,
               F.col("won_count") / F.col("total_leads")).otherwise(0)
    ).withColumn(
        "largest_drop_stage",
        F.when(
            F.greatest(
                F.col("drop_lead_to_mql"),
                F.col("drop_mql_to_sql"),
                F.col("drop_sql_to_opp"),
                F.col("drop_opp_to_qualified"),
                F.col("drop_qualified_to_proposal"),
                F.col("drop_proposal_to_negotiation"),
                F.col("drop_negotiation_to_won")
            ) == F.col("drop_lead_to_mql"), "Lead→MQL"
        ).when(
            F.greatest(
                F.col("drop_lead_to_mql"),
                F.col("drop_mql_to_sql"),
                F.col("drop_sql_to_opp"),
                F.col("drop_opp_to_qualified"),
                F.col("drop_qualified_to_proposal"),
                F.col("drop_proposal_to_negotiation"),
                F.col("drop_negotiation_to_won")
            ) == F.col("drop_mql_to_sql"), "MQL→SQL"
        ).when(
            F.greatest(
                F.col("drop_lead_to_mql"),
                F.col("drop_mql_to_sql"),
                F.col("drop_sql_to_opp"),
                F.col("drop_opp_to_qualified"),
                F.col("drop_qualified_to_proposal"),
                F.col("drop_proposal_to_negotiation"),
                F.col("drop_negotiation_to_won")
            ) == F.col("drop_sql_to_opp"), "SQL→Opp"
        ).when(
            F.greatest(
                F.col("drop_lead_to_mql"),
                F.col("drop_mql_to_sql"),
                F.col("drop_sql_to_opp"),
                F.col("drop_opp_to_qualified"),
                F.col("drop_qualified_to_proposal"),
                F.col("drop_proposal_to_negotiation"),
                F.col("drop_negotiation_to_won")
            ) == F.col("drop_opp_to_qualified"), "Opp→Qualified"
        ).when(
            F.greatest(
                F.col("drop_lead_to_mql"),
                F.col("drop_mql_to_sql"),
                F.col("drop_sql_to_opp"),
                F.col("drop_opp_to_qualified"),
                F.col("drop_qualified_to_proposal"),
                F.col("drop_proposal_to_negotiation"),
                F.col("drop_negotiation_to_won")
            ) == F.col("drop_qualified_to_proposal"), "Qualified→Proposal"
        ).when(
            F.greatest(
                F.col("drop_lead_to_mql"),
                F.col("drop_mql_to_sql"),
                F.col("drop_sql_to_opp"),
                F.col("drop_opp_to_qualified"),
                F.col("drop_qualified_to_proposal"),
                F.col("drop_proposal_to_negotiation"),
                F.col("drop_negotiation_to_won")
            ) == F.col("drop_proposal_to_negotiation"), "Proposal→Negotiation"
        ).otherwise("Negotiation→Won")
    )

    output.write_dataframe(result)


# =============================================================================
# 5. CROSS-FUNCTIONAL TELEMETRY SUMMARY
# =============================================================================

@transform(
    handoffs=Input("/RevOps/Analytics/funnel_handoff_metrics"),
    response=Input("/RevOps/Analytics/lead_response_analytics"),
    cross_team=Input("/RevOps/Analytics/cross_team_activity"),
    drops=Input("/RevOps/Analytics/funnel_drop_analysis"),
    output=Output("/RevOps/Analytics/telemetry_summary")
)
def compute_telemetry_summary(ctx, handoffs, response, cross_team, drops, output):
    """
    Executive summary of cross-functional health and alignment.
    """
    handoffs_df = handoffs.dataframe()
    response_df = response.dataframe()
    cross_team_df = cross_team.dataframe()
    drops_df = drops.dataframe()

    # Calculate overall handoff metrics
    overall_handoffs = handoffs_df.agg(
        F.sum("total_leads").alias("total_leads"),
        F.sum("mql_count").alias("total_mqls"),
        F.sum("sql_count").alias("total_sqls"),
        F.sum("converted_count").alias("total_converted"),
        F.avg("mql_to_sql_rate").alias("avg_mql_to_sql_rate"),
        F.avg("overall_conversion_rate").alias("avg_overall_conversion"),
        F.avg("avg_response_time_days").alias("avg_response_days"),
    ).collect()[0]

    # Response time distribution
    response_summary = response_df.agg(
        F.sum(F.when(F.col("response_time_bucket") == "Within 1 Hour",
                     F.col("lead_count")).otherwise(0)).alias("fast_response_count"),
        F.sum(F.when(F.col("response_time_bucket").isin(["3+ Days", "No Response"]),
                     F.col("lead_count")).otherwise(0)).alias("slow_response_count"),
        F.sum("lead_count").alias("total_with_response_data"),
    ).collect()[0]

    # Cross-team engagement
    engagement_summary = cross_team_df.agg(
        F.avg("engagement_score").alias("avg_engagement_score"),
        F.avg("total_touches").alias("avg_touches_per_deal"),
        F.sum(F.when(F.col("multi_team_engagement") == "FULL", 1).otherwise(0)).alias("full_engagement_count"),
        F.sum(F.when(F.col("is_won") == 1, 1).otherwise(0)).alias("won_deals"),
        F.count("*").alias("total_opps"),
        F.avg(F.when(F.col("multi_team_engagement") == "FULL",
                     F.col("health_score"))).alias("full_engagement_health"),
        F.avg(F.when(F.col("multi_team_engagement") == "SINGLE",
                     F.col("health_score"))).alias("single_engagement_health"),
    ).collect()[0]

    # Calculate health scores
    mql_to_sql_health = "HEALTHY" if (overall_handoffs["avg_mql_to_sql_rate"] or 0) >= 0.35 else \
                        "MONITOR" if (overall_handoffs["avg_mql_to_sql_rate"] or 0) >= 0.25 else "AT_RISK"

    response_health = "HEALTHY" if (overall_handoffs["avg_response_days"] or 99) <= 1 else \
                      "MONITOR" if (overall_handoffs["avg_response_days"] or 99) <= 3 else "AT_RISK"

    fast_pct = (response_summary["fast_response_count"] or 0) / max(response_summary["total_with_response_data"] or 1, 1)
    speed_to_lead_health = "HEALTHY" if fast_pct >= 0.5 else "MONITOR" if fast_pct >= 0.25 else "AT_RISK"

    full_engagement_pct = (engagement_summary["full_engagement_count"] or 0) / max(engagement_summary["total_opps"] or 1, 1)
    team_alignment_health = "HEALTHY" if full_engagement_pct >= 0.4 else "MONITOR" if full_engagement_pct >= 0.2 else "AT_RISK"

    # Overall cross-functional health
    health_scores = {"HEALTHY": 3, "MONITOR": 2, "AT_RISK": 1}
    overall_score = (health_scores[mql_to_sql_health] + health_scores[response_health] +
                     health_scores[speed_to_lead_health] + health_scores[team_alignment_health]) / 4
    overall_health = "HEALTHY" if overall_score >= 2.5 else "MONITOR" if overall_score >= 1.75 else "AT_RISK"

    # Determine primary bottleneck
    issues = []
    if mql_to_sql_health == "AT_RISK":
        issues.append(("MQL to SQL Conversion", overall_handoffs["avg_mql_to_sql_rate"] or 0))
    if speed_to_lead_health == "AT_RISK":
        issues.append(("Speed to Lead", fast_pct))
    if team_alignment_health == "AT_RISK":
        issues.append(("Multi-Team Engagement", full_engagement_pct))

    primary_bottleneck = issues[0][0] if issues else "None identified"

    # Build summary record
    summary = ctx.spark_session.createDataFrame([{
        # Volume metrics
        "total_leads": overall_handoffs["total_leads"] or 0,
        "total_mqls": overall_handoffs["total_mqls"] or 0,
        "total_sqls": overall_handoffs["total_sqls"] or 0,
        "total_converted": overall_handoffs["total_converted"] or 0,

        # Conversion metrics
        "mql_to_sql_rate": overall_handoffs["avg_mql_to_sql_rate"] or 0,
        "overall_conversion_rate": overall_handoffs["avg_overall_conversion"] or 0,

        # Response metrics
        "avg_response_days": overall_handoffs["avg_response_days"] or 0,
        "fast_response_pct": fast_pct,
        "slow_response_count": response_summary["slow_response_count"] or 0,

        # Engagement metrics
        "avg_engagement_score": engagement_summary["avg_engagement_score"] or 0,
        "avg_touches_per_deal": engagement_summary["avg_touches_per_deal"] or 0,
        "full_engagement_pct": full_engagement_pct,
        "full_engagement_health_avg": engagement_summary["full_engagement_health"] or 0,
        "single_engagement_health_avg": engagement_summary["single_engagement_health"] or 0,
        "health_delta": (engagement_summary["full_engagement_health"] or 0) - (engagement_summary["single_engagement_health"] or 0),

        # Health indicators
        "mql_to_sql_health": mql_to_sql_health,
        "response_health": response_health,
        "speed_to_lead_health": speed_to_lead_health,
        "team_alignment_health": team_alignment_health,
        "overall_health": overall_health,

        # Insights
        "primary_bottleneck": primary_bottleneck,
        "recommendation": _get_recommendation(primary_bottleneck),

        # Metadata
        "snapshot_date": str(datetime.now().date()),
        "snapshot_timestamp": str(datetime.now()),
    }])

    output.write_dataframe(summary)


def _get_recommendation(bottleneck: str) -> str:
    """Get actionable recommendation based on primary bottleneck."""
    recommendations = {
        "MQL to SQL Conversion": "Review lead scoring criteria and SDR qualification process. Consider tightening MQL definition or improving SDR training.",
        "Speed to Lead": "Implement automated lead routing and alerts. Set SLA for first touch within 1 hour of MQL.",
        "Multi-Team Engagement": "Establish joint account planning between Sales and CS. Create triggers for CS involvement in late-stage deals.",
        "None identified": "Funnel health is good. Focus on maintaining current performance and scaling volume.",
    }
    return recommendations.get(bottleneck, "Review funnel metrics and identify optimization opportunities.")
