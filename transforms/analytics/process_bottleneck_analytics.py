# /transforms/analytics/process_bottleneck_analytics.py
"""
PROCESS BOTTLENECK ANALYTICS
============================
Analyzes the sales process event logs to identify:
- Stage duration vs benchmarks
- Bottleneck stages
- Rework patterns
- Revenue impact of delays

This powers the Process Analyzer agent.
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@transform(
    events=Input("/RevOps/Scenario/process_events"),
    output=Output("/RevOps/Analytics/process_bottlenecks")
)
def compute(ctx, events, output):
    """Analyze process events to identify bottlenecks."""

    df = events.dataframe()

    # ===========================================
    # STAGE-LEVEL ANALYSIS
    # ===========================================

    stage_analysis = df.filter(
        ~F.col("activity").isin("Lead Created", "Closed Won", "Closed Lost")
    ).groupBy("activity").agg(
        F.count("*").alias("total_occurrences"),
        F.round(F.avg("duration_days"), 1).alias("avg_duration"),
        F.round(F.stddev("duration_days"), 1).alias("stddev_duration"),
        F.min("duration_days").alias("min_duration"),
        F.max("duration_days").alias("max_duration"),
        F.first("benchmark_days").alias("benchmark_days"),
        F.round(F.avg("deviation_days"), 1).alias("avg_deviation"),
        F.sum(F.when(F.col("is_bottleneck") == True, 1).otherwise(0)).alias("bottleneck_count"),
        F.sum(F.when(F.col("is_rework") == True, 1).otherwise(0)).alias("rework_count"),
        F.sum(F.when(F.col("is_bottleneck") == True, F.col("amount")).otherwise(0)).alias("bottleneck_revenue")
    )

    # ===========================================
    # CALCULATE SEVERITY METRICS
    # ===========================================

    with_severity = stage_analysis.withColumn(
        "delay_ratio",
        F.round(F.col("avg_duration") / F.col("benchmark_days"), 2)
    ).withColumn(
        "bottleneck_rate",
        F.round(F.col("bottleneck_count") * 100.0 / F.col("total_occurrences"), 1)
    ).withColumn(
        "severity",
        F.when(F.col("delay_ratio") >= 2.5, "CRITICAL")
        .when(F.col("delay_ratio") >= 2.0, "HIGH")
        .when(F.col("delay_ratio") >= 1.5, "MEDIUM")
        .when(F.col("delay_ratio") >= 1.2, "LOW")
        .otherwise("ON TARGET")
    ).withColumn(
        "days_lost_per_deal",
        F.greatest(F.col("avg_deviation"), F.lit(0))
    ).withColumn(
        "estimated_revenue_impact",
        # Assume 1% of pipeline lost per day of delay
        F.round(F.col("bottleneck_revenue") * F.col("days_lost_per_deal") * 0.01, 0)
    )

    # ===========================================
    # ADD RECOMMENDATIONS
    # ===========================================

    final = with_severity.withColumn(
        "root_cause_hypothesis",
        F.when(
            F.col("activity") == "Legal Review",
            "Legal team capacity insufficient; complex custom terms increasing review time"
        ).when(
            F.col("activity") == "Security Review",
            "Enterprise security requirements creating backlog; limited security resources"
        ).when(
            F.col("activity") == "Procurement",
            "Customer procurement processes vary; limited visibility into requirements"
        ).when(
            F.col("activity") == "Solution Design",
            "Technical complexity; may need more SE resources or clearer scoping"
        ).when(
            F.col("activity") == "Negotiation",
            "Pricing authority unclear; multiple rounds of approvals required"
        ).otherwise("Process variance - investigate specific cases")
    ).withColumn(
        "recommended_action",
        F.when(
            F.col("activity") == "Legal Review",
            "1) Create standard terms template (80% of cases). 2) Add legal headcount or contractor. 3) Implement 5-day SLA with escalation."
        ).when(
            F.col("activity") == "Security Review",
            "1) Pre-populate security questionnaires. 2) Create security package for common questions. 3) Dedicated enterprise security resource."
        ).when(
            F.col("activity") == "Procurement",
            "1) Earlier procurement engagement. 2) Standard procurement guide for customers. 3) Multi-thread to procurement early."
        ).when(
            F.col("activity") == "Solution Design",
            "1) Improve scoping in Discovery. 2) SE capacity planning. 3) Templatized solution designs for common use cases."
        ).when(
            F.col("activity") == "Negotiation",
            "1) Clear pricing authority matrix. 2) Pre-approved discount tiers. 3) Executive sponsor engagement earlier."
        ).otherwise("Review individual cases for patterns")
    ).withColumn(
        "priority_score",
        F.round(
            F.col("bottleneck_rate") * 0.4 +
            F.col("delay_ratio") * 20 +
            F.log10(F.col("bottleneck_revenue") + 1) * 5,
            0
        )
    ).orderBy(F.col("priority_score").desc())

    output.write_dataframe(final)


@transform(
    events=Input("/RevOps/Scenario/process_events"),
    opportunities=Input("/RevOps/Scenario/opportunities"),
    output=Output("/RevOps/Analytics/deals_stuck_in_process")
)
def deals_stuck(ctx, events, opportunities, output):
    """Identify deals currently stuck in bottleneck stages."""

    events_df = events.dataframe()
    opps_df = opportunities.dataframe()

    # Get latest event for each deal
    window = Window.partitionBy("case_id").orderBy(F.col("timestamp").desc())

    latest_events = events_df.withColumn(
        "row_num", F.row_number().over(window)
    ).filter(F.col("row_num") == 1).drop("row_num")

    # Join with open opportunities
    open_opps = opps_df.filter(F.col("is_closed") == False)

    stuck_deals = open_opps.join(
        latest_events,
        open_opps.opportunity_id == latest_events.case_id,
        "inner"
    ).filter(
        (F.col("is_bottleneck") == True) |
        (F.col("deviation_days") > 5)
    ).select(
        opps_df.opportunity_id,
        opps_df.account_name,
        opps_df.opportunity_name,
        opps_df.amount,
        opps_df.owner_name,
        opps_df.stage_name,
        latest_events.activity.alias("stuck_activity"),
        latest_events.duration_days,
        latest_events.benchmark_days,
        latest_events.deviation_days,
        latest_events.is_bottleneck,
        opps_df.health_score_override.alias("health_score"),
        opps_df.close_date
    ).withColumn(
        "urgency",
        F.when(
            (F.datediff(F.col("close_date"), F.current_date()) < 30) &
            (F.col("deviation_days") > 7),
            "CRITICAL"
        ).when(
            F.col("deviation_days") > 10,
            "HIGH"
        ).when(
            F.col("deviation_days") > 5,
            "MEDIUM"
        ).otherwise("LOW")
    ).withColumn(
        "days_until_close",
        F.datediff(F.col("close_date"), F.to_date(F.lit("2024-11-15")))
    ).withColumn(
        "risk_of_slip",
        F.when(
            F.col("days_until_close") < F.col("deviation_days") * 2,
            "HIGH - Likely to slip quarter"
        ).when(
            F.col("days_until_close") < F.col("deviation_days") * 3,
            "MEDIUM - At risk of slipping"
        ).otherwise("LOW - Buffer exists")
    ).orderBy(F.col("amount").desc())

    output.write_dataframe(stuck_deals)


@transform(
    events=Input("/RevOps/Scenario/process_events"),
    output=Output("/RevOps/Analytics/rework_analysis")
)
def rework_analysis(ctx, events, output):
    """Analyze deals that had rework (went backwards in process)."""

    df = events.dataframe()

    rework_events = df.filter(F.col("is_rework") == True)

    # Aggregate rework by rep and stage
    rework_summary = rework_events.groupBy("resource", "activity").agg(
        F.count("*").alias("rework_count"),
        F.sum("amount").alias("total_amount_affected"),
        F.countDistinct("case_id").alias("unique_deals")
    ).withColumn(
        "avg_rework_per_deal",
        F.round(F.col("rework_count") / F.col("unique_deals"), 1)
    ).withColumn(
        "rework_cause_hypothesis",
        F.when(
            F.col("activity") == "Proposal",
            "Insufficient discovery - requirements changed after proposal"
        ).when(
            F.col("activity") == "Discovery",
            "New stakeholders introduced - rediscovery required"
        ).when(
            F.col("activity") == "Solution Design",
            "Technical requirements evolved - redesign needed"
        ).otherwise("Process variance - review individual cases")
    ).orderBy(F.col("rework_count").desc())

    output.write_dataframe(rework_summary)
