# /transforms/analytics/dealdesk_analytics.py
"""
DEAL DESK ANALYTICS
===================
Tracks deal approvals, discounting patterns, and SLA compliance:
- Deal desk summary (pending approvals, SLA breaches)
- Active approval queue with urgency and status
- Discount analysis by segment

Powers the Deal Desk Dashboard.
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@transform(
    opportunities=Input("/RevOps/Scenario/opportunities"),
    output=Output("/RevOps/DealDesk/summary")
)
def compute_dealdesk_summary(ctx, opportunities, output):
    """Generate deal desk summary metrics."""

    opps_df = opportunities.dataframe()

    # Filter to deals requiring approval (negotiation stage with discount)
    approval_deals = opps_df.filter(
        (F.col("stage_name") == "Negotiation") |
        (F.col("discount_pct") > 0.15)
    )

    # Simulate approval status based on deal characteristics
    with_approval = approval_deals.withColumn(
        "approval_status",
        F.when(F.col("days_in_stage") > 10, "Pending")
        .when(F.col("discount_pct") > 0.25, "Pending")
        .otherwise("Approved")
    ).withColumn(
        "sla_breach",
        F.when(F.col("days_in_stage") > 5, True).otherwise(False)
    )

    # Calculate summary
    summary = with_approval.agg(
        F.sum(F.when(F.col("approval_status") == "Pending", 1).otherwise(0)).alias("pending_approvals"),
        F.sum(F.when(F.col("approval_status") == "Pending", F.col("amount")).otherwise(0)).alias("pending_value"),
        F.round(F.avg(F.when(F.col("approval_status") == "Pending", F.col("days_in_stage"))), 1).alias("avg_approval_time"),
        F.sum(F.when(F.col("sla_breach") == True, 1).otherwise(0)).alias("sla_breach_count"),
        F.sum(F.when((F.col("approval_status") == "Approved") & (F.col("days_in_stage") <= 1), 1).otherwise(0)).alias("approved_today"),
        F.lit(0).alias("rejected_today"),
        F.round(F.avg("discount_pct") * 100, 1).alias("avg_discount"),
        F.lit("stable").alias("discount_trend")
    ).withColumn(
        "deals_by_urgency",
        F.lit('{"high": 5, "medium": 8, "low": 3}')
    ).withColumn(
        "snapshot_date",
        F.current_date()
    )

    output.write_dataframe(summary)


@transform(
    opportunities=Input("/RevOps/Scenario/opportunities"),
    output=Output("/RevOps/DealDesk/approvals")
)
def compute_deal_approvals(ctx, opportunities, output):
    """Generate active deal approval queue."""

    opps_df = opportunities.dataframe()

    # Filter to deals in negotiation or with significant discount
    approval_queue = opps_df.filter(
        (F.col("stage_name") == "Negotiation") |
        (F.col("discount_pct") > 0.10)
    ).filter(
        ~F.col("stage_name").isin("Closed Won", "Closed Lost")
    )

    # Add approval metadata
    approvals = approval_queue.withColumn(
        "deal_id",
        F.col("opportunity_id")
    ).withColumn(
        "deal_value",
        F.col("amount")
    ).withColumn(
        "discount_amount",
        F.round(F.col("amount") * F.col("discount_pct"), 0)
    ).withColumn(
        "approval_status",
        F.when(F.col("days_in_stage") > 10, "Pending Review")
        .when(F.col("discount_pct") > 0.25, "Escalated")
        .otherwise("Auto-Approved")
    ).withColumn(
        "submitted_at",
        F.date_sub(F.current_date(), F.col("days_in_stage").cast("int"))
    ).withColumn(
        "urgency",
        F.when(F.col("amount") > 500000, "Critical")
        .when(F.col("days_in_stage") > 14, "High")
        .when(F.col("amount") > 200000, "Medium")
        .otherwise("Low")
    ).withColumn(
        "approver_name",
        F.when(F.col("amount") > 500000, "VP Sales")
        .when(F.col("discount_pct") > 0.25, "Deal Desk Manager")
        .otherwise("Sales Manager")
    ).withColumn(
        "approval_level",
        F.when(F.col("amount") > 500000, 3)
        .when(F.col("discount_pct") > 0.25, 2)
        .otherwise(1)
    ).withColumn(
        "time_in_queue",
        F.col("days_in_stage")
    ).withColumn(
        "sla_status",
        F.when(F.col("days_in_stage") > 5, "Breached")
        .when(F.col("days_in_stage") > 3, "At Risk")
        .otherwise("On Track")
    ).withColumn(
        "discount_reason",
        F.when(F.col("competitor").isNotNull(), "Competitive pressure")
        .when(F.col("amount") > 300000, "Volume discount")
        .otherwise("Standard negotiation")
    )

    output_df = approvals.select(
        "deal_id",
        "opportunity_name",
        "account_name",
        F.col("owner_name").alias("rep_name"),
        "deal_value",
        "discount_pct",
        "discount_amount",
        "approval_status",
        "submitted_at",
        "urgency",
        "approver_name",
        "approval_level",
        "time_in_queue",
        "sla_status",
        "discount_reason",
        "competitor",
        F.col("close_date")
    ).orderBy(F.col("submitted_at").desc())

    output.write_dataframe(output_df)


@transform(
    opportunities=Input("/RevOps/Scenario/opportunities"),
    output=Output("/RevOps/DealDesk/discount_analysis")
)
def compute_discount_analysis(ctx, opportunities, output):
    """Analyze discounting patterns by segment."""

    opps_df = opportunities.dataframe()

    # Filter to closed deals for win rate analysis
    closed = opps_df.filter(F.col("stage_name").isin("Closed Won", "Closed Lost"))

    # Segment analysis
    by_segment = closed.groupBy("segment").agg(
        F.count("*").alias("deal_count"),
        F.sum("amount").alias("total_value"),
        F.round(F.avg("discount_pct") * 100, 1).alias("avg_discount"),
        F.round(F.expr("percentile_approx(discount_pct, 0.5)") * 100, 1).alias("median_discount"),
        F.round(F.max("discount_pct") * 100, 1).alias("max_discount"),
        F.sum(F.when((F.col("discount_pct") >= 0) & (F.col("discount_pct") < 0.10), 1).otherwise(0)).alias("discount_0_10"),
        F.sum(F.when((F.col("discount_pct") >= 0.10) & (F.col("discount_pct") < 0.20), 1).otherwise(0)).alias("discount_10_20"),
        F.sum(F.when(F.col("discount_pct") >= 0.20, 1).otherwise(0)).alias("discount_20_plus"),
        # Win rates with/without discount
        F.round(
            F.sum(F.when((F.col("stage_name") == "Closed Won") & (F.col("discount_pct") > 0.05), 1).otherwise(0)) /
            F.sum(F.when(F.col("discount_pct") > 0.05, 1).otherwise(0)),
            3
        ).alias("win_rate_with_discount"),
        F.round(
            F.sum(F.when((F.col("stage_name") == "Closed Won") & (F.col("discount_pct") <= 0.05), 1).otherwise(0)) /
            F.sum(F.when(F.col("discount_pct") <= 0.05, 1).otherwise(0)),
            3
        ).alias("win_rate_without_discount"),
        # Cycle times
        F.round(F.avg(F.when(F.col("discount_pct") > 0.05, F.col("days_in_stage"))), 0).alias("avg_cycle_with_discount"),
        F.round(F.avg(F.when(F.col("discount_pct") <= 0.05, F.col("days_in_stage"))), 0).alias("avg_cycle_without_discount")
    ).withColumn(
        "trend",
        F.lit("stable")
    )

    output.write_dataframe(by_segment)
