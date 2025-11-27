# /transforms/enrichment/enrich_users.py
"""
USER/REP ENRICHMENT
===================
Enriches sales rep data with derived fields for capacity planning.
Used by capacity and hiring analytics.

Output:
- /RevOps/Enriched/users
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F


@transform(
    sales_reps=Input("/RevOps/Scenario/sales_reps"),
    opportunities=Input("/RevOps/Scenario/opportunities"),
    activities=Input("/RevOps/Scenario/activities"),
    output=Output("/RevOps/Enriched/users")
)
def enrich_users(ctx, sales_reps, opportunities, activities, output):
    """Enrich user/rep data with performance metrics."""

    reps = sales_reps.dataframe()
    opps = opportunities.dataframe()
    acts = activities.dataframe()

    # Calculate rep opportunity metrics
    opp_metrics = opps.groupBy("owner_id").agg(
        F.count("*").alias("total_opportunities"),
        F.sum(F.when(F.col("is_won"), F.col("amount")).otherwise(0)).alias("closed_won_ytd"),
        F.sum(F.when(~F.col("is_closed"), F.col("amount")).otherwise(0)).alias("open_pipeline"),
        F.sum(F.when(F.col("is_won"), 1).otherwise(0)).alias("deals_won"),
        F.sum(F.when(F.col("is_closed") & ~F.col("is_won"), 1).otherwise(0)).alias("deals_lost"),
        F.avg(F.when(F.col("is_won"), F.col("amount"))).alias("avg_deal_size"),
        F.avg(F.when(F.col("is_closed"), F.col("days_to_close"))).alias("avg_sales_cycle"),
    )

    # Calculate win rate
    opp_metrics_enriched = opp_metrics.withColumn(
        "win_rate",
        F.when(
            (F.col("deals_won") + F.col("deals_lost")) > 0,
            F.round(F.col("deals_won") / (F.col("deals_won") + F.col("deals_lost")) * 100, 1)
        ).otherwise(F.lit(None))
    )

    # Calculate activity metrics
    activity_metrics = acts.groupBy("owner_id").agg(
        F.count("*").alias("total_activities_30d"),
        F.countDistinct("activity_type").alias("activity_types_used"),
        F.max("activity_date").alias("last_activity_date"),
    )

    # Join metrics with reps
    with_opp_metrics = reps.join(
        opp_metrics_enriched,
        reps.rep_id == opp_metrics_enriched.owner_id,
        "left"
    ).drop("owner_id")

    with_activity_metrics = with_opp_metrics.join(
        activity_metrics,
        with_opp_metrics.rep_id == activity_metrics.owner_id,
        "left"
    ).drop("owner_id")

    # Add derived fields
    final = with_activity_metrics.withColumn(
        "quota_attainment",
        F.when(
            F.col("quota") > 0,
            F.round(F.coalesce(F.col("closed_won_ytd"), F.lit(0)) / F.col("quota") * 100, 1)
        ).otherwise(F.lit(None))
    ).withColumn(
        "performance_tier",
        F.when(F.coalesce(F.col("quota_attainment"), F.lit(0)) >= 120, "Top Performer")
        .when(F.coalesce(F.col("quota_attainment"), F.lit(0)) >= 100, "On Target")
        .when(F.coalesce(F.col("quota_attainment"), F.lit(0)) >= 80, "Developing")
        .otherwise("Needs Attention")
    ).withColumn(
        "capacity_utilization",
        F.when(
            F.col("max_deals") > 0,
            F.round(F.coalesce(F.col("total_opportunities"), F.lit(0)) / F.col("max_deals") * 100, 1)
        ).otherwise(F.lit(None))
    ).withColumn(
        "has_capacity",
        F.coalesce(F.col("capacity_utilization"), F.lit(100)) < 90
    ).withColumn(
        "tenure_months",
        F.months_between(F.current_date(), F.col("start_date"))
    ).withColumn(
        "is_ramping",
        F.col("tenure_months") < 6
    ).withColumn(
        "ramp_progress",
        F.when(
            F.col("is_ramping"),
            F.round(F.col("tenure_months") / 6 * 100, 0)
        ).otherwise(100)
    ).withColumn(
        "user_type", F.lit("Sales Rep")
    ).withColumn(
        "is_active", F.lit(True)
    ).withColumn(
        "_enriched_at",
        F.current_timestamp()
    )

    output.write_dataframe(final)
