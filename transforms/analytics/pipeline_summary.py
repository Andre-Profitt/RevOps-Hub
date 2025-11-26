# /transforms/analytics/pipeline_summary.py
"""
ANALYTICS: Pipeline Summary
============================
Aggregates pipeline data by various dimensions for reporting.

Outputs:
- Pipeline by rep, region, segment, stage
- Coverage ratios vs quota
- Movement trends
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@transform(
    opportunities=Input("/RevOps/Enriched/opportunity_health_scored"),
    sales_reps=Input("/RevOps/Sample/sales_reps"),
    output=Output("/RevOps/Analytics/pipeline_summary")
)
def compute(ctx, opportunities, sales_reps, output):
    """
    Generate pipeline summary metrics by rep.
    """

    opps_df = opportunities.dataframe()
    reps_df = sales_reps.dataframe()

    # Filter to open opportunities only
    open_opps = opps_df.filter(F.col("is_closed") == False)

    # ===========================================
    # PIPELINE BY REP
    # ===========================================

    pipeline_by_rep = open_opps.groupBy(
        "owner_id",
        "owner_name",
        "manager_id",
        "manager_name",
        "region"
    ).agg(
        # Total pipeline
        F.sum("amount").alias("total_pipeline"),
        F.count("*").alias("deal_count"),
        F.avg("amount").alias("avg_deal_size"),

        # By forecast category
        F.sum(
            F.when(F.col("forecast_category") == "Commit", F.col("amount"))
            .otherwise(0)
        ).alias("commit_amount"),
        F.sum(
            F.when(F.col("forecast_category") == "Best Case", F.col("amount"))
            .otherwise(0)
        ).alias("best_case_amount"),
        F.sum(
            F.when(F.col("forecast_category") == "Pipeline", F.col("amount"))
            .otherwise(0)
        ).alias("pipeline_amount"),

        # By health
        F.sum(
            F.when(F.col("health_category") == "Healthy", F.col("amount"))
            .otherwise(0)
        ).alias("healthy_pipeline"),
        F.sum(
            F.when(F.col("health_category") == "Monitor", F.col("amount"))
            .otherwise(0)
        ).alias("monitor_pipeline"),
        F.sum(
            F.when(F.col("health_category").isin("At Risk", "Critical"), F.col("amount"))
            .otherwise(0)
        ).alias("at_risk_pipeline"),

        # Deal counts by health
        F.sum(F.when(F.col("health_category") == "Healthy", 1).otherwise(0)).alias("healthy_count"),
        F.sum(F.when(F.col("health_category") == "Monitor", 1).otherwise(0)).alias("monitor_count"),
        F.sum(F.when(F.col("health_category").isin("At Risk", "Critical"), 1).otherwise(0)).alias("at_risk_count"),

        # Closing this quarter
        F.sum(
            F.when(
                F.col("close_date") <= F.date_trunc("quarter", F.current_date()) + F.expr("INTERVAL 3 MONTHS"),
                F.col("amount")
            ).otherwise(0)
        ).alias("closing_this_quarter"),

        # Weighted pipeline (by probability)
        F.sum(F.col("amount") * F.col("probability")).alias("weighted_pipeline"),

        # Average health score
        F.avg("health_score").alias("avg_health_score"),

        # Average days to close
        F.avg("days_to_close").alias("avg_days_to_close"),
    )

    # ===========================================
    # JOIN WITH REP QUOTAS
    # ===========================================

    with_quota = pipeline_by_rep.join(
        reps_df.select(
            F.col("rep_id"),
            F.col("quarterly_quota"),
            F.col("ytd_closed_revenue"),
            F.col("segment").alias("rep_segment")
        ),
        pipeline_by_rep.owner_id == reps_df.rep_id,
        "left"
    )

    # ===========================================
    # CALCULATE COVERAGE RATIOS
    # ===========================================

    final = with_quota.withColumn(
        "coverage_ratio",
        F.when(
            F.col("quarterly_quota") > 0,
            F.col("total_pipeline") / F.col("quarterly_quota")
        ).otherwise(0)
    ).withColumn(
        "commit_coverage",
        F.when(
            F.col("quarterly_quota") > 0,
            F.col("commit_amount") / F.col("quarterly_quota")
        ).otherwise(0)
    ).withColumn(
        "healthy_coverage",
        F.when(
            F.col("quarterly_quota") > 0,
            F.col("healthy_pipeline") / F.col("quarterly_quota")
        ).otherwise(0)
    ).withColumn(
        "pipeline_health_pct",
        F.when(
            F.col("total_pipeline") > 0,
            F.col("healthy_pipeline") / F.col("total_pipeline")
        ).otherwise(0)
    ).withColumn(
        "at_risk_pct",
        F.when(
            F.col("total_pipeline") > 0,
            F.col("at_risk_pipeline") / F.col("total_pipeline")
        ).otherwise(0)
    )

    # Select final columns
    result = final.select(
        "owner_id",
        "owner_name",
        "manager_id",
        "manager_name",
        "region",
        "rep_segment",
        "quarterly_quota",
        "ytd_closed_revenue",
        "total_pipeline",
        "deal_count",
        "avg_deal_size",
        "commit_amount",
        "best_case_amount",
        "pipeline_amount",
        "healthy_pipeline",
        "monitor_pipeline",
        "at_risk_pipeline",
        "healthy_count",
        "monitor_count",
        "at_risk_count",
        "closing_this_quarter",
        "weighted_pipeline",
        "avg_health_score",
        "avg_days_to_close",
        "coverage_ratio",
        "commit_coverage",
        "healthy_coverage",
        "pipeline_health_pct",
        "at_risk_pct",
        F.current_timestamp().alias("calculated_at")
    )

    output.write_dataframe(result)
