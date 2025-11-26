# /transforms/analytics/pipeline_health_summary.py
"""
PIPELINE HEALTH SUMMARY
=======================
Aggregates deal health scores into actionable summary views:
- By forecast category
- By stage
- By rep/manager
- Overall health distribution

This is what powers the executive dashboard view.
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


@transform(
    deal_health=Input("/RevOps/Enriched/deal_health_scores"),
    output=Output("/RevOps/Analytics/pipeline_health_summary")
)
def compute(ctx, deal_health, output):
    """Generate pipeline health summary metrics."""

    df = deal_health.dataframe()

    # Filter to open opportunities
    open_opps = df.filter(F.col("stage_name").isin(
        "Prospecting", "Discovery", "Solution Design",
        "Proposal", "Negotiation", "Verbal Commit"
    ))

    # ===========================================
    # SUMMARY BY FORECAST CATEGORY
    # ===========================================

    by_forecast = open_opps.groupBy("forecast_category").agg(
        F.count("*").alias("deal_count"),
        F.sum("amount").alias("total_amount"),
        F.round(F.avg("health_score"), 1).alias("avg_health"),
        F.sum(F.when(F.col("health_category") == "Healthy", F.col("amount")).otherwise(0)).alias("healthy_amount"),
        F.sum(F.when(F.col("health_category") == "Monitor", F.col("amount")).otherwise(0)).alias("monitor_amount"),
        F.sum(F.when(F.col("health_category") == "At Risk", F.col("amount")).otherwise(0)).alias("at_risk_amount"),
        F.sum(F.when(F.col("health_category") == "Critical", F.col("amount")).otherwise(0)).alias("critical_amount"),
        F.count(F.when(F.col("needs_attention") == True, 1)).alias("deals_needing_attention")
    ).withColumn("view_type", F.lit("by_forecast_category"))

    # ===========================================
    # SUMMARY BY STAGE
    # ===========================================

    by_stage = open_opps.groupBy("stage_name").agg(
        F.count("*").alias("deal_count"),
        F.sum("amount").alias("total_amount"),
        F.round(F.avg("health_score"), 1).alias("avg_health"),
        F.sum(F.when(F.col("health_category") == "Healthy", F.col("amount")).otherwise(0)).alias("healthy_amount"),
        F.sum(F.when(F.col("health_category") == "Monitor", F.col("amount")).otherwise(0)).alias("monitor_amount"),
        F.sum(F.when(F.col("health_category") == "At Risk", F.col("amount")).otherwise(0)).alias("at_risk_amount"),
        F.sum(F.when(F.col("health_category") == "Critical", F.col("amount")).otherwise(0)).alias("critical_amount"),
        F.count(F.when(F.col("needs_attention") == True, 1)).alias("deals_needing_attention")
    ).withColumn("view_type", F.lit("by_stage"))

    # Rename column for union
    by_stage = by_stage.withColumnRenamed("stage_name", "forecast_category")

    # ===========================================
    # OVERALL SUMMARY
    # ===========================================

    overall = open_opps.agg(
        F.count("*").alias("deal_count"),
        F.sum("amount").alias("total_amount"),
        F.round(F.avg("health_score"), 1).alias("avg_health"),
        F.sum(F.when(F.col("health_category") == "Healthy", F.col("amount")).otherwise(0)).alias("healthy_amount"),
        F.sum(F.when(F.col("health_category") == "Monitor", F.col("amount")).otherwise(0)).alias("monitor_amount"),
        F.sum(F.when(F.col("health_category") == "At Risk", F.col("amount")).otherwise(0)).alias("at_risk_amount"),
        F.sum(F.when(F.col("health_category") == "Critical", F.col("amount")).otherwise(0)).alias("critical_amount"),
        F.count(F.when(F.col("needs_attention") == True, 1)).alias("deals_needing_attention")
    ).withColumn(
        "forecast_category", F.lit("TOTAL")
    ).withColumn(
        "view_type", F.lit("overall")
    )

    # ===========================================
    # COMBINE ALL VIEWS
    # ===========================================

    # Add calculated percentages
    combined = by_forecast.union(by_stage).union(overall).withColumn(
        "healthy_pct",
        F.round(F.col("healthy_amount") * 100 / F.col("total_amount"), 1)
    ).withColumn(
        "at_risk_pct",
        F.round((F.col("at_risk_amount") + F.col("critical_amount")) * 100 / F.col("total_amount"), 1)
    ).withColumn(
        "health_weighted_amount",
        F.round(
            F.col("healthy_amount") * 1.0 +
            F.col("monitor_amount") * 0.85 +
            F.col("at_risk_amount") * 0.60 +
            F.col("critical_amount") * 0.30,
            0
        )
    ).withColumn(
        "confidence_factor",
        F.round(F.col("health_weighted_amount") / F.col("total_amount"), 2)
    )

    output.write_dataframe(combined)
