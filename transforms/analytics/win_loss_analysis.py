"""
Win/Loss Analysis Analytics

Deep-dive competitive intelligence, loss pattern detection,
and win factor analysis.
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@transform(
    opportunities=Input("/RevOps/Enriched/deal_health_scores"),
    output=Output("/RevOps/Analytics/win_loss_summary"),
)
def compute_win_loss_summary(opportunities, output):
    """
    Overall win/loss metrics and trends.
    """
    opps_df = opportunities.dataframe()

    # Filter to closed deals
    closed_deals = opps_df.filter(F.col("is_closed") == True)

    # Overall metrics
    summary = closed_deals.agg(
        F.count("*").alias("total_closed"),
        F.sum(F.when(F.col("is_won") == True, 1).otherwise(0)).alias("total_won"),
        F.sum(F.when(F.col("is_won") == False, 1).otherwise(0)).alias("total_lost"),
        F.sum(F.when(F.col("is_won") == True, F.col("amount")).otherwise(0)).alias("won_revenue"),
        F.sum(F.when(F.col("is_won") == False, F.col("amount")).otherwise(0)).alias("lost_revenue"),
        F.avg(F.when(F.col("is_won") == True, F.col("amount"))).alias("avg_won_deal_size"),
        F.avg(F.when(F.col("is_won") == False, F.col("amount"))).alias("avg_lost_deal_size"),
        F.avg(F.when(F.col("is_won") == True, F.col("days_to_close"))).alias("avg_won_cycle"),
        F.avg(F.when(F.col("is_won") == False, F.col("days_to_close"))).alias("avg_lost_cycle"),
    )

    # Calculate rates
    summary = summary.withColumn(
        "win_rate",
        F.round(F.col("total_won") / F.col("total_closed") * 100, 1)
    ).withColumn(
        "loss_rate",
        F.round(F.col("total_lost") / F.col("total_closed") * 100, 1)
    ).withColumn(
        "win_rate_by_value",
        F.round(F.col("won_revenue") / (F.col("won_revenue") + F.col("lost_revenue")) * 100, 1)
    ).withColumn(
        "deal_size_gap",
        F.col("avg_won_deal_size") - F.col("avg_lost_deal_size")
    ).withColumn(
        "cycle_gap",
        F.col("avg_lost_cycle") - F.col("avg_won_cycle")
    )

    output.write_dataframe(summary)


@transform(
    opportunities=Input("/RevOps/Enriched/deal_health_scores"),
    output=Output("/RevOps/Analytics/loss_reasons"),
)
def compute_loss_reasons(opportunities, output):
    """
    Analyze loss reasons and patterns.
    """
    opps_df = opportunities.dataframe()

    # Filter to lost deals
    lost_deals = opps_df.filter(
        (F.col("is_closed") == True) & (F.col("is_won") == False)
    )

    # Group by loss reason (using competitor as proxy, or stage where lost)
    by_competitor = lost_deals.groupBy("primary_competitor").agg(
        F.count("*").alias("deals_lost"),
        F.sum("amount").alias("revenue_lost"),
        F.avg("amount").alias("avg_deal_size"),
        F.avg("health_score").alias("avg_health_at_loss"),
        F.avg("days_to_close").alias("avg_cycle_days"),
    ).filter(F.col("primary_competitor").isNotNull())

    # Add loss percentage
    total_lost = lost_deals.count()
    total_lost_revenue = lost_deals.agg(F.sum("amount")).first()[0]

    result = by_competitor.withColumn(
        "loss_share_pct",
        F.round(F.col("deals_lost") / F.lit(total_lost) * 100, 1)
    ).withColumn(
        "revenue_share_pct",
        F.round(F.col("revenue_lost") / F.lit(total_lost_revenue) * 100, 1)
    ).withColumn(
        "threat_level",
        F.when(F.col("revenue_share_pct") >= 30, "Critical")
         .when(F.col("revenue_share_pct") >= 15, "High")
         .when(F.col("revenue_share_pct") >= 5, "Medium")
         .otherwise("Low")
    )

    output.write_dataframe(result)


@transform(
    opportunities=Input("/RevOps/Enriched/deal_health_scores"),
    output=Output("/RevOps/Analytics/win_factors"),
)
def compute_win_factors(opportunities, output):
    """
    Identify factors correlated with wins vs losses.
    """
    opps_df = opportunities.dataframe()
    closed_deals = opps_df.filter(F.col("is_closed") == True)

    # Calculate factor correlations
    factors = closed_deals.groupBy("is_won").agg(
        F.avg("stakeholder_count").alias("avg_stakeholders"),
        F.avg("activity_count").alias("avg_activities"),
        F.avg("health_score").alias("avg_health"),
        F.avg("days_in_stage").alias("avg_stage_duration"),
        F.avg("discount_percent").alias("avg_discount"),
        F.avg("amount").alias("avg_deal_size"),
        F.count("*").alias("deal_count"),
    )

    # Pivot to compare won vs lost
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    won = factors.filter(F.col("is_won") == True).first()
    lost = factors.filter(F.col("is_won") == False).first()

    if won and lost:
        factor_analysis = spark.createDataFrame([
            {
                "factor": "Stakeholder Count",
                "won_avg": won["avg_stakeholders"],
                "lost_avg": lost["avg_stakeholders"],
                "difference": won["avg_stakeholders"] - lost["avg_stakeholders"],
                "impact": "positive" if won["avg_stakeholders"] > lost["avg_stakeholders"] else "negative",
                "recommendation": "Increase multi-threading on deals" if won["avg_stakeholders"] > lost["avg_stakeholders"] else "Review stakeholder strategy",
            },
            {
                "factor": "Activity Volume",
                "won_avg": won["avg_activities"],
                "lost_avg": lost["avg_activities"],
                "difference": won["avg_activities"] - lost["avg_activities"],
                "impact": "positive" if won["avg_activities"] > lost["avg_activities"] else "negative",
                "recommendation": "Maintain high activity levels" if won["avg_activities"] > lost["avg_activities"] else "Increase engagement frequency",
            },
            {
                "factor": "Health Score",
                "won_avg": won["avg_health"],
                "lost_avg": lost["avg_health"],
                "difference": won["avg_health"] - lost["avg_health"],
                "impact": "positive",
                "recommendation": "Health score is predictive - monitor closely",
            },
            {
                "factor": "Discount Level",
                "won_avg": won["avg_discount"],
                "lost_avg": lost["avg_discount"],
                "difference": won["avg_discount"] - lost["avg_discount"],
                "impact": "negative" if won["avg_discount"] > lost["avg_discount"] else "neutral",
                "recommendation": "Higher discounts correlate with wins but impact margin" if won["avg_discount"] > lost["avg_discount"] else "Discounting not primary factor",
            },
            {
                "factor": "Deal Size",
                "won_avg": won["avg_deal_size"],
                "lost_avg": lost["avg_deal_size"],
                "difference": won["avg_deal_size"] - lost["avg_deal_size"],
                "impact": "neutral",
                "recommendation": "Larger deals have different dynamics - adjust strategy by size",
            },
        ])
    else:
        factor_analysis = spark.createDataFrame([{
            "factor": "No data",
            "won_avg": 0.0,
            "lost_avg": 0.0,
            "difference": 0.0,
            "impact": "neutral",
            "recommendation": "Insufficient data for analysis",
        }])

    output.write_dataframe(factor_analysis)


@transform(
    opportunities=Input("/RevOps/Enriched/deal_health_scores"),
    output=Output("/RevOps/Analytics/competitive_battles"),
)
def compute_competitive_battles(opportunities, output):
    """
    Head-to-head competitive analysis.
    """
    opps_df = opportunities.dataframe()

    # Filter to competitive deals (has competitor)
    competitive = opps_df.filter(
        F.col("primary_competitor").isNotNull() & (F.col("is_closed") == True)
    )

    # Win rate by competitor
    by_competitor = competitive.groupBy("primary_competitor").agg(
        F.count("*").alias("total_battles"),
        F.sum(F.when(F.col("is_won") == True, 1).otherwise(0)).alias("wins"),
        F.sum(F.when(F.col("is_won") == False, 1).otherwise(0)).alias("losses"),
        F.sum(F.when(F.col("is_won") == True, F.col("amount")).otherwise(0)).alias("revenue_won"),
        F.sum(F.when(F.col("is_won") == False, F.col("amount")).otherwise(0)).alias("revenue_lost"),
        F.avg("amount").alias("avg_battle_size"),
    )

    result = by_competitor.withColumn(
        "win_rate",
        F.round(F.col("wins") / F.col("total_battles") * 100, 1)
    ).withColumn(
        "loss_rate",
        F.round(F.col("losses") / F.col("total_battles") * 100, 1)
    ).withColumn(
        "net_revenue",
        F.col("revenue_won") - F.col("revenue_lost")
    ).withColumn(
        "competitive_position",
        F.when(F.col("win_rate") >= 60, "Strong")
         .when(F.col("win_rate") >= 40, "Competitive")
         .otherwise("Weak")
    ).withColumn(
        "priority",
        F.when(
            (F.col("total_battles") >= 5) & (F.col("win_rate") < 50),
            "High"
        ).when(F.col("total_battles") >= 3, "Medium").otherwise("Low")
    )

    output.write_dataframe(result)


@transform(
    opportunities=Input("/RevOps/Enriched/deal_health_scores"),
    output=Output("/RevOps/Analytics/win_loss_by_segment"),
)
def compute_win_loss_by_segment(opportunities, output):
    """
    Win/loss breakdown by segment, industry, and size.
    """
    opps_df = opportunities.dataframe()
    closed_deals = opps_df.filter(F.col("is_closed") == True)

    # By segment
    by_segment = closed_deals.groupBy("segment").agg(
        F.count("*").alias("total_deals"),
        F.sum(F.when(F.col("is_won") == True, 1).otherwise(0)).alias("wins"),
        F.sum(F.when(F.col("is_won") == False, 1).otherwise(0)).alias("losses"),
        F.sum(F.when(F.col("is_won") == True, F.col("amount")).otherwise(0)).alias("won_revenue"),
        F.sum(F.when(F.col("is_won") == False, F.col("amount")).otherwise(0)).alias("lost_revenue"),
        F.avg(F.when(F.col("is_won") == True, F.col("days_to_close"))).alias("avg_won_cycle"),
        F.avg(F.when(F.col("is_won") == False, F.col("days_to_close"))).alias("avg_lost_cycle"),
    ).withColumn("dimension", F.lit("segment")).withColumnRenamed("segment", "dimension_value")

    # By deal size band
    sized_deals = closed_deals.withColumn(
        "size_band",
        F.when(F.col("amount") >= 500000, "Enterprise ($500K+)")
         .when(F.col("amount") >= 100000, "Mid-Market ($100K-$500K)")
         .when(F.col("amount") >= 25000, "SMB ($25K-$100K)")
         .otherwise("Velocity (<$25K)")
    )

    by_size = sized_deals.groupBy("size_band").agg(
        F.count("*").alias("total_deals"),
        F.sum(F.when(F.col("is_won") == True, 1).otherwise(0)).alias("wins"),
        F.sum(F.when(F.col("is_won") == False, 1).otherwise(0)).alias("losses"),
        F.sum(F.when(F.col("is_won") == True, F.col("amount")).otherwise(0)).alias("won_revenue"),
        F.sum(F.when(F.col("is_won") == False, F.col("amount")).otherwise(0)).alias("lost_revenue"),
        F.avg(F.when(F.col("is_won") == True, F.col("days_to_close"))).alias("avg_won_cycle"),
        F.avg(F.when(F.col("is_won") == False, F.col("days_to_close"))).alias("avg_lost_cycle"),
    ).withColumn("dimension", F.lit("size_band")).withColumnRenamed("size_band", "dimension_value")

    # Union
    result = by_segment.unionByName(by_size)
    result = result.withColumn(
        "win_rate",
        F.round(F.col("wins") / F.col("total_deals") * 100, 1)
    ).withColumn(
        "revenue_win_rate",
        F.round(F.col("won_revenue") / (F.col("won_revenue") + F.col("lost_revenue")) * 100, 1)
    ).withColumn(
        "performance_tier",
        F.when(F.col("win_rate") >= 40, "Strong")
         .when(F.col("win_rate") >= 25, "Average")
         .otherwise("Needs Improvement")
    )

    output.write_dataframe(result)
