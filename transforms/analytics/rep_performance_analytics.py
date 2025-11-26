# /transforms/analytics/rep_performance_analytics.py
"""
REP PERFORMANCE ANALYTICS
=========================
Analyzes sales rep performance to identify:
- Top performer patterns
- Coaching opportunities
- Win rate drivers
- Multi-threading effectiveness

This powers the Rep Coaching Assistant agent.
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@transform(
    reps=Input("/RevOps/Scenario/sales_reps"),
    opportunities=Input("/RevOps/Scenario/opportunities"),
    output=Output("/RevOps/Analytics/rep_performance")
)
def compute(ctx, reps, opportunities, output):
    """Generate rep performance analytics with pattern analysis."""

    reps_df = reps.dataframe()
    opps_df = opportunities.dataframe()

    # ===========================================
    # CALCULATE LIVE METRICS FROM OPPORTUNITIES
    # ===========================================

    # Win rate and stakeholder analysis by rep
    rep_metrics = opps_df.filter(
        F.col("stage_name").isin("Closed Won", "Closed Lost")
    ).groupBy("owner_id", "owner_name").agg(
        F.count("*").alias("total_closed_deals"),
        F.sum(F.when(F.col("stage_name") == "Closed Won", 1).otherwise(0)).alias("won_deals"),
        F.sum(F.when(F.col("stage_name") == "Closed Lost", 1).otherwise(0)).alias("lost_deals"),
        F.sum(F.when(F.col("stage_name") == "Closed Won", F.col("amount")).otherwise(0)).alias("won_revenue"),
        F.round(F.avg("stakeholder_count"), 1).alias("avg_stakeholders_all"),
        F.round(
            F.avg(F.when(F.col("stage_name") == "Closed Won", F.col("stakeholder_count"))),
            1
        ).alias("avg_stakeholders_won"),
        F.round(
            F.avg(F.when(F.col("stage_name") == "Closed Lost", F.col("stakeholder_count"))),
            1
        ).alias("avg_stakeholders_lost")
    ).withColumn(
        "calculated_win_rate",
        F.round(F.col("won_deals") / F.col("total_closed_deals"), 2)
    )

    # ===========================================
    # JOIN WITH REP DATA
    # ===========================================

    rep_analysis = reps_df.join(
        rep_metrics,
        reps_df.rep_id == rep_metrics.owner_id,
        "left"
    )

    # ===========================================
    # CALCULATE RANKINGS
    # ===========================================

    window_all = Window.orderBy(F.col("win_rate").desc())
    window_segment = Window.partitionBy("segment").orderBy(F.col("win_rate").desc())

    ranked = rep_analysis.withColumn(
        "overall_rank",
        F.rank().over(window_all)
    ).withColumn(
        "segment_rank",
        F.rank().over(window_segment)
    )

    # ===========================================
    # CALCULATE TEAM AVERAGES FOR COMPARISON
    # ===========================================

    team_avgs = reps_df.agg(
        F.round(F.avg("win_rate"), 3).alias("team_avg_win_rate"),
        F.round(F.avg("avg_stakeholders"), 1).alias("team_avg_stakeholders"),
        F.round(F.avg("avg_sales_cycle_days"), 0).alias("team_avg_cycle"),
        F.round(F.avg("ytd_attainment"), 3).alias("team_avg_attainment")
    )

    # Cross join to add team averages to each rep
    with_comparison = ranked.crossJoin(team_avgs)

    # ===========================================
    # IDENTIFY COACHING NEEDS
    # ===========================================

    final = with_comparison.withColumn(
        "win_rate_vs_team",
        F.round(F.col("win_rate") - F.col("team_avg_win_rate"), 3)
    ).withColumn(
        "stakeholders_vs_team",
        F.round(F.col("avg_stakeholders") - F.col("team_avg_stakeholders"), 1)
    ).withColumn(
        "cycle_vs_team",
        F.round(F.col("avg_sales_cycle_days") - F.col("team_avg_cycle"), 0)
    ).withColumn(
        "performance_tier",
        F.when(F.col("ytd_attainment") >= 1.1, "Top Performer")
        .when(F.col("ytd_attainment") >= 0.9, "On Track")
        .when(F.col("ytd_attainment") >= 0.7, "Needs Improvement")
        .otherwise("At Risk")
    ).withColumn(
        "primary_coaching_need",
        F.when(
            F.col("avg_stakeholders") < 2.0,
            "Multi-threading - Avg stakeholders below 2.0"
        ).when(
            F.col("win_rate") < 0.20,
            "Qualification - Win rate below 20%"
        ).when(
            F.col("avg_sales_cycle_days") > F.col("team_avg_cycle") * 1.3,
            "Deal velocity - Cycles 30%+ longer than team"
        ).when(
            F.col("current_pipeline") < F.col("quarterly_quota") * 2,
            "Pipeline building - Below 2x coverage"
        ).otherwise("No critical gaps")
    ).withColumn(
        "recommended_action",
        F.when(
            F.col("avg_stakeholders") < 2.0,
            F.concat(
                F.lit("Shadow top performer (stakeholder avg: "),
                F.lit("4.2"), F.lit(") to learn multi-threading")
            )
        ).when(
            F.col("win_rate") < 0.20,
            "Review qualification criteria; inspect recent losses"
        ).when(
            F.col("avg_sales_cycle_days") > F.col("team_avg_cycle") * 1.3,
            "Analyze stuck deals; identify common stall points"
        ).when(
            F.col("current_pipeline") < F.col("quarterly_quota") * 2,
            "Focus on prospecting; increase outbound activity"
        ).otherwise("Continue current approach")
    )

    # Select output columns
    output_df = final.select(
        "rep_id",
        "rep_name",
        "email",
        "manager_name",
        "region",
        "segment",
        "role",
        "tenure_months",
        # Performance metrics
        "ytd_attainment",
        "win_rate",
        "avg_stakeholders",
        "avg_sales_cycle_days",
        "current_pipeline",
        "pipeline_coverage",
        "deals_in_pipeline",
        "activities_this_month",
        # Live calculated metrics
        "calculated_win_rate",
        "avg_stakeholders_won",
        "avg_stakeholders_lost",
        "won_deals",
        "lost_deals",
        "won_revenue",
        # Rankings
        "overall_rank",
        "segment_rank",
        # Vs Team
        "team_avg_win_rate",
        "team_avg_stakeholders",
        "win_rate_vs_team",
        "stakeholders_vs_team",
        "cycle_vs_team",
        # Coaching
        "performance_tier",
        "primary_coaching_need",
        "recommended_action",
        "story"
    )

    output.write_dataframe(output_df)


@transform(
    reps=Input("/RevOps/Scenario/sales_reps"),
    output=Output("/RevOps/Analytics/top_performer_patterns")
)
def extract_patterns(ctx, reps, output):
    """Extract patterns from top performers to guide coaching."""

    df = reps.dataframe()

    # Define top performer threshold
    top_performers = df.filter(F.col("ytd_attainment") >= 1.0)
    bottom_performers = df.filter(F.col("ytd_attainment") < 0.70)

    # Calculate averages for each group
    top_avgs = top_performers.agg(
        F.lit("Top Performers (100%+ attainment)").alias("group"),
        F.count("*").alias("count"),
        F.round(F.avg("win_rate"), 3).alias("avg_win_rate"),
        F.round(F.avg("avg_stakeholders"), 2).alias("avg_stakeholders"),
        F.round(F.avg("avg_sales_cycle_days"), 0).alias("avg_cycle_days"),
        F.round(F.avg("activities_this_month"), 0).alias("avg_activities"),
        F.round(F.avg("current_pipeline"), 0).alias("avg_pipeline")
    )

    bottom_avgs = bottom_performers.agg(
        F.lit("Struggling (<70% attainment)").alias("group"),
        F.count("*").alias("count"),
        F.round(F.avg("win_rate"), 3).alias("avg_win_rate"),
        F.round(F.avg("avg_stakeholders"), 2).alias("avg_stakeholders"),
        F.round(F.avg("avg_sales_cycle_days"), 0).alias("avg_cycle_days"),
        F.round(F.avg("activities_this_month"), 0).alias("avg_activities"),
        F.round(F.avg("current_pipeline"), 0).alias("avg_pipeline")
    )

    team_avgs = df.agg(
        F.lit("Team Average").alias("group"),
        F.count("*").alias("count"),
        F.round(F.avg("win_rate"), 3).alias("avg_win_rate"),
        F.round(F.avg("avg_stakeholders"), 2).alias("avg_stakeholders"),
        F.round(F.avg("avg_sales_cycle_days"), 0).alias("avg_cycle_days"),
        F.round(F.avg("activities_this_month"), 0).alias("avg_activities"),
        F.round(F.avg("current_pipeline"), 0).alias("avg_pipeline")
    )

    # Union results
    patterns = top_avgs.union(team_avgs).union(bottom_avgs)

    # Add insights
    final = patterns.withColumn(
        "key_insight",
        F.when(
            F.col("group") == "Top Performers (100%+ attainment)",
            "Multi-threading drives wins: 4.0+ stakeholders correlates with higher win rates"
        ).when(
            F.col("group") == "Struggling (<70% attainment)",
            "Single-threaded approach failing: <2 stakeholders leads to champion dependency"
        ).otherwise("Benchmark for comparison")
    )

    output.write_dataframe(final)
