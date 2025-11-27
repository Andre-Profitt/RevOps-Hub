# /transforms/analytics/coaching_analytics.py
"""
REP COACHING ANALYTICS
======================
Generates comprehensive coaching data for individual rep performance views:
- Rep performance summary (vs team benchmarks)
- Driver comparisons (what metrics impact quota)
- Activity metrics (call, email, meeting patterns)
- Next best actions (AI-recommended actions per deal)
- Coaching insights (strengths, improvements, actions)
- Rep roster (for dropdown selectors)

Powers the Rep Coaching Dashboard.
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StringType


@transform(
    reps=Input("/RevOps/Scenario/sales_reps"),
    opportunities=Input("/RevOps/Scenario/opportunities"),
    output=Output("/RevOps/Coaching/rep_performance")
)
def compute_rep_performance(ctx, reps, opportunities, output):
    """Generate detailed rep performance metrics for coaching view."""

    reps_df = reps.dataframe()
    opps_df = opportunities.dataframe()

    # Calculate closed deal metrics per rep
    rep_closed = opps_df.filter(
        F.col("stage_name").isin("Closed Won", "Closed Lost")
    ).groupBy("owner_id", "owner_name").agg(
        F.count("*").alias("total_closed"),
        F.sum(F.when(F.col("stage_name") == "Closed Won", 1).otherwise(0)).alias("won_count"),
        F.sum(F.when(F.col("stage_name") == "Closed Won", F.col("amount")).otherwise(0)).alias("closed_won_amount"),
        F.round(F.avg(F.when(F.col("stage_name") == "Closed Won", F.col("amount"))), 0).alias("avg_deal_size"),
        F.round(F.avg(F.when(F.col("stage_name") == "Closed Won", F.col("days_in_stage"))), 0).alias("avg_sales_cycle")
    ).withColumn(
        "live_win_rate",
        F.round(F.col("won_count") / F.col("total_closed"), 3)
    )

    # Calculate team averages
    team_avgs = reps_df.agg(
        F.round(F.avg("win_rate"), 3).alias("team_avg_win_rate"),
        F.round(F.avg("avg_deal_size"), 0).alias("team_avg_deal_size"),
        F.round(F.avg("avg_sales_cycle_days"), 0).alias("team_avg_cycle")
    )

    # Calculate rankings
    window_rank = Window.orderBy(F.col("ytd_attainment").desc())
    total_reps = reps_df.count()

    # Join all data
    rep_perf = reps_df.join(
        rep_closed,
        reps_df.rep_id == rep_closed.owner_id,
        "left"
    ).crossJoin(team_avgs).withColumn(
        "overall_rank",
        F.rank().over(window_rank)
    ).withColumn(
        "total_reps",
        F.lit(total_reps)
    ).withColumn(
        "rank_change",
        F.lit(0)  # Would require historical data
    ).withColumn(
        "performance_tier",
        F.when(F.col("ytd_attainment") >= 1.1, "Top Performer")
        .when(F.col("ytd_attainment") >= 0.9, "On Track")
        .when(F.col("ytd_attainment") >= 0.7, "Needs Coaching")
        .otherwise("At Risk")
    )

    # Select output columns matching webapp expectations
    output_df = rep_perf.select(
        F.col("rep_id"),
        F.col("rep_name"),
        F.col("manager_name"),
        F.col("region"),
        F.col("segment"),
        F.col("ytd_attainment"),
        F.col("win_rate"),
        F.coalesce(F.col("avg_deal_size"), F.lit(0)).alias("avg_deal_size"),
        F.col("avg_sales_cycle_days").alias("avg_sales_cycle"),
        F.col("overall_rank"),
        F.col("total_reps"),
        F.col("rank_change"),
        F.col("team_avg_win_rate"),
        F.col("team_avg_deal_size"),
        F.col("team_avg_cycle"),
        F.col("performance_tier")
    )

    output.write_dataframe(output_df)


@transform(
    reps=Input("/RevOps/Scenario/sales_reps"),
    output=Output("/RevOps/Coaching/driver_comparisons")
)
def compute_driver_comparisons(ctx, reps, output):
    """Generate driver comparison data showing rep vs team vs top performer."""

    df = reps.dataframe()

    # Calculate team and top performer benchmarks
    team_avgs = df.agg(
        F.round(F.avg("win_rate"), 3).alias("team_win_rate"),
        F.round(F.avg("avg_stakeholders"), 1).alias("team_stakeholders"),
        F.round(F.avg("avg_sales_cycle_days"), 0).alias("team_cycle"),
        F.round(F.avg("pipeline_coverage"), 1).alias("team_coverage"),
        F.round(F.avg("activities_this_month"), 0).alias("team_activities")
    ).collect()[0]

    top_performer = df.filter(F.col("ytd_attainment") >= 1.0).agg(
        F.round(F.avg("win_rate"), 3).alias("top_win_rate"),
        F.round(F.avg("avg_stakeholders"), 1).alias("top_stakeholders"),
        F.round(F.avg("avg_sales_cycle_days"), 0).alias("top_cycle"),
        F.round(F.avg("pipeline_coverage"), 1).alias("top_coverage"),
        F.round(F.avg("activities_this_month"), 0).alias("top_activities")
    ).collect()[0]

    # Create driver metrics for each rep
    metrics = [
        ("Win Rate", "win_rate", team_avgs["team_win_rate"], top_performer["top_win_rate"], 0.15),
        ("Avg Stakeholders", "avg_stakeholders", team_avgs["team_stakeholders"], top_performer["top_stakeholders"], 0.10),
        ("Sales Cycle (days)", "avg_sales_cycle_days", team_avgs["team_cycle"], top_performer["top_cycle"], -0.08),
        ("Pipeline Coverage", "pipeline_coverage", team_avgs["team_coverage"], top_performer["top_coverage"], 0.12),
        ("Monthly Activities", "activities_this_month", team_avgs["team_activities"], top_performer["top_activities"], 0.05)
    ]

    # Explode each rep into multiple metric rows
    driver_rows = []
    for metric_name, col_name, team_avg, top_perf, impact in metrics:
        metric_df = df.select(
            F.col("rep_id"),
            F.lit(metric_name).alias("metric_name"),
            F.col(col_name).alias("rep_value"),
            F.lit(float(team_avg) if team_avg else 0).alias("team_avg"),
            F.lit(float(top_perf) if top_perf else 0).alias("top_performer"),
            F.lit(impact).alias("impact_on_quota")
        ).withColumn(
            "percentile",
            F.percent_rank().over(Window.orderBy(F.col("rep_value")))
        ).withColumn(
            "trend_direction",
            F.lit("stable")  # Would require historical data
        )
        driver_rows.append(metric_df)

    # Union all metric DataFrames
    result = driver_rows[0]
    for df_metric in driver_rows[1:]:
        result = result.union(df_metric)

    output.write_dataframe(result)


@transform(
    reps=Input("/RevOps/Scenario/sales_reps"),
    activities=Input("/RevOps/Scenario/activities"),
    output=Output("/RevOps/Coaching/activity_metrics")
)
def compute_activity_metrics(ctx, reps, activities, output):
    """Generate activity metrics per rep."""

    reps_df = reps.dataframe()
    acts_df = activities.dataframe()

    # Aggregate activities by rep and type
    rep_activities = acts_df.groupBy("rep_id").agg(
        F.count("*").alias("total_activities"),
        F.sum(F.when(F.col("activity_type") == "Call", 1).otherwise(0)).alias("calls"),
        F.sum(F.when(F.col("activity_type") == "Email", 1).otherwise(0)).alias("emails"),
        F.sum(F.when(F.col("activity_type") == "Meeting", 1).otherwise(0)).alias("meetings"),
        F.sum(F.when(F.col("activity_type") == "Demo", 1).otherwise(0)).alias("demos")
    )

    # Calculate daily averages (assuming 20 working days per month)
    with_daily = rep_activities.withColumn(
        "avg_daily",
        F.round(F.col("total_activities") / 20, 1)
    )

    # Calculate team benchmarks
    team_daily = with_daily.agg(
        F.round(F.avg("avg_daily"), 1).alias("team_avg_daily")
    ).collect()[0]["team_avg_daily"]

    top_daily = with_daily.orderBy(F.col("avg_daily").desc()).limit(3).agg(
        F.round(F.avg("avg_daily"), 1).alias("top_avg_daily")
    ).collect()[0]["top_avg_daily"]

    # Add benchmarks
    final = with_daily.withColumn(
        "team_avg_daily",
        F.lit(float(team_daily) if team_daily else 10.0)
    ).withColumn(
        "top_performer_daily",
        F.lit(float(top_daily) if top_daily else 15.0)
    ).withColumn(
        "daily_trend",
        F.lit("[]")  # JSON array placeholder - would be calculated from time series
    )

    output.write_dataframe(final)


@transform(
    opportunities=Input("/RevOps/Scenario/opportunities"),
    deal_health=Input("/RevOps/Enriched/deal_health_scores"),
    output=Output("/RevOps/Coaching/next_best_actions")
)
def compute_next_best_actions(ctx, opportunities, deal_health, output):
    """Generate AI-recommended next best actions per deal per rep."""

    opps_df = opportunities.dataframe()
    health_df = deal_health.dataframe()

    # Join opportunities with health scores
    open_opps = opps_df.filter(
        ~F.col("stage_name").isin("Closed Won", "Closed Lost")
    ).join(
        health_df.select("opportunity_id", "health_score", "health_category", "needs_attention"),
        "opportunity_id",
        "left"
    )

    # Generate action recommendations based on deal state
    actions = open_opps.withColumn(
        "action_type",
        F.when(F.col("days_since_activity") > 7, "Re-engage")
        .when(F.col("stakeholder_count") < 2, "Multi-thread")
        .when(F.col("days_in_stage") > 14, "Accelerate")
        .when(F.col("health_category") == "Critical", "Rescue")
        .when(F.col("stage_name") == "Negotiation", "Close")
        .otherwise("Progress")
    ).withColumn(
        "action_description",
        F.when(F.col("action_type") == "Re-engage",
               F.concat(F.lit("No activity in "), F.col("days_since_activity"), F.lit(" days - schedule touchpoint")))
        .when(F.col("action_type") == "Multi-thread",
               F.lit("Only 1-2 stakeholders engaged - identify and engage additional contacts"))
        .when(F.col("action_type") == "Accelerate",
               F.concat(F.lit("Stuck in "), F.col("stage_name"), F.lit(" for "), F.col("days_in_stage"), F.lit(" days - identify blocker")))
        .when(F.col("action_type") == "Rescue",
               F.lit("Deal health critical - executive intervention may be needed"))
        .when(F.col("action_type") == "Close",
               F.lit("In negotiation - push for verbal commit this week"))
        .otherwise(F.lit("Continue current approach"))
    ).withColumn(
        "priority",
        F.when(F.col("action_type").isin("Rescue", "Re-engage"), 3)
        .when(F.col("action_type").isin("Multi-thread", "Accelerate"), 2)
        .otherwise(1)
    ).withColumn(
        "expected_impact",
        F.when(F.col("action_type") == "Rescue", "Prevent $" + F.col("amount").cast("string") + " loss")
        .when(F.col("action_type") == "Close", "Win $" + F.col("amount").cast("string") + " this quarter")
        .otherwise(F.lit("Improve deal health"))
    ).withColumn(
        "due_date",
        F.date_add(F.current_date(), F.when(F.col("priority") == 3, 2).otherwise(5))
    )

    output_df = actions.select(
        F.col("owner_id").alias("rep_id"),
        "opportunity_id",
        F.col("account_name"),
        "action_type",
        "action_description",
        "priority",
        "expected_impact",
        "due_date"
    ).orderBy(F.col("rep_id"), F.col("priority").desc())

    output.write_dataframe(output_df)


@transform(
    reps=Input("/RevOps/Scenario/sales_reps"),
    rep_performance=Input("/RevOps/Analytics/rep_performance"),
    output=Output("/RevOps/Coaching/coaching_insights")
)
def compute_coaching_insights(ctx, reps, rep_performance, output):
    """Generate coaching insights (strengths, improvements, actions) per rep."""

    reps_df = reps.dataframe()
    perf_df = rep_performance.dataframe()

    # Join data
    combined = reps_df.alias("r").join(
        perf_df.alias("p"),
        F.col("r.rep_id") == F.col("p.rep_id"),
        "left"
    )

    # Generate strength insights
    strengths = combined.filter(F.col("r.win_rate") >= 0.30).select(
        F.col("r.rep_id"),
        F.lit("strength").alias("insight_type"),
        F.lit("Strong Closer").alias("title"),
        F.concat(
            F.lit("Win rate of "),
            F.round(F.col("r.win_rate") * 100, 0),
            F.lit("% exceeds team average")
        ).alias("description"),
        F.lit("+5% quota impact").alias("metric_impact"),
        F.lit("Continue current approach").alias("recommended_action"),
        F.lit(1).alias("priority"),
        F.lit("Performance").alias("category")
    )

    # Generate improvement insights
    improvements = combined.filter(F.col("r.avg_stakeholders") < 2.5).select(
        F.col("r.rep_id"),
        F.lit("improvement").alias("insight_type"),
        F.lit("Multi-threading Opportunity").alias("title"),
        F.concat(
            F.lit("Average of "),
            F.round(F.col("r.avg_stakeholders"), 1),
            F.lit(" stakeholders per deal below top performer benchmark of 4.0")
        ).alias("description"),
        F.lit("+12% win rate potential").alias("metric_impact"),
        F.lit("Shadow top performer; focus on executive engagement").alias("recommended_action"),
        F.lit(2).alias("priority"),
        F.lit("Deal Strategy").alias("category")
    )

    # Generate action insights
    actions = combined.filter(F.col("r.pipeline_coverage") < 2.5).select(
        F.col("r.rep_id"),
        F.lit("action").alias("insight_type"),
        F.lit("Pipeline Building Required").alias("title"),
        F.concat(
            F.lit("Pipeline coverage of "),
            F.round(F.col("r.pipeline_coverage"), 1),
            F.lit("x below target of 3x")
        ).alias("description"),
        F.lit("-15% quota risk").alias("metric_impact"),
        F.lit("Increase prospecting activity by 20%").alias("recommended_action"),
        F.lit(3).alias("priority"),
        F.lit("Pipeline").alias("category")
    )

    # Union all insights
    all_insights = strengths.union(improvements).union(actions)

    output.write_dataframe(all_insights)


@transform(
    reps=Input("/RevOps/Scenario/sales_reps"),
    output=Output("/RevOps/Coaching/rep_roster")
)
def compute_rep_roster(ctx, reps, output):
    """Generate rep roster for dropdown selectors."""

    df = reps.dataframe()

    roster = df.select(
        F.col("rep_id"),
        F.col("rep_name"),
        F.col("region"),
        F.col("segment"),
        F.col("manager_name"),
        F.col("role")
    ).orderBy("rep_name")

    output.write_dataframe(roster)
