"""
Automated QBR Pack Builder

Generates quarterly business review data and insights automatically.
Provides executive summaries, performance analysis, and recommendations.

Outputs:
- /RevOps/Analytics/qbr_performance_summary: Rep and region performance metrics
- /RevOps/Analytics/qbr_pipeline_health: Pipeline health snapshot
- /RevOps/Analytics/qbr_forecast_accuracy: Forecast accuracy analysis
- /RevOps/Analytics/qbr_win_loss_analysis: Win/loss patterns and insights
- /RevOps/Analytics/qbr_executive_summary: High-level executive summary
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime


# =============================================================================
# 1. PERFORMANCE SUMMARY BY REP/REGION
# =============================================================================

@transform(
    opportunities=Input("/RevOps/Scenario/opportunities"),
    reps=Input("/RevOps/Reference/sales_reps"),
    quota=Input("/RevOps/Reference/quota_targets"),
    output=Output("/RevOps/Analytics/qbr_performance_summary")
)
def compute_performance_summary(ctx, opportunities, reps, quota, output):
    """
    Generate performance summary by rep and region for QBR.
    """
    opps_df = opportunities.dataframe()
    reps_df = reps.dataframe()
    quota_df = quota.dataframe()

    # Current quarter filter (Q4 2024 for demo)
    current_quarter = "Q4 2024"

    # Aggregate by owner
    rep_metrics = opps_df.filter(
        F.col("fiscal_quarter") == current_quarter
    ).groupBy("owner_id", "owner_name").agg(
        F.sum(F.when(F.col("stage_name") == "Closed Won", F.col("amount")).otherwise(0)).alias("closed_won"),
        F.sum(F.when(F.col("stage_name") != "Closed Lost", F.col("amount")).otherwise(0)).alias("open_pipeline"),
        F.count("*").alias("total_deals"),
        F.sum(F.when(F.col("stage_name") == "Closed Won", 1).otherwise(0)).alias("won_count"),
        F.sum(F.when(F.col("stage_name") == "Closed Lost", 1).otherwise(0)).alias("lost_count"),
        F.avg("amount").alias("avg_deal_size"),
        F.avg(F.when(F.col("stage_name") == "Closed Won",
                     F.datediff(F.col("close_date"), F.col("created_date")))).alias("avg_sales_cycle"),
    )

    # Calculate win rate
    rep_metrics = rep_metrics.withColumn(
        "win_rate",
        F.when((F.col("won_count") + F.col("lost_count")) > 0,
               F.col("won_count") / (F.col("won_count") + F.col("lost_count"))).otherwise(0)
    )

    # Join with quota targets
    rep_summary = rep_metrics.join(
        quota_df.select("rep_id", "quota_amount"),
        rep_metrics.owner_id == quota_df.rep_id,
        "left"
    ).drop("rep_id")

    # Calculate attainment
    result = rep_summary.withColumn(
        "quota_attainment",
        F.when(F.col("quota_amount") > 0,
               F.col("closed_won") / F.col("quota_amount")).otherwise(0)
    ).withColumn(
        "gap_to_quota",
        F.col("quota_amount") - F.col("closed_won")
    ).withColumn(
        "performance_tier",
        F.when(F.col("quota_attainment") >= 1.1, "Top Performer")
         .when(F.col("quota_attainment") >= 0.9, "On Track")
         .when(F.col("quota_attainment") >= 0.7, "Needs Improvement")
         .otherwise("At Risk")
    ).withColumn(
        "quarter",
        F.lit(current_quarter)
    ).withColumn(
        "generated_at",
        F.lit(str(datetime.now()))
    )

    # Join with reps to get region
    final = result.join(
        reps_df.select("rep_id", "region", "segment"),
        result.owner_id == reps_df.rep_id,
        "left"
    ).drop("rep_id")

    output.write_dataframe(final)


# =============================================================================
# 2. PIPELINE HEALTH SNAPSHOT
# =============================================================================

@transform(
    deal_health=Input("/RevOps/Enriched/deal_health_scores"),
    opportunities=Input("/RevOps/Scenario/opportunities"),
    hygiene=Input("/RevOps/Analytics/pipeline_hygiene_summary"),
    output=Output("/RevOps/Analytics/qbr_pipeline_health")
)
def compute_pipeline_health(ctx, deal_health, opportunities, hygiene, output):
    """
    Generate pipeline health snapshot for QBR.
    """
    health_df = deal_health.dataframe()
    opps_df = opportunities.dataframe()
    hygiene_df = hygiene.dataframe()

    # Current quarter
    current_quarter = "Q4 2024"

    # Join deals with health scores
    pipeline = opps_df.filter(
        (F.col("fiscal_quarter") == current_quarter) &
        (F.col("stage_name") != "Closed Won") &
        (F.col("stage_name") != "Closed Lost")
    ).join(
        health_df.select("opportunity_id", "health_score_override"),
        "opportunity_id",
        "left"
    )

    # Aggregate pipeline metrics
    pipeline_summary = pipeline.agg(
        F.count("*").alias("total_opportunities"),
        F.sum("amount").alias("total_pipeline"),
        F.avg("amount").alias("avg_deal_size"),
        F.avg("health_score_override").alias("avg_health_score"),
        F.sum(F.when(F.col("health_score_override") >= 70, F.col("amount")).otherwise(0)).alias("healthy_pipeline"),
        F.sum(F.when((F.col("health_score_override") >= 50) & (F.col("health_score_override") < 70),
                     F.col("amount")).otherwise(0)).alias("monitor_pipeline"),
        F.sum(F.when(F.col("health_score_override") < 50, F.col("amount")).otherwise(0)).alias("at_risk_pipeline"),
    ).collect()[0]

    # Get hygiene metrics
    hygiene_row = hygiene_df.limit(1).collect()
    hygiene_score = hygiene_row[0]["avg_hygiene_score"] if hygiene_row else 70

    # Build summary record
    result = ctx.spark_session.createDataFrame([{
        "quarter": current_quarter,
        "total_opportunities": pipeline_summary["total_opportunities"] or 0,
        "total_pipeline": pipeline_summary["total_pipeline"] or 0,
        "avg_deal_size": pipeline_summary["avg_deal_size"] or 0,
        "avg_health_score": pipeline_summary["avg_health_score"] or 0,
        "healthy_pipeline": pipeline_summary["healthy_pipeline"] or 0,
        "healthy_pct": (pipeline_summary["healthy_pipeline"] or 0) / max(pipeline_summary["total_pipeline"] or 1, 1),
        "monitor_pipeline": pipeline_summary["monitor_pipeline"] or 0,
        "monitor_pct": (pipeline_summary["monitor_pipeline"] or 0) / max(pipeline_summary["total_pipeline"] or 1, 1),
        "at_risk_pipeline": pipeline_summary["at_risk_pipeline"] or 0,
        "at_risk_pct": (pipeline_summary["at_risk_pipeline"] or 0) / max(pipeline_summary["total_pipeline"] or 1, 1),
        "hygiene_score": float(hygiene_score),
        "pipeline_health_grade": _get_health_grade(pipeline_summary["avg_health_score"] or 0),
        "generated_at": str(datetime.now()),
    }])

    output.write_dataframe(result)


# =============================================================================
# 3. FORECAST ACCURACY ANALYSIS
# =============================================================================

@transform(
    forecasts=Input("/RevOps/Analytics/forecast_history"),
    opportunities=Input("/RevOps/Scenario/opportunities"),
    output=Output("/RevOps/Analytics/qbr_forecast_accuracy")
)
def compute_forecast_accuracy(ctx, forecasts, opportunities, output):
    """
    Analyze forecast accuracy over the quarter.
    """
    # For demo, generate simulated forecast accuracy data
    # In production, this would compare actual forecasts vs actuals

    weeks_data = []
    base_forecast = 12500000
    base_actual = 8200000

    for week in range(1, 13):  # 12 weeks in quarter
        forecast_at_week = base_forecast - (week * 50000)  # Forecasts get more accurate
        actual_at_week = base_actual * (week / 12) if week <= 10 else base_actual

        accuracy = 1 - abs(forecast_at_week - actual_at_week) / max(forecast_at_week, 1) if week > 4 else 0

        weeks_data.append({
            "week_number": week,
            "week_label": f"Week {week}",
            "forecast_amount": forecast_at_week,
            "actual_amount": actual_at_week,
            "variance": forecast_at_week - actual_at_week,
            "variance_pct": (forecast_at_week - actual_at_week) / max(forecast_at_week, 1),
            "accuracy_score": min(accuracy, 1.0),
            "quarter": "Q4 2024",
        })

    result = ctx.spark_session.createDataFrame(weeks_data)

    # Add summary metrics
    final = result.withColumn(
        "accuracy_grade",
        F.when(F.col("accuracy_score") >= 0.9, "A")
         .when(F.col("accuracy_score") >= 0.8, "B")
         .when(F.col("accuracy_score") >= 0.7, "C")
         .when(F.col("accuracy_score") >= 0.6, "D")
         .otherwise("F")
    ).withColumn(
        "generated_at",
        F.lit(str(datetime.now()))
    )

    output.write_dataframe(final)


# =============================================================================
# 4. WIN/LOSS ANALYSIS
# =============================================================================

@transform(
    opportunities=Input("/RevOps/Scenario/opportunities"),
    activities=Input("/RevOps/Scenario/activities"),
    output=Output("/RevOps/Analytics/qbr_win_loss_analysis")
)
def compute_win_loss_analysis(ctx, opportunities, activities, output):
    """
    Analyze win/loss patterns for QBR insights.
    """
    opps_df = opportunities.dataframe()
    activities_df = activities.dataframe()

    current_quarter = "Q4 2024"

    # Filter to closed deals
    closed_deals = opps_df.filter(
        (F.col("fiscal_quarter") == current_quarter) &
        (F.col("stage_name").isin(["Closed Won", "Closed Lost"]))
    )

    # Activity counts per deal
    activity_counts = activities_df.groupBy("opportunity_id").agg(
        F.count("*").alias("activity_count"),
        F.sum(F.when(F.col("activity_type") == "Meeting", 1).otherwise(0)).alias("meeting_count"),
        F.sum(F.when(F.col("activity_type") == "Call", 1).otherwise(0)).alias("call_count"),
        F.sum(F.when(F.col("activity_type") == "Email", 1).otherwise(0)).alias("email_count"),
    )

    # Join
    deals_with_activity = closed_deals.join(
        activity_counts,
        "opportunity_id",
        "left"
    )

    # Win vs Loss comparison
    win_metrics = deals_with_activity.filter(F.col("stage_name") == "Closed Won").agg(
        F.count("*").alias("count"),
        F.sum("amount").alias("total_revenue"),
        F.avg("amount").alias("avg_deal_size"),
        F.avg("activity_count").alias("avg_activities"),
        F.avg("meeting_count").alias("avg_meetings"),
        F.avg(F.datediff(F.col("close_date"), F.col("created_date"))).alias("avg_sales_cycle"),
    ).collect()[0]

    loss_metrics = deals_with_activity.filter(F.col("stage_name") == "Closed Lost").agg(
        F.count("*").alias("count"),
        F.sum("amount").alias("total_lost"),
        F.avg("amount").alias("avg_deal_size"),
        F.avg("activity_count").alias("avg_activities"),
        F.avg("meeting_count").alias("avg_meetings"),
        F.avg(F.datediff(F.col("close_date"), F.col("created_date"))).alias("avg_sales_cycle"),
    ).collect()[0]

    # Loss reasons (simulated - in production would come from CRM)
    loss_reasons = [
        {"reason": "Price/Budget", "count": 8, "pct": 0.32},
        {"reason": "Competitor", "count": 6, "pct": 0.24},
        {"reason": "No Decision", "count": 5, "pct": 0.20},
        {"reason": "Timing", "count": 4, "pct": 0.16},
        {"reason": "Product Fit", "count": 2, "pct": 0.08},
    ]

    # Build result
    result = ctx.spark_session.createDataFrame([{
        "quarter": current_quarter,
        # Win metrics
        "wins_count": win_metrics["count"] or 0,
        "wins_revenue": win_metrics["total_revenue"] or 0,
        "wins_avg_deal_size": win_metrics["avg_deal_size"] or 0,
        "wins_avg_activities": win_metrics["avg_activities"] or 0,
        "wins_avg_meetings": win_metrics["avg_meetings"] or 0,
        "wins_avg_cycle": win_metrics["avg_sales_cycle"] or 0,
        # Loss metrics
        "losses_count": loss_metrics["count"] or 0,
        "losses_revenue": loss_metrics["total_lost"] or 0,
        "losses_avg_deal_size": loss_metrics["avg_deal_size"] or 0,
        "losses_avg_activities": loss_metrics["avg_activities"] or 0,
        "losses_avg_meetings": loss_metrics["avg_meetings"] or 0,
        "losses_avg_cycle": loss_metrics["avg_sales_cycle"] or 0,
        # Win rate
        "win_rate": (win_metrics["count"] or 0) / max((win_metrics["count"] or 0) + (loss_metrics["count"] or 0), 1),
        # Top loss reasons (JSON encoded)
        "top_loss_reason_1": "Price/Budget",
        "top_loss_reason_1_pct": 0.32,
        "top_loss_reason_2": "Competitor",
        "top_loss_reason_2_pct": 0.24,
        "top_loss_reason_3": "No Decision",
        "top_loss_reason_3_pct": 0.20,
        # Insights
        "activity_delta": (win_metrics["avg_activities"] or 0) - (loss_metrics["avg_activities"] or 0),
        "meeting_delta": (win_metrics["avg_meetings"] or 0) - (loss_metrics["avg_meetings"] or 0),
        "cycle_delta": (loss_metrics["avg_sales_cycle"] or 0) - (win_metrics["avg_sales_cycle"] or 0),
        "generated_at": str(datetime.now()),
    }])

    output.write_dataframe(result)


# =============================================================================
# 5. EXECUTIVE SUMMARY
# =============================================================================

@transform(
    performance=Input("/RevOps/Analytics/qbr_performance_summary"),
    pipeline=Input("/RevOps/Analytics/qbr_pipeline_health"),
    forecast=Input("/RevOps/Analytics/qbr_forecast_accuracy"),
    win_loss=Input("/RevOps/Analytics/qbr_win_loss_analysis"),
    output=Output("/RevOps/Analytics/qbr_executive_summary")
)
def compute_executive_summary(ctx, performance, pipeline, forecast, win_loss, output):
    """
    Generate executive summary combining all QBR metrics.
    """
    perf_df = performance.dataframe()
    pipeline_df = pipeline.dataframe()
    forecast_df = forecast.dataframe()
    win_loss_df = win_loss.dataframe()

    current_quarter = "Q4 2024"

    # Performance aggregates
    perf_agg = perf_df.agg(
        F.sum("closed_won").alias("total_closed"),
        F.sum("quota_amount").alias("total_quota"),
        F.avg("quota_attainment").alias("avg_attainment"),
        F.avg("win_rate").alias("avg_win_rate"),
        F.sum(F.when(F.col("performance_tier") == "Top Performer", 1).otherwise(0)).alias("top_performers"),
        F.sum(F.when(F.col("performance_tier") == "At Risk", 1).otherwise(0)).alias("at_risk_reps"),
        F.count("*").alias("total_reps"),
    ).collect()[0]

    # Pipeline metrics
    pipeline_row = pipeline_df.limit(1).collect()[0] if pipeline_df.count() > 0 else {}

    # Latest forecast accuracy
    latest_forecast = forecast_df.orderBy(F.col("week_number").desc()).limit(1).collect()
    forecast_accuracy = latest_forecast[0]["accuracy_score"] if latest_forecast else 0

    # Win/loss metrics
    wl_row = win_loss_df.limit(1).collect()[0] if win_loss_df.count() > 0 else {}

    # Calculate overall health score
    attainment_score = min((perf_agg["avg_attainment"] or 0) / 0.8, 1.0) * 25
    pipeline_score = min((pipeline_row.get("avg_health_score", 50) or 50) / 80, 1.0) * 25
    forecast_score = forecast_accuracy * 25
    win_rate_score = min((wl_row.get("win_rate", 0.3) or 0.3) / 0.4, 1.0) * 25
    overall_health = attainment_score + pipeline_score + forecast_score + win_rate_score

    # Generate insights
    insights = _generate_insights(perf_agg, pipeline_row, wl_row, forecast_accuracy)
    recommendations = _generate_recommendations(perf_agg, pipeline_row, wl_row)

    # Build summary
    result = ctx.spark_session.createDataFrame([{
        "quarter": current_quarter,
        # Revenue metrics
        "total_closed_won": perf_agg["total_closed"] or 0,
        "total_quota": perf_agg["total_quota"] or 0,
        "team_attainment": (perf_agg["total_closed"] or 0) / max(perf_agg["total_quota"] or 1, 1),
        "avg_rep_attainment": perf_agg["avg_attainment"] or 0,
        # Team metrics
        "total_reps": perf_agg["total_reps"] or 0,
        "top_performers": perf_agg["top_performers"] or 0,
        "at_risk_reps": perf_agg["at_risk_reps"] or 0,
        # Pipeline metrics
        "total_pipeline": pipeline_row.get("total_pipeline", 0) or 0,
        "avg_health_score": pipeline_row.get("avg_health_score", 50) or 50,
        "at_risk_pipeline": pipeline_row.get("at_risk_pipeline", 0) or 0,
        "hygiene_score": pipeline_row.get("hygiene_score", 70) or 70,
        # Forecast metrics
        "forecast_accuracy": forecast_accuracy,
        "forecast_grade": _get_forecast_grade(forecast_accuracy),
        # Win/loss metrics
        "win_rate": wl_row.get("win_rate", 0) or 0,
        "avg_deal_size": wl_row.get("wins_avg_deal_size", 0) or 0,
        "avg_sales_cycle": wl_row.get("wins_avg_cycle", 0) or 0,
        "top_loss_reason": wl_row.get("top_loss_reason_1", "Unknown"),
        # Overall health
        "overall_health_score": overall_health,
        "health_grade": _get_health_grade(overall_health),
        # Insights (top 3)
        "insight_1": insights[0] if len(insights) > 0 else "",
        "insight_2": insights[1] if len(insights) > 1 else "",
        "insight_3": insights[2] if len(insights) > 2 else "",
        # Recommendations (top 3)
        "recommendation_1": recommendations[0] if len(recommendations) > 0 else "",
        "recommendation_2": recommendations[1] if len(recommendations) > 1 else "",
        "recommendation_3": recommendations[2] if len(recommendations) > 2 else "",
        # Metadata
        "generated_at": str(datetime.now()),
    }])

    output.write_dataframe(result)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def _get_health_grade(score: float) -> str:
    """Convert score to letter grade."""
    if score >= 85:
        return "A"
    elif score >= 70:
        return "B"
    elif score >= 55:
        return "C"
    elif score >= 40:
        return "D"
    return "F"


def _get_forecast_grade(accuracy: float) -> str:
    """Convert forecast accuracy to grade."""
    if accuracy >= 0.9:
        return "A"
    elif accuracy >= 0.8:
        return "B"
    elif accuracy >= 0.7:
        return "C"
    elif accuracy >= 0.6:
        return "D"
    return "F"


def _generate_insights(perf_agg: dict, pipeline_row: dict, wl_row: dict, forecast_accuracy: float) -> list:
    """Generate key insights from QBR data."""
    insights = []

    # Attainment insight
    attainment = (perf_agg.get("total_closed", 0) or 0) / max(perf_agg.get("total_quota", 1) or 1, 1)
    if attainment >= 0.9:
        insights.append(f"Team is on track at {attainment*100:.0f}% of quota with strong performer distribution.")
    elif attainment >= 0.7:
        insights.append(f"Team at {attainment*100:.0f}% of quota. Gap of ${(perf_agg.get('total_quota', 0) or 0) - (perf_agg.get('total_closed', 0) or 0):,.0f} needs to be addressed in remaining weeks.")
    else:
        insights.append(f"Team significantly behind at {attainment*100:.0f}% of quota. Urgent intervention required.")

    # Pipeline insight
    at_risk_pct = (pipeline_row.get("at_risk_pct", 0) or 0)
    if at_risk_pct > 0.3:
        insights.append(f"{at_risk_pct*100:.0f}% of pipeline is at risk (${pipeline_row.get('at_risk_pipeline', 0):,.0f}). Focus on deal acceleration and risk mitigation.")
    elif at_risk_pct < 0.15:
        insights.append("Pipeline health is strong with minimal at-risk deals. Maintain current engagement levels.")

    # Win rate insight
    win_rate = wl_row.get("win_rate", 0) or 0
    activity_delta = wl_row.get("activity_delta", 0) or 0
    if activity_delta > 5:
        insights.append(f"Won deals average {activity_delta:.0f} more activities than lost deals. Ensure adequate engagement on all opportunities.")

    # Loss reason insight
    top_loss = wl_row.get("top_loss_reason_1", "")
    if top_loss == "Price/Budget":
        insights.append("Price is the top loss reason. Review discounting authority and value messaging.")
    elif top_loss == "Competitor":
        insights.append("Competitive losses are high. Consider competitive battle card refresh and win-back programs.")

    return insights[:3]


def _generate_recommendations(perf_agg: dict, pipeline_row: dict, wl_row: dict) -> list:
    """Generate actionable recommendations."""
    recommendations = []

    # At-risk rep intervention
    at_risk_reps = perf_agg.get("at_risk_reps", 0) or 0
    if at_risk_reps > 0:
        recommendations.append(f"Schedule 1:1 coaching sessions with {at_risk_reps} at-risk reps to develop deal-specific action plans.")

    # Pipeline health
    hygiene_score = pipeline_row.get("hygiene_score", 70) or 70
    if hygiene_score < 70:
        recommendations.append("Implement weekly pipeline hygiene reviews. Require close date and next step updates for all active deals.")

    # Win rate improvement
    win_rate = wl_row.get("win_rate", 0) or 0
    if win_rate < 0.35:
        recommendations.append("Win rate below target. Review qualification criteria and implement MEDDIC framework adoption.")

    # Forecast accuracy
    at_risk_pipeline = pipeline_row.get("at_risk_pipeline", 0) or 0
    if at_risk_pipeline > 2000000:
        recommendations.append(f"${at_risk_pipeline/1000000:.1f}M at risk. Launch deal review cadence for all opportunities over $100K in late stages.")

    # Default recommendation
    if len(recommendations) == 0:
        recommendations.append("Maintain current processes. Focus on scaling best practices from top performers.")

    return recommendations[:3]
