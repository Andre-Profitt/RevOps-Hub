"""
Scenario Modeling Analytics

Transforms for what-if analysis, forecast simulations,
and revenue impact modeling.
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@transform(
    opportunities=Input("/RevOps/Enriched/deal_health_scores"),
    quotas=Input("/RevOps/Raw/quotas"),
    output=Output("/RevOps/Analytics/scenario_base_metrics"),
)
def compute_scenario_base_metrics(opportunities, quotas, output):
    """
    Compute baseline metrics for scenario modeling.
    """
    opps_df = opportunities.dataframe()
    quotas_df = quotas.dataframe()

    # Current pipeline metrics
    pipeline_metrics = opps_df.filter(F.col("is_closed") == False).agg(
        F.count("*").alias("total_deals"),
        F.sum("amount").alias("total_pipeline"),
        F.avg("amount").alias("avg_deal_size"),
        F.avg("health_score").alias("avg_health_score"),
        F.avg("win_probability").alias("avg_win_probability"),
        F.sum(F.col("amount") * F.col("win_probability") / 100).alias("weighted_pipeline"),
    )

    # Win rate by stage
    stage_metrics = opps_df.groupBy("stage_name").agg(
        F.count("*").alias("deal_count"),
        F.avg("win_probability").alias("stage_win_rate"),
        F.avg("amount").alias("avg_stage_amount"),
    )

    # Quota totals
    quota_total = quotas_df.agg(F.sum("quota_amount").alias("total_quota")).first()

    # Combine for baseline
    baseline = pipeline_metrics.withColumn("total_quota", F.lit(quota_total["total_quota"]))
    baseline = baseline.withColumn(
        "coverage_ratio",
        F.round(F.col("total_pipeline") / F.col("total_quota"), 2)
    ).withColumn(
        "weighted_coverage",
        F.round(F.col("weighted_pipeline") / F.col("total_quota"), 2)
    )

    output.write_dataframe(baseline)


@transform(
    opportunities=Input("/RevOps/Enriched/deal_health_scores"),
    output=Output("/RevOps/Analytics/scenario_win_rate_impact"),
)
def compute_win_rate_scenarios(opportunities, output):
    """
    Model impact of win rate changes on forecast.

    Scenarios: -10%, -5%, base, +5%, +10%
    """
    opps_df = opportunities.dataframe()

    # Get current metrics
    current = opps_df.filter(F.col("is_closed") == False).agg(
        F.sum("amount").alias("total_pipeline"),
        F.avg("win_probability").alias("base_win_rate"),
    ).first()

    base_pipeline = current["total_pipeline"]
    base_win_rate = current["base_win_rate"] / 100

    # Create scenarios
    scenarios = []
    for delta in [-0.10, -0.05, 0, 0.05, 0.10]:
        new_rate = min(1.0, max(0, base_win_rate + delta))
        projected = base_pipeline * new_rate
        scenarios.append({
            "scenario_name": f"{delta * 100:+.0f}% Win Rate" if delta != 0 else "Baseline",
            "win_rate_delta": delta,
            "projected_win_rate": new_rate,
            "projected_revenue": projected,
            "revenue_delta": projected - (base_pipeline * base_win_rate),
            "revenue_delta_pct": delta / base_win_rate if base_win_rate > 0 else 0,
        })

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    result = spark.createDataFrame(scenarios)
    output.write_dataframe(result)


@transform(
    opportunities=Input("/RevOps/Enriched/deal_health_scores"),
    output=Output("/RevOps/Analytics/scenario_deal_size_impact"),
)
def compute_deal_size_scenarios(opportunities, output):
    """
    Model impact of deal size changes on forecast.
    """
    opps_df = opportunities.dataframe()

    # Get current metrics
    current = opps_df.filter(F.col("is_closed") == False).agg(
        F.count("*").alias("deal_count"),
        F.avg("amount").alias("avg_deal_size"),
        F.sum("amount").alias("total_pipeline"),
        F.avg("win_probability").alias("base_win_rate"),
    ).first()

    deal_count = current["deal_count"]
    base_size = current["avg_deal_size"]
    base_win_rate = current["base_win_rate"] / 100

    # Create scenarios
    scenarios = []
    for delta_pct in [-0.15, -0.10, -0.05, 0, 0.05, 0.10, 0.15]:
        new_size = base_size * (1 + delta_pct)
        new_pipeline = new_size * deal_count
        projected = new_pipeline * base_win_rate
        base_projected = base_size * deal_count * base_win_rate
        scenarios.append({
            "scenario_name": f"{delta_pct * 100:+.0f}% Avg Deal Size" if delta_pct != 0 else "Baseline",
            "deal_size_delta_pct": delta_pct,
            "projected_deal_size": new_size,
            "projected_pipeline": new_pipeline,
            "projected_revenue": projected,
            "revenue_delta": projected - base_projected,
        })

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    result = spark.createDataFrame(scenarios)
    output.write_dataframe(result)


@transform(
    opportunities=Input("/RevOps/Enriched/deal_health_scores"),
    output=Output("/RevOps/Analytics/scenario_cycle_time_impact"),
)
def compute_cycle_time_scenarios(opportunities, output):
    """
    Model impact of sales cycle acceleration/deceleration.
    """
    opps_df = opportunities.dataframe()

    # Calculate current cycle metrics
    current = opps_df.filter(F.col("is_closed") == False).agg(
        F.count("*").alias("deal_count"),
        F.sum("amount").alias("total_pipeline"),
        F.avg("days_in_stage").alias("avg_days_in_stage"),
        F.avg("win_probability").alias("base_win_rate"),
    ).first()

    base_days = current["avg_days_in_stage"]
    base_pipeline = current["total_pipeline"]
    base_win_rate = current["base_win_rate"] / 100

    # Scenarios: faster cycles typically improve win rates
    scenarios = []
    for days_delta in [14, 7, 0, -7, -14]:  # Negative = faster
        # Assume 1% win rate improvement per week faster
        win_rate_adj = -days_delta / 7 * 0.01
        new_win_rate = min(1.0, max(0, base_win_rate + win_rate_adj))
        projected = base_pipeline * new_win_rate

        scenario_name = "Baseline" if days_delta == 0 else \
                       f"{abs(days_delta)}d faster" if days_delta < 0 else \
                       f"{days_delta}d slower"

        scenarios.append({
            "scenario_name": scenario_name,
            "cycle_days_delta": days_delta,
            "projected_cycle_days": base_days + days_delta,
            "win_rate_impact": win_rate_adj,
            "projected_win_rate": new_win_rate,
            "projected_revenue": projected,
            "revenue_delta": projected - (base_pipeline * base_win_rate),
            "deals_per_quarter_change": -days_delta / 7 * 0.05,  # 5% more deals per week saved
        })

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    result = spark.createDataFrame(scenarios)
    output.write_dataframe(result)


@transform(
    win_rate=Input("/RevOps/Analytics/scenario_win_rate_impact"),
    deal_size=Input("/RevOps/Analytics/scenario_deal_size_impact"),
    cycle_time=Input("/RevOps/Analytics/scenario_cycle_time_impact"),
    output=Output("/RevOps/Analytics/scenario_summary"),
)
def compute_scenario_summary(win_rate, deal_size, cycle_time, output):
    """
    Aggregate scenario summary with key levers.
    """
    win_df = win_rate.dataframe()
    deal_df = deal_size.dataframe()
    cycle_df = cycle_time.dataframe()

    # Get baseline values
    baseline_win = win_df.filter(F.col("scenario_name") == "Baseline").first()
    baseline_deal = deal_df.filter(F.col("scenario_name") == "Baseline").first()

    # Best case scenarios
    best_win = win_df.orderBy(F.col("projected_revenue").desc()).first()
    best_deal = deal_df.orderBy(F.col("projected_revenue").desc()).first()
    best_cycle = cycle_df.orderBy(F.col("projected_revenue").desc()).first()

    # Create summary
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    summary = spark.createDataFrame([{
        "baseline_revenue": baseline_win["projected_revenue"],
        "baseline_win_rate": baseline_win["projected_win_rate"],
        "baseline_deal_size": baseline_deal["projected_deal_size"],
        # Win rate lever
        "win_rate_best_scenario": best_win["scenario_name"],
        "win_rate_upside": best_win["revenue_delta"],
        "win_rate_upside_pct": best_win["revenue_delta"] / baseline_win["projected_revenue"] if baseline_win["projected_revenue"] > 0 else 0,
        # Deal size lever
        "deal_size_best_scenario": best_deal["scenario_name"],
        "deal_size_upside": best_deal["revenue_delta"],
        "deal_size_upside_pct": best_deal["revenue_delta"] / baseline_deal["projected_revenue"] if baseline_deal["projected_revenue"] > 0 else 0,
        # Cycle time lever
        "cycle_time_best_scenario": best_cycle["scenario_name"],
        "cycle_time_upside": best_cycle["revenue_delta"],
        # Combined upside
        "total_upside_potential": best_win["revenue_delta"] + best_deal["revenue_delta"] + best_cycle["revenue_delta"],
        "snapshot_date": str(F.current_date()),
    }])

    output.write_dataframe(summary)
