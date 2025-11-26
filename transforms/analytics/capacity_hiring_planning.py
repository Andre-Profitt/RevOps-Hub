"""
Capacity & Hiring Planning Analytics

Transforms for sales capacity modeling, quota coverage analysis,
ramp time tracking, and hiring impact simulation.
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@transform(
    users=Input("/RevOps/Enriched/users"),
    quotas=Input("/RevOps/Raw/quotas"),
    opportunities=Input("/RevOps/Enriched/deal_health_scores"),
    output=Output("/RevOps/Analytics/rep_capacity_model"),
)
def compute_rep_capacity_model(users, quotas, opportunities, output):
    """
    Compute capacity utilization and productivity metrics for each rep.

    Features:
    - Current capacity utilization
    - Quota coverage ratio
    - Productivity index (actual vs expected)
    - Ramp status tracking
    - Capacity headroom
    """
    users_df = users.dataframe()
    quotas_df = quotas.dataframe()
    opps_df = opportunities.dataframe()

    # Calculate rep productivity metrics
    rep_metrics = opps_df.filter(
        F.col("close_date") >= F.date_sub(F.current_date(), 90)
    ).groupBy("owner_id", "owner_name").agg(
        F.sum(
            F.when(F.col("stage_name") == "Closed Won", F.col("amount")).otherwise(0)
        ).alias("closed_won_90d"),
        F.sum(
            F.when(F.col("is_closed") == False, F.col("amount")).otherwise(0)
        ).alias("open_pipeline"),
        F.count("*").alias("total_opps"),
        F.countDistinct("account_id").alias("active_accounts"),
        F.avg("health_score").alias("avg_health_score"),
    )

    # Join with user and quota data
    capacity_model = users_df.join(
        quotas_df,
        users_df["user_id"] == quotas_df["rep_id"],
        "left"
    ).join(
        rep_metrics,
        users_df["user_id"] == rep_metrics["owner_id"],
        "left"
    )

    # Calculate capacity metrics
    capacity_model = capacity_model.withColumn(
        "days_since_hire",
        F.datediff(F.current_date(), F.col("hire_date"))
    ).withColumn(
        "ramp_status",
        F.when(F.col("days_since_hire") < 90, "Ramping")
         .when(F.col("days_since_hire") < 180, "Developing")
         .otherwise("Fully Ramped")
    ).withColumn(
        "ramp_factor",
        F.when(F.col("days_since_hire") < 30, 0.25)
         .when(F.col("days_since_hire") < 60, 0.50)
         .when(F.col("days_since_hire") < 90, 0.75)
         .when(F.col("days_since_hire") < 180, 0.90)
         .otherwise(1.0)
    ).withColumn(
        "effective_quota",
        F.round(F.col("quota_amount") * F.col("ramp_factor"), 0)
    ).withColumn(
        "quota_attainment",
        F.round(F.coalesce(F.col("closed_won_90d"), F.lit(0)) / F.col("effective_quota"), 3)
    ).withColumn(
        "coverage_ratio",
        F.round(F.coalesce(F.col("open_pipeline"), F.lit(0)) / F.col("effective_quota"), 2)
    ).withColumn(
        "productivity_index",
        F.round(
            (F.coalesce(F.col("closed_won_90d"), F.lit(0)) / F.col("effective_quota")) *
            (F.coalesce(F.col("avg_health_score"), F.lit(50)) / 100),
            2
        )
    ).withColumn(
        "capacity_utilization",
        F.least(
            F.round(F.col("active_accounts") / F.lit(50), 2),  # 50 accounts = 100% capacity
            F.lit(1.5)  # Cap at 150%
        )
    ).withColumn(
        "capacity_headroom",
        F.round(F.lit(1.0) - F.col("capacity_utilization"), 2)
    ).withColumn(
        "capacity_status",
        F.when(F.col("capacity_utilization") > 1.2, "Over Capacity")
         .when(F.col("capacity_utilization") > 0.9, "At Capacity")
         .when(F.col("capacity_utilization") > 0.6, "Healthy")
         .otherwise("Under Utilized")
    )

    # Select final columns
    result = capacity_model.select(
        F.col("user_id").alias("rep_id"),
        F.coalesce(F.col("full_name"), F.col("owner_name")).alias("rep_name"),
        "region",
        "segment",
        "hire_date",
        "days_since_hire",
        "ramp_status",
        "ramp_factor",
        "quota_amount",
        "effective_quota",
        F.coalesce(F.col("closed_won_90d"), F.lit(0)).alias("closed_won_90d"),
        F.coalesce(F.col("open_pipeline"), F.lit(0)).alias("open_pipeline"),
        "quota_attainment",
        "coverage_ratio",
        F.coalesce(F.col("active_accounts"), F.lit(0)).alias("active_accounts"),
        "capacity_utilization",
        "capacity_headroom",
        "capacity_status",
        "productivity_index",
        F.coalesce(F.col("avg_health_score"), F.lit(50)).alias("avg_deal_health"),
    )

    output.write_dataframe(result)


@transform(
    rep_capacity=Input("/RevOps/Analytics/rep_capacity_model"),
    quotas=Input("/RevOps/Raw/quotas"),
    output=Output("/RevOps/Analytics/team_capacity_summary"),
)
def compute_team_capacity_summary(rep_capacity, quotas, output):
    """
    Aggregate capacity metrics at the team/region level.

    Metrics:
    - Total team quota and attainment
    - Ramping vs ramped headcount
    - Capacity coverage gap
    - Hiring impact projections
    """
    capacity_df = rep_capacity.dataframe()

    # Team-level aggregations
    team_summary = capacity_df.groupBy("region", "segment").agg(
        F.count("*").alias("total_reps"),
        F.sum(F.when(F.col("ramp_status") == "Fully Ramped", 1).otherwise(0)).alias("ramped_reps"),
        F.sum(F.when(F.col("ramp_status") == "Ramping", 1).otherwise(0)).alias("ramping_reps"),
        F.sum(F.when(F.col("ramp_status") == "Developing", 1).otherwise(0)).alias("developing_reps"),
        F.sum("quota_amount").alias("total_quota"),
        F.sum("effective_quota").alias("effective_quota"),
        F.sum("closed_won_90d").alias("total_closed_won"),
        F.sum("open_pipeline").alias("total_pipeline"),
        F.sum("active_accounts").alias("total_active_accounts"),
        F.avg("capacity_utilization").alias("avg_capacity_utilization"),
        F.avg("productivity_index").alias("avg_productivity"),
        F.sum(
            F.when(F.col("capacity_status") == "Over Capacity", 1).otherwise(0)
        ).alias("over_capacity_count"),
        F.sum(
            F.when(F.col("capacity_status") == "Under Utilized", 1).otherwise(0)
        ).alias("under_utilized_count"),
    )

    # Calculate team metrics
    team_summary = team_summary.withColumn(
        "team_attainment",
        F.round(F.col("total_closed_won") / F.col("effective_quota"), 3)
    ).withColumn(
        "team_coverage_ratio",
        F.round(F.col("total_pipeline") / F.col("effective_quota"), 2)
    ).withColumn(
        "ramp_capacity_loss",
        F.round(F.col("total_quota") - F.col("effective_quota"), 0)
    ).withColumn(
        "avg_accounts_per_rep",
        F.round(F.col("total_active_accounts") / F.col("total_reps"), 0)
    ).withColumn(
        "hiring_need_score",
        F.when(
            (F.col("avg_capacity_utilization") > 1.0) & (F.col("team_coverage_ratio") < 2.5),
            F.lit("Critical")
        ).when(
            F.col("avg_capacity_utilization") > 0.9,
            F.lit("High")
        ).when(
            F.col("avg_capacity_utilization") > 0.7,
            F.lit("Medium")
        ).otherwise(F.lit("Low"))
    ).withColumn(
        "recommended_hires",
        F.when(F.col("hiring_need_score") == "Critical", F.ceil(F.col("total_reps") * 0.3))
         .when(F.col("hiring_need_score") == "High", F.ceil(F.col("total_reps") * 0.2))
         .when(F.col("hiring_need_score") == "Medium", F.ceil(F.col("total_reps") * 0.1))
         .otherwise(F.lit(0))
    ).withColumn(
        "capacity_health",
        F.when(
            (F.col("avg_capacity_utilization").between(0.7, 1.0)) &
            (F.col("team_coverage_ratio") >= 3.0),
            "Strong"
        ).when(
            F.col("avg_capacity_utilization").between(0.5, 1.1),
            "Healthy"
        ).when(
            F.col("avg_capacity_utilization") > 1.1,
            "Strained"
        ).otherwise("Under Utilized")
    )

    output.write_dataframe(team_summary)


@transform(
    rep_capacity=Input("/RevOps/Analytics/rep_capacity_model"),
    output=Output("/RevOps/Analytics/ramp_time_analysis"),
)
def compute_ramp_time_analysis(rep_capacity, output):
    """
    Analyze ramp time patterns and productivity curves.

    Shows time to productivity for different cohorts
    and identifies factors that accelerate ramp.
    """
    capacity_df = rep_capacity.dataframe()

    # Ramp cohort analysis
    ramp_analysis = capacity_df.withColumn(
        "hire_month",
        F.date_format(F.col("hire_date"), "yyyy-MM")
    ).withColumn(
        "ramp_cohort",
        F.when(F.col("days_since_hire") < 30, "0-30 days")
         .when(F.col("days_since_hire") < 60, "30-60 days")
         .when(F.col("days_since_hire") < 90, "60-90 days")
         .when(F.col("days_since_hire") < 120, "90-120 days")
         .when(F.col("days_since_hire") < 180, "120-180 days")
         .otherwise("180+ days")
    ).withColumn(
        "days_to_first_close",
        F.when(F.col("closed_won_90d") > 0,
            F.datediff(F.current_date(), F.col("hire_date")) - 45  # Estimate
        ).otherwise(None)
    )

    # Aggregate by cohort
    cohort_summary = ramp_analysis.groupBy("ramp_cohort").agg(
        F.count("*").alias("rep_count"),
        F.avg("quota_attainment").alias("avg_attainment"),
        F.avg("productivity_index").alias("avg_productivity"),
        F.avg("coverage_ratio").alias("avg_coverage"),
        F.avg("active_accounts").alias("avg_accounts"),
        F.avg("days_to_first_close").alias("avg_days_to_first_close"),
    ).withColumn(
        "expected_attainment",
        F.when(F.col("ramp_cohort") == "0-30 days", 0.10)
         .when(F.col("ramp_cohort") == "30-60 days", 0.25)
         .when(F.col("ramp_cohort") == "60-90 days", 0.50)
         .when(F.col("ramp_cohort") == "90-120 days", 0.75)
         .when(F.col("ramp_cohort") == "120-180 days", 0.90)
         .otherwise(1.0)
    ).withColumn(
        "performance_vs_expected",
        F.round(F.col("avg_attainment") - F.col("expected_attainment"), 3)
    ).withColumn(
        "cohort_order",
        F.when(F.col("ramp_cohort") == "0-30 days", 1)
         .when(F.col("ramp_cohort") == "30-60 days", 2)
         .when(F.col("ramp_cohort") == "60-90 days", 3)
         .when(F.col("ramp_cohort") == "90-120 days", 4)
         .when(F.col("ramp_cohort") == "120-180 days", 5)
         .otherwise(6)
    ).orderBy("cohort_order")

    output.write_dataframe(cohort_summary)


@transform(
    team_capacity=Input("/RevOps/Analytics/team_capacity_summary"),
    output=Output("/RevOps/Analytics/hiring_impact_model"),
)
def compute_hiring_impact_model(team_capacity, output):
    """
    Model the impact of hiring on quota capacity.

    Projects:
    - Time to full productivity
    - Quota coverage improvement
    - Break-even timeline
    """
    team_df = team_capacity.dataframe()

    # Model hiring scenarios
    # Assume: New hire costs $150K/year, average quota $1.5M, 6-month ramp
    hiring_model = team_df.withColumn(
        "avg_rep_quota",
        F.round(F.col("total_quota") / F.col("total_reps"), 0)
    ).withColumn(
        "current_capacity_gap",
        F.greatest(
            F.round(F.col("total_quota") * 3.0 - F.col("total_pipeline"), 0),
            F.lit(0)
        )
    ).withColumn(
        "quota_per_hire_y1",
        F.round(F.col("avg_rep_quota") * 0.5, 0)  # 50% productivity in year 1
    ).withColumn(
        "pipeline_per_hire_y1",
        F.round(F.col("quota_per_hire_y1") * 3.0, 0)
    ).withColumn(
        "hires_to_close_gap",
        F.ceil(F.col("current_capacity_gap") / F.col("pipeline_per_hire_y1"))
    ).withColumn(
        "investment_per_hire",
        F.lit(150000)
    ).withColumn(
        "total_investment",
        F.col("recommended_hires") * F.col("investment_per_hire")
    ).withColumn(
        "projected_revenue_y1",
        F.col("recommended_hires") * F.col("quota_per_hire_y1")
    ).withColumn(
        "projected_pipeline_y1",
        F.col("recommended_hires") * F.col("pipeline_per_hire_y1")
    ).withColumn(
        "roi_y1",
        F.round(F.col("projected_revenue_y1") / F.col("total_investment"), 2)
    ).withColumn(
        "months_to_breakeven",
        F.when(F.col("recommended_hires") > 0,
            F.round(F.col("total_investment") / (F.col("projected_revenue_y1") / 12), 0)
        ).otherwise(F.lit(0))
    )

    output.write_dataframe(hiring_model)


@transform(
    rep_capacity=Input("/RevOps/Analytics/rep_capacity_model"),
    team_capacity=Input("/RevOps/Analytics/team_capacity_summary"),
    hiring_model=Input("/RevOps/Analytics/hiring_impact_model"),
    output=Output("/RevOps/Analytics/capacity_planning_summary"),
)
def compute_capacity_planning_summary(rep_capacity, team_capacity, hiring_model, output):
    """
    Executive summary of capacity planning metrics.
    """
    rep_df = rep_capacity.dataframe()
    team_df = team_capacity.dataframe()
    hiring_df = hiring_model.dataframe()

    # Overall metrics
    overall = rep_df.agg(
        F.count("*").alias("total_reps"),
        F.sum(F.when(F.col("ramp_status") == "Fully Ramped", 1).otherwise(0)).alias("ramped_reps"),
        F.sum(F.when(F.col("ramp_status") == "Ramping", 1).otherwise(0)).alias("ramping_reps"),
        F.sum(F.when(F.col("ramp_status") == "Developing", 1).otherwise(0)).alias("developing_reps"),
        F.sum("quota_amount").alias("total_quota"),
        F.sum("effective_quota").alias("effective_quota"),
        F.sum("closed_won_90d").alias("total_closed_won"),
        F.sum("open_pipeline").alias("total_pipeline"),
        F.avg("capacity_utilization").alias("avg_utilization"),
        F.avg("productivity_index").alias("avg_productivity"),
        F.sum(F.when(F.col("capacity_status") == "Over Capacity", 1).otherwise(0)).alias("over_capacity_count"),
        F.sum(F.when(F.col("capacity_status") == "Under Utilized", 1).otherwise(0)).alias("under_utilized_count"),
    )

    # Hiring totals
    hiring_totals = hiring_df.agg(
        F.sum("recommended_hires").alias("total_recommended_hires"),
        F.sum("total_investment").alias("total_hiring_investment"),
        F.sum("projected_revenue_y1").alias("total_projected_revenue"),
        F.sum("current_capacity_gap").alias("total_capacity_gap"),
    )

    # Cross join for single row output
    summary = overall.crossJoin(hiring_totals)

    # Calculate derived metrics
    summary = summary.withColumn(
        "team_attainment",
        F.round(F.col("total_closed_won") / F.col("effective_quota"), 3)
    ).withColumn(
        "coverage_ratio",
        F.round(F.col("total_pipeline") / F.col("effective_quota"), 2)
    ).withColumn(
        "ramp_capacity_loss_pct",
        F.round((F.col("total_quota") - F.col("effective_quota")) / F.col("total_quota"), 3)
    ).withColumn(
        "capacity_health",
        F.when(
            (F.col("avg_utilization").between(0.7, 1.0)) &
            (F.col("coverage_ratio") >= 3.0) &
            (F.col("over_capacity_count") == 0),
            "Strong"
        ).when(
            F.col("avg_utilization").between(0.5, 1.1),
            "Healthy"
        ).when(
            F.col("avg_utilization") > 1.1,
            "Strained"
        ).otherwise("Under Utilized")
    ).withColumn(
        "hiring_urgency",
        F.when(F.col("over_capacity_count") > F.col("total_reps") * 0.3, "Critical")
         .when(F.col("over_capacity_count") > F.col("total_reps") * 0.2, "High")
         .when(F.col("over_capacity_count") > 0, "Medium")
         .otherwise("Low")
    ).withColumn(
        "snapshot_date",
        F.current_date()
    )

    output.write_dataframe(summary)
