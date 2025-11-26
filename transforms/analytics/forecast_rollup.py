# /transforms/analytics/forecast_rollup.py
"""
ANALYTICS: Forecast Rollup
===========================
Aggregates forecast data at multiple levels:
- Company total
- By region
- By manager
- By rep

Includes health-weighted confidence adjustments.
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@transform(
    opportunities=Input("/RevOps/Enriched/opportunity_health_scored"),
    sales_reps=Input("/RevOps/Sample/sales_reps"),
    company_forecast=Output("/RevOps/Analytics/company_forecast"),
    region_forecast=Output("/RevOps/Analytics/region_forecast"),
    manager_forecast=Output("/RevOps/Analytics/manager_forecast")
)
def compute(ctx, opportunities, sales_reps, company_forecast, region_forecast, manager_forecast):
    """
    Generate forecast rollups at multiple levels.
    """

    opps_df = opportunities.dataframe()
    reps_df = sales_reps.dataframe()

    # Filter to current quarter, open and recently closed won
    current_quarter_start = F.date_trunc("quarter", F.current_date())
    current_quarter_end = current_quarter_start + F.expr("INTERVAL 3 MONTHS - INTERVAL 1 DAY")

    current_quarter_opps = opps_df.filter(
        (F.col("close_date") >= current_quarter_start) &
        (F.col("close_date") <= current_quarter_end)
    )

    # ===========================================
    # HEALTH-ADJUSTED CONFIDENCE
    # ===========================================
    # Adjust commit/best case based on deal health

    with_confidence = current_quarter_opps.withColumn(
        "health_multiplier",
        F.when(F.col("health_category") == "Healthy", F.lit(1.0))
        .when(F.col("health_category") == "Monitor", F.lit(0.85))
        .when(F.col("health_category") == "At Risk", F.lit(0.60))
        .when(F.col("health_category") == "Critical", F.lit(0.30))
        .when(F.col("stage_name") == "Closed Won", F.lit(1.0))
        .otherwise(F.lit(0.5))
    ).withColumn(
        "adjusted_amount",
        F.col("amount") * F.col("health_multiplier")
    )

    # ===========================================
    # COMPANY LEVEL FORECAST
    # ===========================================

    company_agg = with_confidence.agg(
        # Closed Won
        F.sum(
            F.when(F.col("stage_name") == "Closed Won", F.col("amount")).otherwise(0)
        ).alias("closed_won"),

        # By forecast category (raw)
        F.sum(
            F.when(F.col("forecast_category") == "Commit", F.col("amount")).otherwise(0)
        ).alias("commit_raw"),
        F.sum(
            F.when(F.col("forecast_category") == "Best Case", F.col("amount")).otherwise(0)
        ).alias("best_case_raw"),
        F.sum(
            F.when(F.col("forecast_category") == "Pipeline", F.col("amount")).otherwise(0)
        ).alias("pipeline_raw"),

        # Health-adjusted amounts
        F.sum(
            F.when(F.col("forecast_category") == "Commit", F.col("adjusted_amount")).otherwise(0)
        ).alias("commit_adjusted"),
        F.sum(
            F.when(F.col("forecast_category") == "Best Case", F.col("adjusted_amount")).otherwise(0)
        ).alias("best_case_adjusted"),

        # Counts
        F.count(F.when(F.col("forecast_category") == "Commit", True)).alias("commit_count"),
        F.count(F.when(F.col("forecast_category") == "Best Case", True)).alias("best_case_count"),
        F.count(F.when(F.col("stage_name") == "Closed Won", True)).alias("closed_won_count"),

        # Health distribution
        F.avg(
            F.when(F.col("stage_name") != "Closed Won", F.col("health_score"))
        ).alias("avg_open_deal_health"),

        # At risk in forecast
        F.sum(
            F.when(
                (F.col("forecast_category").isin("Commit", "Best Case")) &
                (F.col("health_category").isin("At Risk", "Critical")),
                F.col("amount")
            ).otherwise(0)
        ).alias("at_risk_in_forecast"),
    )

    # Calculate derived metrics
    company_result = company_agg.withColumn(
        "total_forecast",
        F.col("closed_won") + F.col("commit_raw") + F.col("best_case_raw")
    ).withColumn(
        "adjusted_forecast",
        F.col("closed_won") + F.col("commit_adjusted") + (F.col("best_case_adjusted") * 0.5)
    ).withColumn(
        "forecast_confidence",
        F.when(
            F.col("total_forecast") > 0,
            F.col("adjusted_forecast") / F.col("total_forecast")
        ).otherwise(1.0)
    ).withColumn(
        "closed_pct",
        F.when(
            F.col("total_forecast") > 0,
            F.col("closed_won") / F.col("total_forecast")
        ).otherwise(0)
    ).withColumn(
        "remaining_to_close",
        F.col("commit_raw") + F.col("best_case_raw")
    ).withColumn(
        "quarter",
        F.concat(
            F.lit("Q"),
            F.quarter(F.current_date()),
            F.lit(" "),
            F.year(F.current_date())
        )
    ).withColumn(
        "calculated_at",
        F.current_timestamp()
    )

    company_forecast.write_dataframe(company_result)

    # ===========================================
    # REGION LEVEL FORECAST
    # ===========================================

    region_agg = with_confidence.groupBy("region").agg(
        F.sum(F.when(F.col("stage_name") == "Closed Won", F.col("amount")).otherwise(0)).alias("closed_won"),
        F.sum(F.when(F.col("forecast_category") == "Commit", F.col("amount")).otherwise(0)).alias("commit_raw"),
        F.sum(F.when(F.col("forecast_category") == "Best Case", F.col("amount")).otherwise(0)).alias("best_case_raw"),
        F.sum(F.when(F.col("forecast_category") == "Commit", F.col("adjusted_amount")).otherwise(0)).alias("commit_adjusted"),
        F.sum(F.when(F.col("forecast_category") == "Best Case", F.col("adjusted_amount")).otherwise(0)).alias("best_case_adjusted"),
        F.count(F.when(F.col("forecast_category") == "Commit", True)).alias("commit_count"),
        F.avg(F.when(F.col("stage_name") != "Closed Won", F.col("health_score"))).alias("avg_open_deal_health"),
    ).withColumn(
        "total_forecast",
        F.col("closed_won") + F.col("commit_raw") + F.col("best_case_raw")
    ).withColumn(
        "adjusted_forecast",
        F.col("closed_won") + F.col("commit_adjusted") + (F.col("best_case_adjusted") * 0.5)
    ).withColumn(
        "forecast_confidence",
        F.when(F.col("total_forecast") > 0, F.col("adjusted_forecast") / F.col("total_forecast")).otherwise(1.0)
    ).withColumn(
        "calculated_at",
        F.current_timestamp()
    )

    region_forecast.write_dataframe(region_agg)

    # ===========================================
    # MANAGER LEVEL FORECAST
    # ===========================================

    manager_agg = with_confidence.groupBy(
        "manager_id",
        "manager_name",
        "region"
    ).agg(
        F.sum(F.when(F.col("stage_name") == "Closed Won", F.col("amount")).otherwise(0)).alias("closed_won"),
        F.sum(F.when(F.col("forecast_category") == "Commit", F.col("amount")).otherwise(0)).alias("commit_raw"),
        F.sum(F.when(F.col("forecast_category") == "Best Case", F.col("amount")).otherwise(0)).alias("best_case_raw"),
        F.sum(F.when(F.col("forecast_category") == "Commit", F.col("adjusted_amount")).otherwise(0)).alias("commit_adjusted"),
        F.count("*").alias("total_deals"),
        F.countDistinct("owner_id").alias("rep_count"),
        F.avg(F.when(F.col("stage_name") != "Closed Won", F.col("health_score"))).alias("avg_team_health"),
    ).withColumn(
        "total_forecast",
        F.col("closed_won") + F.col("commit_raw") + F.col("best_case_raw")
    ).withColumn(
        "calculated_at",
        F.current_timestamp()
    )

    manager_forecast.write_dataframe(manager_agg)
