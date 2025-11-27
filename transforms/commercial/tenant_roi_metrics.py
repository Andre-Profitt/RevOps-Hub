# /transforms/commercial/tenant_roi_metrics.py
"""
TENANT ROI METRICS
==================
Calculates return on investment metrics for sales collateral.

Metrics tracked:
- Win rate improvement
- Cycle time reduction
- Pipeline accuracy improvement
- Hygiene improvement
- Time savings

Outputs:
- /RevOps/Commercial/tenant_roi_baseline: Baseline metrics captured at onboarding
- /RevOps/Commercial/tenant_roi_current: Current metrics for comparison
- /RevOps/Commercial/tenant_roi_summary: ROI calculations and improvements
"""

from transforms.api import transform, Input, Output
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, BooleanType
from datetime import datetime, timedelta


# =============================================================================
# ROI BASELINE CAPTURE
# =============================================================================

@transform(
    customer_config=Input("/RevOps/Config/customer_settings"),
    opportunities=Input("/RevOps/Staging/opportunities"),
    implementation_status=Input("/RevOps/Commercial/implementation_status"),
    previous_baseline=Input("/RevOps/Commercial/tenant_roi_baseline"),
    output=Output("/RevOps/Commercial/tenant_roi_baseline"),
)
def compute_roi_baseline(
    ctx,
    customer_config: DataFrame,
    opportunities: DataFrame,
    implementation_status: DataFrame,
    previous_baseline: DataFrame,
    output,
):
    """
    Capture baseline metrics when customer goes live.
    Only captures once per customer (at go-live).
    """
    spark = ctx.spark_session
    now = datetime.utcnow()
    ninety_days_ago = now - timedelta(days=90)

    # Find newly live customers (not already in baseline)
    live_customers = implementation_status.filter(
        F.col("current_stage") == "live"
    ).select("customer_id", "go_live_date")

    if previous_baseline.count() > 0:
        # Only process customers not in baseline
        new_customers = live_customers.join(
            previous_baseline.select("customer_id"),
            "customer_id",
            "left_anti"
        )
    else:
        new_customers = live_customers

    if new_customers.count() == 0:
        # No new customers to baseline
        if previous_baseline.count() > 0:
            output.write_dataframe(previous_baseline)
        else:
            output.write_dataframe(spark.createDataFrame([], StructType([
                StructField("customer_id", StringType(), True),
                StructField("baseline_captured_at", TimestampType(), True),
                StructField("baseline_win_rate", DoubleType(), True),
                StructField("baseline_avg_cycle_days", DoubleType(), True),
                StructField("baseline_forecast_accuracy", DoubleType(), True),
                StructField("baseline_hygiene_score", DoubleType(), True),
                StructField("baseline_deals_per_month", DoubleType(), True),
            ])))
        return

    # Calculate baseline metrics from historical data (90 days pre-live)
    customer_ids = [row["customer_id"] for row in new_customers.collect()]

    baseline_opps = opportunities.filter(
        (F.col("customer_id").isin(customer_ids)) &
        (F.col("close_date") >= F.lit(ninety_days_ago)) &
        (F.col("close_date") < F.lit(now)) &
        (F.col("is_closed") == True)
    )

    if baseline_opps.count() > 0:
        baseline_metrics = baseline_opps.groupBy("customer_id").agg(
            # Win rate
            F.avg(F.when(F.col("is_won"), 1.0).otherwise(0.0)).alias("baseline_win_rate"),
            # Average cycle days (from create to close)
            F.avg(
                F.datediff(F.col("close_date"), F.col("created_date"))
            ).alias("baseline_avg_cycle_days"),
            # Deals per month
            (F.count("*") / 3.0).alias("baseline_deals_per_month"),
        )
    else:
        # Default baseline if no historical data
        baseline_metrics = new_customers.select("customer_id").withColumn(
            "baseline_win_rate", F.lit(0.20)  # Industry average
        ).withColumn(
            "baseline_avg_cycle_days", F.lit(45.0)  # Industry average
        ).withColumn(
            "baseline_deals_per_month", F.lit(10.0)
        )

    # Add capture timestamp and default values for other metrics
    baseline = baseline_metrics.withColumn(
        "baseline_captured_at", F.lit(now)
    ).withColumn(
        "baseline_forecast_accuracy", F.lit(0.70)  # Will be calculated over time
    ).withColumn(
        "baseline_hygiene_score", F.lit(60.0)  # Typical starting point
    )

    # Combine with previous baseline
    if previous_baseline.count() > 0:
        result = previous_baseline.unionByName(baseline, allowMissingColumns=True)
    else:
        result = baseline

    output.write_dataframe(result)


# =============================================================================
# ROI CURRENT METRICS
# =============================================================================

@transform(
    customer_config=Input("/RevOps/Config/customer_settings"),
    opportunities=Input("/RevOps/Staging/opportunities"),
    hygiene_alerts=Input("/RevOps/Analytics/pipeline_hygiene_alerts"),
    forecast_accuracy=Input("/RevOps/Analytics/forecast_accuracy"),
    implementation_status=Input("/RevOps/Commercial/implementation_status"),
    output=Output("/RevOps/Commercial/tenant_roi_current"),
)
def compute_roi_current(
    ctx,
    customer_config: DataFrame,
    opportunities: DataFrame,
    hygiene_alerts: DataFrame,
    forecast_accuracy: DataFrame,
    implementation_status: DataFrame,
    output,
):
    """
    Compute current metrics for ROI comparison.
    """
    spark = ctx.spark_session
    now = datetime.utcnow()
    thirty_days_ago = now - timedelta(days=30)

    # Get live customers only
    live_customers = implementation_status.filter(
        F.col("current_stage") == "live"
    ).select("customer_id")

    if live_customers.count() == 0:
        output.write_dataframe(spark.createDataFrame([], StructType([
            StructField("customer_id", StringType(), True),
            StructField("metrics_date", TimestampType(), True),
            StructField("current_win_rate", DoubleType(), True),
            StructField("current_avg_cycle_days", DoubleType(), True),
            StructField("current_forecast_accuracy", DoubleType(), True),
            StructField("current_hygiene_score", DoubleType(), True),
            StructField("current_deals_per_month", DoubleType(), True),
        ])))
        return

    customer_ids = [row["customer_id"] for row in live_customers.collect()]

    # Current win rate and cycle time
    recent_opps = opportunities.filter(
        (F.col("customer_id").isin(customer_ids)) &
        (F.col("close_date") >= F.lit(thirty_days_ago)) &
        (F.col("is_closed") == True)
    )

    if recent_opps.count() > 0:
        opp_metrics = recent_opps.groupBy("customer_id").agg(
            F.avg(F.when(F.col("is_won"), 1.0).otherwise(0.0)).alias("current_win_rate"),
            F.avg(F.datediff(F.col("close_date"), F.col("created_date"))).alias("current_avg_cycle_days"),
            F.count("*").alias("current_deals_per_month"),
        )
    else:
        opp_metrics = live_customers.withColumn(
            "current_win_rate", F.lit(None).cast(DoubleType())
        ).withColumn(
            "current_avg_cycle_days", F.lit(None).cast(DoubleType())
        ).withColumn(
            "current_deals_per_month", F.lit(None).cast(DoubleType())
        )

    # Current hygiene score (inverse of alerts per opportunity)
    if hygiene_alerts.count() > 0:
        hygiene = hygiene_alerts.filter(
            F.col("customer_id").isin(customer_ids)
        ).groupBy("customer_id").agg(
            F.count("*").alias("alert_count"),
        ).join(
            opportunities.filter(F.col("is_closed") == False).groupBy("customer_id").agg(
                F.count("*").alias("open_opps")
            ),
            "customer_id",
            "left"
        ).withColumn(
            "current_hygiene_score",
            F.greatest(
                100 - (F.col("alert_count") / F.greatest(F.col("open_opps"), F.lit(1)) * 10),
                F.lit(0.0)
            )
        ).select("customer_id", "current_hygiene_score")
    else:
        hygiene = live_customers.withColumn(
            "current_hygiene_score", F.lit(100.0)
        )

    # Current forecast accuracy
    if forecast_accuracy.count() > 0:
        accuracy = forecast_accuracy.filter(
            F.col("customer_id").isin(customer_ids)
        ).groupBy("customer_id").agg(
            F.avg("accuracy_pct").alias("current_forecast_accuracy")
        )
    else:
        accuracy = live_customers.withColumn(
            "current_forecast_accuracy", F.lit(None).cast(DoubleType())
        )

    # Combine metrics
    current = live_customers \
        .join(opp_metrics, "customer_id", "left") \
        .join(hygiene, "customer_id", "left") \
        .join(accuracy, "customer_id", "left")

    current = current.withColumn("metrics_date", F.lit(now))

    output.write_dataframe(current)


# =============================================================================
# ROI SUMMARY (Improvement Calculations)
# =============================================================================

@transform(
    baseline=Input("/RevOps/Commercial/tenant_roi_baseline"),
    current=Input("/RevOps/Commercial/tenant_roi_current"),
    customer_config=Input("/RevOps/Config/customer_settings"),
    output=Output("/RevOps/Commercial/tenant_roi_summary"),
)
def compute_roi_summary(ctx, baseline: DataFrame, current: DataFrame, customer_config: DataFrame, output):
    """
    Calculate ROI improvements for sales collateral.
    """
    spark = ctx.spark_session
    now = datetime.utcnow()

    if baseline.count() == 0 or current.count() == 0:
        output.write_dataframe(spark.createDataFrame([], StructType([
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("tier", StringType(), True),
            StructField("days_live", IntegerType(), True),
            StructField("win_rate_improvement_pct", DoubleType(), True),
            StructField("cycle_time_reduction_days", DoubleType(), True),
            StructField("forecast_accuracy_improvement_pct", DoubleType(), True),
            StructField("hygiene_improvement_pct", DoubleType(), True),
            StructField("estimated_revenue_impact", DoubleType(), True),
            StructField("calculated_at", TimestampType(), True),
        ])))
        return

    # Join baseline and current
    roi = baseline.join(current, "customer_id", "inner")

    # Join customer info
    roi = roi.join(
        customer_config.select("customer_id", "customer_name", "tier"),
        "customer_id",
        "left"
    )

    # Calculate improvements
    roi = roi.withColumn(
        "days_live",
        F.datediff(F.lit(now), F.col("baseline_captured_at"))
    ).withColumn(
        "win_rate_improvement_pct",
        F.when(
            F.col("baseline_win_rate") > 0,
            ((F.col("current_win_rate") - F.col("baseline_win_rate")) / F.col("baseline_win_rate")) * 100
        ).otherwise(0.0)
    ).withColumn(
        "cycle_time_reduction_days",
        F.col("baseline_avg_cycle_days") - F.coalesce(F.col("current_avg_cycle_days"), F.col("baseline_avg_cycle_days"))
    ).withColumn(
        "cycle_time_reduction_pct",
        F.when(
            F.col("baseline_avg_cycle_days") > 0,
            (F.col("cycle_time_reduction_days") / F.col("baseline_avg_cycle_days")) * 100
        ).otherwise(0.0)
    ).withColumn(
        "forecast_accuracy_improvement_pct",
        F.coalesce(F.col("current_forecast_accuracy"), F.lit(0.0)) -
        F.col("baseline_forecast_accuracy")
    ).withColumn(
        "hygiene_improvement_pct",
        F.coalesce(F.col("current_hygiene_score"), F.lit(0.0)) -
        F.col("baseline_hygiene_score")
    )

    # Estimate revenue impact (simplified model)
    # Assumes: higher win rate + shorter cycles = more deals = more revenue
    roi = roi.withColumn(
        "estimated_deals_gained",
        F.col("current_deals_per_month") - F.col("baseline_deals_per_month")
    ).withColumn(
        "estimated_revenue_impact",
        F.when(
            F.col("win_rate_improvement_pct") > 0,
            F.col("estimated_deals_gained") * 50000  # Avg deal size estimate
        ).otherwise(0.0)
    )

    # ROI category
    roi = roi.withColumn(
        "roi_status",
        F.when(
            (F.col("win_rate_improvement_pct") > 10) |
            (F.col("cycle_time_reduction_pct") > 10),
            "strong_roi"
        ).when(
            (F.col("win_rate_improvement_pct") > 0) |
            (F.col("cycle_time_reduction_days") > 0),
            "positive_roi"
        ).when(F.col("days_live") < 30, "ramping")
        .otherwise("needs_attention")
    )

    # Add timestamp
    roi = roi.withColumn("calculated_at", F.lit(now))

    # Select output columns
    result = roi.select(
        "customer_id",
        "customer_name",
        "tier",
        "days_live",
        "baseline_captured_at",
        # Baseline metrics
        "baseline_win_rate",
        "baseline_avg_cycle_days",
        "baseline_forecast_accuracy",
        "baseline_hygiene_score",
        "baseline_deals_per_month",
        # Current metrics
        "current_win_rate",
        "current_avg_cycle_days",
        "current_forecast_accuracy",
        "current_hygiene_score",
        "current_deals_per_month",
        # Improvements
        "win_rate_improvement_pct",
        "cycle_time_reduction_days",
        "cycle_time_reduction_pct",
        "forecast_accuracy_improvement_pct",
        "hygiene_improvement_pct",
        # ROI estimates
        "estimated_deals_gained",
        "estimated_revenue_impact",
        "roi_status",
        "calculated_at",
    )

    output.write_dataframe(result)
