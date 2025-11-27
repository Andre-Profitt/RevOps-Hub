# /transforms/analytics/stage_velocity_analytics.py
"""
STAGE VELOCITY ANALYTICS
========================
Analyzes deal movement through pipeline stages:
- Average duration per stage vs benchmark
- Conversion rates between stages
- Deviation trends and health indicators
- Amount in each stage

Powers the Pipeline Health Dashboard stage velocity view.
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@transform(
    opportunities=Input("/RevOps/Scenario/opportunities"),
    benchmarks=Input("/RevOps/Reference/stage_benchmarks"),
    output=Output("/RevOps/Analytics/stage_velocity")
)
def compute_stage_velocity(ctx, opportunities, benchmarks, output):
    """Calculate stage velocity metrics with benchmark comparisons."""

    opps_df = opportunities.dataframe()
    bench_df = benchmarks.dataframe()

    # Define stage order for funnel analysis
    stage_order = {
        "Prospecting": 1,
        "Discovery": 2,
        "Solution Design": 3,
        "Proposal": 4,
        "Negotiation": 5,
        "Verbal Commit": 6,
        "Closed Won": 7,
        "Closed Lost": 8
    }

    # Add stage order to opportunities
    opps_with_order = opps_df.withColumn(
        "stage_order",
        F.when(F.col("stage_name") == "Prospecting", 1)
        .when(F.col("stage_name") == "Discovery", 2)
        .when(F.col("stage_name") == "Solution Design", 3)
        .when(F.col("stage_name") == "Proposal", 4)
        .when(F.col("stage_name") == "Negotiation", 5)
        .when(F.col("stage_name") == "Verbal Commit", 6)
        .when(F.col("stage_name") == "Closed Won", 7)
        .when(F.col("stage_name") == "Closed Lost", 8)
        .otherwise(0)
    )

    # Filter to open opportunities (exclude closed)
    open_opps = opps_with_order.filter(
        ~F.col("stage_name").isin("Closed Won", "Closed Lost")
    )

    # Calculate metrics per stage
    stage_metrics = open_opps.groupBy("stage_name", "stage_order").agg(
        F.count("*").alias("deals_in_stage"),
        F.sum("amount").alias("amount_in_stage"),
        F.round(F.avg("days_in_stage"), 1).alias("avg_duration")
    )

    # Join with benchmarks
    with_benchmarks = stage_metrics.join(
        bench_df.select(
            F.col("stage_name").alias("bench_stage"),
            F.col("target_days").alias("benchmark")
        ),
        stage_metrics.stage_name == F.col("bench_stage"),
        "left"
    ).drop("bench_stage")

    # Calculate deviation and trends
    velocity_analysis = with_benchmarks.withColumn(
        "deviation",
        F.round((F.col("avg_duration") - F.col("benchmark")) / F.col("benchmark") * 100, 1)
    ).withColumn(
        "trend_direction",
        F.when(F.col("deviation") > 20, "slowing")
        .when(F.col("deviation") < -10, "accelerating")
        .otherwise("on_track")
    )

    # Calculate conversion rates (simplified - based on stage progression)
    # In real implementation, this would use historical data
    conversion_base = {
        "Prospecting": 0.65,
        "Discovery": 0.70,
        "Solution Design": 0.75,
        "Proposal": 0.60,
        "Negotiation": 0.80,
        "Verbal Commit": 0.95
    }

    final = velocity_analysis.withColumn(
        "conversion_rate",
        F.when(F.col("stage_name") == "Prospecting", 0.65)
        .when(F.col("stage_name") == "Discovery", 0.70)
        .when(F.col("stage_name") == "Solution Design", 0.75)
        .when(F.col("stage_name") == "Proposal", 0.60)
        .when(F.col("stage_name") == "Negotiation", 0.80)
        .when(F.col("stage_name") == "Verbal Commit", 0.95)
        .otherwise(0.50)
    ).withColumn(
        "quarter",
        F.lit("Q4 2024")  # Default quarter for filtering
    ).withColumn(
        "region",
        F.lit("All")  # Aggregate view
    )

    # Select and order output columns
    output_df = final.select(
        "stage_name",
        "stage_order",
        "avg_duration",
        "benchmark",
        "deviation",
        "trend_direction",
        "conversion_rate",
        "deals_in_stage",
        "amount_in_stage",
        "quarter",
        "region"
    ).orderBy("stage_order")

    output.write_dataframe(output_df)


@transform(
    opportunities=Input("/RevOps/Scenario/opportunities"),
    benchmarks=Input("/RevOps/Reference/stage_benchmarks"),
    output=Output("/RevOps/Analytics/pipeline_funnel")
)
def compute_pipeline_funnel(ctx, opportunities, benchmarks, output):
    """Calculate pipeline funnel metrics for visualization."""

    opps_df = opportunities.dataframe()

    # Define stage order
    stage_order_map = {
        "Prospecting": 1,
        "Discovery": 2,
        "Solution Design": 3,
        "Proposal": 4,
        "Negotiation": 5,
        "Verbal Commit": 6
    }

    # Filter to open pipeline stages
    open_stages = ["Prospecting", "Discovery", "Solution Design",
                   "Proposal", "Negotiation", "Verbal Commit"]

    funnel_opps = opps_df.filter(F.col("stage_name").isin(open_stages))

    # Calculate funnel metrics
    funnel_metrics = funnel_opps.groupBy("stage_name").agg(
        F.sum("amount").alias("amount"),
        F.count("*").alias("count")
    )

    # Add stage order
    funnel_with_order = funnel_metrics.withColumn(
        "stage_order",
        F.when(F.col("stage_name") == "Prospecting", 1)
        .when(F.col("stage_name") == "Discovery", 2)
        .when(F.col("stage_name") == "Solution Design", 3)
        .when(F.col("stage_name") == "Proposal", 4)
        .when(F.col("stage_name") == "Negotiation", 5)
        .when(F.col("stage_name") == "Verbal Commit", 6)
        .otherwise(7)
    )

    # Calculate total for conversion rates
    total_pipeline = funnel_opps.agg(F.sum("amount")).collect()[0][0] or 1

    # Add conversion rates and rename for webapp
    final = funnel_with_order.withColumn(
        "name",
        F.col("stage_name")
    ).withColumn(
        "conversion_rate",
        F.round(F.col("amount") / F.lit(total_pipeline), 3)
    ).withColumn(
        "quarter",
        F.lit("Q4 2024")
    ).withColumn(
        "region",
        F.lit("All")
    )

    output_df = final.select(
        "name",
        "amount",
        "count",
        "conversion_rate",
        "stage_order",
        "quarter",
        "region"
    ).orderBy("stage_order")

    output.write_dataframe(output_df)
