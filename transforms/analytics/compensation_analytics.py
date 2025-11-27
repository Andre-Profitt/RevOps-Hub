# /transforms/analytics/compensation_analytics.py
"""
COMPENSATION & ATTAINMENT ANALYTICS
===================================
Tracks sales compensation metrics and quota attainment:
- Comp plan summary (team totals, commission projections)
- Per-rep attainment (quota progress, accelerator status)
- Attainment trends over time

Powers the Compensation Dashboard.
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@transform(
    reps=Input("/RevOps/Scenario/sales_reps"),
    opportunities=Input("/RevOps/Scenario/opportunities"),
    output=Output("/RevOps/Compensation/summary")
)
def compute_comp_summary(ctx, reps, opportunities, output):
    """Generate compensation summary metrics."""

    reps_df = reps.dataframe()
    opps_df = opportunities.dataframe()

    # Calculate closed won revenue per rep
    closed_won = opps_df.filter(F.col("stage_name") == "Closed Won").groupBy("owner_id").agg(
        F.sum("amount").alias("closed_won_amount")
    )

    # Join with rep data
    combined = reps_df.join(
        closed_won,
        reps_df.rep_id == closed_won.owner_id,
        "left"
    ).fillna({"closed_won_amount": 0})

    # Calculate commission (simplified: 10% base, accelerator above 100%)
    with_commission = combined.withColumn(
        "commission_rate",
        F.when(F.col("ytd_attainment") >= 1.2, 0.15)
        .when(F.col("ytd_attainment") >= 1.0, 0.12)
        .otherwise(0.10)
    ).withColumn(
        "commission_earned",
        F.round(F.col("closed_won_amount") * F.col("commission_rate"), 0)
    ).withColumn(
        "status",
        F.when(F.col("ytd_attainment") >= 1.0, "On Track")
        .when(F.col("ytd_attainment") >= 0.8, "At Risk")
        .otherwise("Behind")
    )

    # Aggregate summary
    summary = with_commission.agg(
        F.sum("quarterly_quota").alias("total_quota"),
        F.sum("closed_won_amount").alias("total_closed"),
        F.round(F.sum("closed_won_amount") / F.sum("quarterly_quota"), 3).alias("team_attainment"),
        F.round(F.avg("ytd_attainment"), 3).alias("avg_rep_attainment"),
        F.sum(F.when(F.col("status") == "On Track", 1).otherwise(0)).alias("on_track_count"),
        F.sum(F.when(F.col("status") == "At Risk", 1).otherwise(0)).alias("at_risk_count"),
        F.sum(F.when(F.col("ytd_attainment") >= 1.1, 1).otherwise(0)).alias("overachievers_count"),
        F.sum("commission_earned").alias("total_commission_paid"),
        F.round(F.sum("commission_earned") * 1.15, 0).alias("projected_commission"),
        F.sum(F.when(F.col("ytd_attainment") >= 1.0, 1).otherwise(0)).alias("accelerator_eligible")
    ).withColumn(
        "snapshot_date",
        F.current_date()
    )

    output.write_dataframe(summary)


@transform(
    reps=Input("/RevOps/Scenario/sales_reps"),
    opportunities=Input("/RevOps/Scenario/opportunities"),
    output=Output("/RevOps/Compensation/rep_attainment")
)
def compute_rep_attainment(ctx, reps, opportunities, output):
    """Generate per-rep attainment and commission data."""

    reps_df = reps.dataframe()
    opps_df = opportunities.dataframe()

    # Calculate closed won and pipeline per rep
    closed = opps_df.filter(F.col("stage_name") == "Closed Won").groupBy("owner_id").agg(
        F.sum("amount").alias("closed_won")
    )

    pipeline = opps_df.filter(~F.col("stage_name").isin("Closed Won", "Closed Lost")).groupBy("owner_id").agg(
        F.sum("amount").alias("pipeline"),
        F.sum(F.when(F.col("forecast_category") == "Commit", F.col("amount")).otherwise(0)).alias("commit")
    )

    # Join all data
    combined = reps_df.join(closed, reps_df.rep_id == closed.owner_id, "left") \
        .join(pipeline, reps_df.rep_id == pipeline.owner_id, "left") \
        .fillna({"closed_won": 0, "pipeline": 0, "commit": 0})

    # Calculate compensation metrics
    attainment = combined.withColumn(
        "attainment",
        F.round(F.col("closed_won") / F.col("quarterly_quota"), 3)
    ).withColumn(
        "commission_rate",
        F.when(F.col("attainment") >= 1.2, 0.15)
        .when(F.col("attainment") >= 1.0, 0.12)
        .otherwise(0.10)
    ).withColumn(
        "commission_earned",
        F.round(F.col("closed_won") * F.col("commission_rate"), 0)
    ).withColumn(
        "projected_commission",
        F.round((F.col("closed_won") + F.col("commit") * 0.8) * F.col("commission_rate"), 0)
    ).withColumn(
        "accelerator_tier",
        F.when(F.col("attainment") >= 1.3, "President's Club")
        .when(F.col("attainment") >= 1.2, "Platinum")
        .when(F.col("attainment") >= 1.0, "Gold")
        .otherwise("Standard")
    ).withColumn(
        "accelerator_multiplier",
        F.when(F.col("attainment") >= 1.3, 2.0)
        .when(F.col("attainment") >= 1.2, 1.5)
        .when(F.col("attainment") >= 1.0, 1.2)
        .otherwise(1.0)
    ).withColumn(
        "gap_to_accelerator",
        F.greatest(F.lit(0), F.col("quarterly_quota") - F.col("closed_won"))
    ).withColumn(
        "status",
        F.when(F.col("attainment") >= 1.0, "Achieved")
        .when(F.col("attainment") >= 0.8, "On Track")
        .otherwise("At Risk")
    ).withColumn(
        "projected_attainment",
        F.round((F.col("closed_won") + F.col("commit") * 0.7) / F.col("quarterly_quota"), 3)
    )

    output_df = attainment.select(
        F.col("rep_id"),
        F.col("rep_name"),
        F.col("region"),
        F.col("segment"),
        F.col("quarterly_quota").alias("quota"),
        F.col("closed_won"),
        F.col("attainment"),
        F.col("commission_earned"),
        F.col("projected_commission"),
        F.col("accelerator_tier"),
        F.col("accelerator_multiplier"),
        F.col("gap_to_accelerator"),
        F.col("status"),
        F.col("pipeline"),
        F.col("commit"),
        F.col("projected_attainment")
    ).orderBy(F.col("attainment").desc())

    output.write_dataframe(output_df)


@transform(
    reps=Input("/RevOps/Scenario/sales_reps"),
    output=Output("/RevOps/Compensation/attainment_trend")
)
def compute_attainment_trend(ctx, reps, output):
    """Generate attainment trend data (simulated weekly snapshots)."""

    reps_df = reps.dataframe()

    # Generate simulated weekly data (in production, this would use historical snapshots)
    weeks = [
        ("2024-10-04", 1, 0.25),
        ("2024-10-11", 2, 0.35),
        ("2024-10-18", 3, 0.48),
        ("2024-10-25", 4, 0.55),
        ("2024-11-01", 5, 0.62),
        ("2024-11-08", 6, 0.70),
        ("2024-11-15", 7, 0.78),
        ("2024-11-22", 8, 0.85)
    ]

    # Calculate current stats
    current_stats = reps_df.agg(
        F.round(F.avg("ytd_attainment"), 3).alias("avg_attainment"),
        F.count("*").alias("total_reps"),
        F.sum(F.when(F.col("ytd_attainment") >= 0.9, 1).otherwise(0)).alias("on_track"),
        F.sum(F.when(F.col("ytd_attainment") < 0.7, 1).otherwise(0)).alias("at_risk"),
        F.sum(F.when(F.col("ytd_attainment") >= 1.1, 1).otherwise(0)).alias("overachiever")
    ).collect()[0]

    total = current_stats["total_reps"]

    # Create trend DataFrame with spark
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

    schema = StructType([
        StructField("snapshot_date", StringType(), False),
        StructField("week_number", IntegerType(), False),
        StructField("team_attainment", DoubleType(), False),
        StructField("avg_rep_attainment", DoubleType(), False),
        StructField("on_track_pct", DoubleType(), False),
        StructField("at_risk_pct", DoubleType(), False),
        StructField("overachiever_pct", DoubleType(), False),
        StructField("quota_pace", DoubleType(), False)
    ])

    data = []
    for date, week, pace in weeks:
        data.append((
            date,
            week,
            round(pace * 0.95, 3),  # team attainment
            round(pace * 0.90, 3),  # avg rep attainment
            round(0.4 + pace * 0.3, 3),  # on track %
            round(0.3 - pace * 0.15, 3),  # at risk %
            round(0.1 + pace * 0.1, 3),  # overachiever %
            round(pace, 3)  # quota pace
        ))

    trend_df = ctx.spark_session.createDataFrame(data, schema)
    output.write_dataframe(trend_df)
