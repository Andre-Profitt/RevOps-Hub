# /transforms/analytics/action_impact_tracker.py
"""
ACTION IMPACT TRACKER
=====================

Summarises how the Action Inbox is performing:
- How many actions are created vs actioned
- Time to decision
- Expected vs realized impact

Use cases:
- RevOps program dashboard: "This quarter: 182 actions created, 137 actioned
  (75% decision rate), avg time-to-decision 2.3 days, realized vs expected = 0.92x."
- Regional view: Filter by region/segment to see where managers are burning down
  the inbox vs letting it rot.

Inputs:
- /RevOps/Dashboard/next_best_actions
- /RevOps/Decisions/sales_interventions

Output:
- /RevOps/Inbox/impact_tracker
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F


@transform(
    actions=Input("/RevOps/Dashboard/next_best_actions"),
    interventions=Input("/RevOps/Decisions/sales_interventions"),
    output=Output("/RevOps/Inbox/impact_tracker"),
)
def compute(ctx, actions, interventions, output):
    actions_df = actions.dataframe()
    interventions_df = interventions.dataframe()

    if actions_df.rdd.isEmpty():
        empty = actions_df.limit(0)
        output.write_dataframe(empty)
        return

    # Only need a subset of columns from interventions
    interv_small = interventions_df.select(
        "action_id",
        "decision_ts",
        "realized_impact_amount",
        "result",
        "quarter",
    )

    joined = actions_df.join(interv_small, on="action_id", how="left")

    # Derive some helper fields
    joined = joined.withColumn(
        "has_decision",
        F.when(F.col("decision_ts").isNotNull(), F.lit(1)).otherwise(F.lit(0)),
    ).withColumn(
        "time_to_decision_days",
        F.when(
            F.col("decision_ts").isNotNull(),
            F.datediff(F.col("decision_ts"), F.to_date("created_at")),
        ),
    ).withColumn(
        "quarter",
        F.coalesce(F.col("quarter"), F.lit("Unspecified")),
    )

    # Group by quarter, region, segment for exec / RevOps program views
    group_cols = ["quarter", "region", "segment"]

    aggregated = (
        joined.groupBy(*group_cols)
        .agg(
            F.countDistinct("action_id").alias("actions_created"),
            F.sum("has_decision").alias("actions_with_decision"),
            F.countDistinct(
                F.when(F.col("has_decision") == 0, F.col("action_id"))
            ).alias("actions_without_decision"),
            F.avg("time_to_decision_days").alias("avg_time_to_decision_days"),
            F.sum("expected_impact_amount").alias("total_expected_impact_amount"),
            F.sum("realized_impact_amount").alias("total_realized_impact_amount"),
            F.sum(F.when(F.col("result") == "Won", 1).otherwise(0)).alias("deals_won"),
            F.sum(F.when(F.col("result") == "Lost", 1).otherwise(0)).alias("deals_lost"),
            F.sum(F.when(F.col("result") == "Slip", 1).otherwise(0)).alias("deals_slipped"),
        )
        .withColumn(
            "decision_rate",
            F.when(
                F.col("actions_created") > 0,
                F.col("actions_with_decision") / F.col("actions_created"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "realized_vs_expected_ratio",
            F.when(
                F.col("total_expected_impact_amount") > 0,
                F.col("total_realized_impact_amount")
                / F.col("total_expected_impact_amount"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("last_updated_ts", F.current_timestamp())
    )

    output.write_dataframe(aggregated)
