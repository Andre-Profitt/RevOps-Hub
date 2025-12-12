# /transforms/analytics/intervention_effectiveness.py
"""
INTERVENTION EFFECTIVENESS
==========================

Aggregates closed-loop intervention data to show:
- Which decision types and playbooks actually move the needle
- How health scores, win rates, and impact change after actions

Use cases:
- Exec/QBR slide: "Exec outreach play improved avg health by +18 points
  and converted 42% of at-risk hero deals."
- Program tuning: Filter by actor_role to see whose interventions drive lift.

Inputs:
- /RevOps/Decisions/sales_interventions
- /RevOps/Dashboard/next_best_actions

Output:
- /RevOps/Analytics/intervention_effectiveness
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F


@transform(
    interventions=Input("/RevOps/Decisions/sales_interventions"),
    actions=Input("/RevOps/Dashboard/next_best_actions"),
    output=Output("/RevOps/Analytics/intervention_effectiveness"),
)
def compute(ctx, interventions, actions, output):
    interventions_df = interventions.dataframe()
    actions_df = actions.dataframe()

    if interventions_df.rdd.isEmpty():
        # If nothing has been logged yet, emit a single empty row
        # so downstream dashboards don't error out.
        empty = interventions_df.limit(0)
        output.write_dataframe(empty)
        return

    # Pull playbook & priority from action items (if the decision came from an ActionItem)
    actions_small = actions_df.select(
        "action_id",
        "playbook",
        "priority",
    )

    df = interventions_df.join(actions_small, on="action_id", how="left")

    # Normalise some dimensions
    df = df.withColumn(
        "playbook",
        F.coalesce(F.col("playbook"), F.lit("No Playbook")),
    ).withColumn(
        "decision_type",
        F.coalesce(F.col("decision_type"), F.lit("Unspecified")),
    ).withColumn(
        "actor_role",
        F.coalesce(F.col("actor_role"), F.lit("Unknown")),
    ).withColumn(
        "quarter",
        F.coalesce(F.col("quarter"), F.lit("Unspecified")),
    )

    group_cols = [
        "quarter",
        "decision_type",
        "playbook",
        "actor_role",
    ]

    aggregated = (
        df.groupBy(*group_cols)
        .agg(
            F.count("*").alias("decisions"),
            F.countDistinct("opportunity_id").alias("deals_impacted"),
            F.sum(F.when(F.col("result") == "Won", 1).otherwise(0)).alias("deals_won"),
            F.sum(F.when(F.col("result") == "Lost", 1).otherwise(0)).alias("deals_lost"),
            F.sum(F.when(F.col("result") == "Slip", 1).otherwise(0)).alias("deals_slipped"),
            F.sum(F.when(F.col("result") == "Open", 1).otherwise(0)).alias("deals_still_open"),
            F.avg("pre_health_score").alias("avg_pre_health_score"),
            F.avg("post_health_score").alias("avg_post_health_score"),
            F.avg("expected_impact_amount").alias("avg_expected_impact_amount"),
            F.sum("expected_impact_amount").alias("total_expected_impact_amount"),
            F.avg("realized_impact_amount").alias("avg_realized_impact_amount"),
            F.sum("realized_impact_amount").alias("total_realized_impact_amount"),
        )
        .withColumn(
            "health_delta",
            F.col("avg_post_health_score") - F.col("avg_pre_health_score"),
        )
        .withColumn(
            "win_rate",
            F.when(
                F.col("deals_impacted") > 0,
                F.col("deals_won") / F.col("deals_impacted"),
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
    )

    output.write_dataframe(aggregated)
