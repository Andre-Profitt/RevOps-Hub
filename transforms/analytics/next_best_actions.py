# /transforms/analytics/next_best_actions.py
"""
NEXT BEST ACTIONS
=================
Generates prioritized action items from deal health and rep context.

Outputs:
- /RevOps/Dashboard/next_best_actions   (exec / manager Action Inbox)
- /RevOps/Coaching/next_best_actions    (rep-specific action list)
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window


SCENARIO_DATE_STR = "2024-11-15"  # Keep consistent with scenario generators


def _build_base_actions(deal_health_df, reps_df):
    """
    Build a base actions dataframe:
    1 row per opportunity that needs attention, with priority & suggested action.
    """

    # Only open deals that need attention or are hero deals in trouble
    candidates = deal_health_df.filter(
        (F.col("needs_attention") == True) |  # noqa: E712
        ((F.col("is_hero_deal") == True) & (F.col("health_category").isin("At Risk", "Critical")))  # noqa: E712
    )

    # Join rep context to get region / segment (owner_id -> rep_id)
    reps_small = reps_df.select(
        "rep_id",
        "rep_name",
        "region",
        "segment",
    )

    joined = candidates.join(
        reps_small,
        candidates.owner_id == reps_small.rep_id,
        "left",
    )

    # Derive an action_type based on risk pattern
    action_type = (
        F.when(
            (F.col("is_hero_deal") == True) & (F.col("health_category") == "Critical"),  # noqa: E712
            F.lit("EXEC_SPONSOR_OUTREACH"),
        )
        .when(
            (F.col("days_since_activity") > 7),
            F.lit("REENGAGE_CONTACTS"),
        )
        .when(
            (F.col("stakeholder_count") < 2),
            F.lit("MULTI_THREAD_DEAL"),
        )
        .when(
            (F.col("is_competitive") == True),  # noqa: E712
            F.lit("COMPETITIVE_STRATEGY_REVIEW"),
        )
        .otherwise(F.lit("MANAGER_DEAL_REVIEW"))
    )

    # Human-friendly title
    action_title = (
        F.when(
            F.col("is_hero_deal") == True,  # noqa: E712
            F.concat(F.lit("Save Hero Deal: "), F.col("account_name")),
        )
        .when(
            F.col("health_category") == "Critical",
            F.concat(F.lit("Critical Deal Intervention: "), F.col("account_name")),
        )
        .when(
            F.col("health_category") == "At Risk",
            F.concat(F.lit("At Risk Deal: "), F.col("account_name")),
        )
        .otherwise(F.concat(F.lit("Follow Up: "), F.col("account_name")))
    )

    # Description based on pattern
    action_description = (
        F.when(
            action_type == "EXEC_SPONSOR_OUTREACH",
            F.lit(
                "Executive sponsor to re-engage champion and unblock negotiations "
                "on a critical hero deal."
            ),
        )
        .when(
            action_type == "REENGAGE_CONTACTS",
            F.lit(
                "Deal has gone quiet. Schedule outreach to primary contacts and "
                "reconfirm timeline and next steps."
            ),
        )
        .when(
            action_type == "MULTI_THREAD_DEAL",
            F.lit(
                "Single-threaded deal. Identify and engage at least 2 additional "
                "stakeholders (economic + technical)."
            ),
        )
        .when(
            action_type == "COMPETITIVE_STRATEGY_REVIEW",
            F.lit(
                "Competitor identified. Align on differentiation story and send "
                "targeted follow-up."
            ),
        )
        .otherwise(
            F.lit(
                "Manager to review deal strategy and unblock process bottlenecks "
                "before end of quarter."
            )
        )
    )

    # Priority: 100 = highest
    priority = (
        F.when(F.col("health_category") == "Critical", 100)
        .when(F.col("health_category") == "At Risk", 80)
        .when(F.col("health_category") == "Monitor", 60)
        .otherwise(40)
    )

    # Extra boost for large / hero deals
    priority = (
        priority
        + F.when(F.col("is_hero_deal") == True, 10).otherwise(0)  # noqa: E712
        + F.when(F.col("amount") >= 500000, 5).otherwise(0)
    )

    # Expected impact in dollars = portion of amount we expect to protect / unlock
    expected_impact_amount = (
        F.col("amount")
        * F.when(F.col("health_category") == "Critical", 0.6)
        .when(F.col("health_category") == "At Risk", 0.4)
        .when(F.col("health_category") == "Monitor", 0.2)
        .otherwise(0.1)
    )

    expected_impact_text = F.concat(
        F.lit("Protect approximately $"),
        F.format_number(expected_impact_amount, 0),
        F.lit(" in at-risk pipeline."),
    )

    # Confidence heuristic
    confidence = (
        F.when(F.col("health_category") == "Critical", 0.8)
        .when(F.col("health_category") == "At Risk", 0.75)
        .when(F.col("health_category") == "Monitor", 0.6)
        .otherwise(0.5)
    )

    # Due date based on urgency
    due_in_days = (
        F.when(F.col("health_category") == "Critical", 2)
        .when(F.col("health_category") == "At Risk", 5)
        .otherwise(7)
    )
    due_date = F.date_add(F.to_date(F.lit(SCENARIO_DATE_STR)), due_in_days)

    # Unique action ID (deterministic for this demo)
    row_num = F.row_number().over(Window.orderBy(F.col("opportunity_id")))
    action_id = F.concat(F.lit("ACT-"), F.col("opportunity_id"), F.lit("-"), row_num)

    base = (
        joined.withColumn("action_type", action_type)
        .withColumn("action_title", action_title)
        .withColumn("action_description", action_description)
        .withColumn("priority", priority.cast("int"))
        .withColumn("expected_impact_amount", F.round(expected_impact_amount, 0))
        .withColumn("expected_impact", expected_impact_text)
        .withColumn("confidence", confidence)
        .withColumn("reasoning", F.col("story_notes"))
        .withColumn("due_date", due_date)
        .withColumn("created_at", F.to_timestamp(F.lit(f"{SCENARIO_DATE_STR} 08:00:00")))
        .withColumn("status", F.lit("Open"))
        .withColumn("source", F.lit("RevOps-Hub-HealthModel"))
        .withColumn(
            "playbook",
            F.when(action_type == "EXEC_SPONSOR_OUTREACH", F.lit("Exec Save Play"))
            .when(action_type == "MULTI_THREAD_DEAL", F.lit("Multi-threading Play"))
            .when(action_type == "REENGAGE_CONTACTS", F.lit("Re-engagement Play"))
            .otherwise(F.lit("Manager Review Play")),
        )
        .withColumn("action_id", action_id)
    )

    return base


@transform(
    deal_health=Input("/RevOps/Enriched/deal_health_scores"),
    reps=Input("/RevOps/Scenario/sales_reps"),
    dashboard_actions=Output("/RevOps/Dashboard/next_best_actions"),
    coaching_actions=Output("/RevOps/Coaching/next_best_actions"),
)
def compute(ctx, deal_health, reps, dashboard_actions, coaching_actions):
    """
    Generate action items for dashboard + rep coaching.
    """

    deal_health_df = deal_health.dataframe()
    reps_df = reps.dataframe()

    base = _build_base_actions(deal_health_df, reps_df)

    # -----------------------------
    # Dashboard-level actions
    # -----------------------------
    dashboard_df = base.select(
        "action_id",
        "opportunity_id",
        "account_id",
        "account_name",
        "opportunity_name",
        F.col("rep_id"),
        F.col("rep_name").alias("owner_name"),
        "region",
        "segment",
        "action_type",
        "action_title",
        "action_description",
        "priority",
        "expected_impact",
        "expected_impact_amount",
        "confidence",
        "reasoning",
        "due_date",
        "created_at",
        "status",
        "source",
        "playbook",
        "amount",
        "health_score",
        "health_category",
        "risk_level",
        "needs_attention",
    )

    dashboard_actions.write_dataframe(dashboard_df)

    # -----------------------------
    # Rep coaching actions
    # -----------------------------
    coaching_df = base.select(
        F.col("rep_id").alias("rep_id"),
        "opportunity_id",
        "account_name",
        "action_type",
        "action_description",
        "priority",
        "expected_impact",
        "due_date",
    )

    coaching_actions.write_dataframe(coaching_df)
