# /transforms/decisions/sales_interventions_schema.py
"""
SALES INTERVENTIONS SCHEMA
==========================
Defines the schema for /RevOps/Decisions/sales_interventions.

This is the closed-loop record of "we took action X on deal Y and here's what happened."
Use this dataset to track intervention effectiveness and calculate playbook ROI.

In a real deployment, rows would typically be written by:
- AIP Agents (via Foundry Actions)
- Workshop / external apps calling the Foundry API
- Automated triggers from CRM events

This transform ensures downstream analytics can rely on a stable schema.

SCHEMA REFERENCE:
-----------------
decision_id          - Primary key for the intervention
decision_ts          - When the decision/action was taken
actor_id             - ID of the actor (user or agent)
actor_name           - Human-readable name (e.g. "Michael Torres")
actor_role           - Role: CRO, Sales Manager, Rep, AIP Agent, etc.
opportunity_id       - Opportunity ID
opportunity_name     - Opportunity name at the time of decision
account_id           - Account ID
account_name         - Account name
rep_id               - Primary owner rep ID at decision time
rep_name             - Primary owner name
region               - Region (from SalesRep)
segment              - Segment (Enterprise / Mid-Market / SMB)
action_id            - FK to /RevOps/Dashboard/next_best_actions.action_id
action_type          - Mirrors action_type from ActionItem (e.g. EXEC_SPONSOR_OUTREACH)
action_source        - NextBestAction, AgentSuggestion, Manual, etc.
decision_type        - Business-friendly label: Exec outreach, Reassign, Increase discount, etc.
decision_status      - Planned, Executed, Cancelled
pre_health_score     - Health score before the intervention
post_health_score    - Health score after the intervention (when measured)
pre_health_category  - Healthy, Monitor, At Risk, Critical
post_health_category - Same categories, post-intervention
pre_forecast_category  - Pipeline, Best Case, Commit, etc.
post_forecast_category - Updated forecast category
pre_stage_name       - Stage at decision time
post_stage_name      - Stage when outcome is evaluated
pre_commit_amount    - Amount counted in commit before intervention
post_commit_amount   - Amount counted in commit after intervention
expected_impact_amount - Expected dollar impact at decision time
realized_impact_amount - Realized impact (e.g. saved pipeline, upsell)
realized_impact_type - SavedDeal, UpsideWon, SlipPrevented, NoImpact, NegativeImpact
result               - Final outcome: Won, Lost, Slip, Open
result_ts            - When the final outcome was observed
quarter              - Reporting bucket, e.g. Q4 2024
notes                - Free-text notes
"""

from transforms.api import transform, Output
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)


@transform(output=Output("/RevOps/Decisions/sales_interventions"))
def define_schema(ctx, output):
    """
    Create the sales_interventions dataset with the defined schema.
    Rows will be written via Foundry Actions, AIP Agents, or API calls.
    """
    spark = ctx.spark_session

    schema = StructType(
        [
            StructField("decision_id", StringType(), False),
            StructField("decision_ts", TimestampType(), True),
            StructField("actor_id", StringType(), True),
            StructField("actor_name", StringType(), True),
            StructField("actor_role", StringType(), True),
            StructField("opportunity_id", StringType(), True),
            StructField("opportunity_name", StringType(), True),
            StructField("account_id", StringType(), True),
            StructField("account_name", StringType(), True),
            StructField("rep_id", StringType(), True),
            StructField("rep_name", StringType(), True),
            StructField("region", StringType(), True),
            StructField("segment", StringType(), True),
            StructField("action_id", StringType(), True),
            StructField("action_type", StringType(), True),
            StructField("action_source", StringType(), True),
            StructField("decision_type", StringType(), True),
            StructField("decision_status", StringType(), True),
            StructField("pre_health_score", IntegerType(), True),
            StructField("post_health_score", IntegerType(), True),
            StructField("pre_health_category", StringType(), True),
            StructField("post_health_category", StringType(), True),
            StructField("pre_forecast_category", StringType(), True),
            StructField("post_forecast_category", StringType(), True),
            StructField("pre_stage_name", StringType(), True),
            StructField("post_stage_name", StringType(), True),
            StructField("pre_commit_amount", DoubleType(), True),
            StructField("post_commit_amount", DoubleType(), True),
            StructField("expected_impact_amount", DoubleType(), True),
            StructField("realized_impact_amount", DoubleType(), True),
            StructField("realized_impact_type", StringType(), True),
            StructField("result", StringType(), True),
            StructField("result_ts", TimestampType(), True),
            StructField("quarter", StringType(), True),
            StructField("notes", StringType(), True),
        ]
    )

    # Empty dataframe with correct schema - rows come from writeback flows
    df = spark.createDataFrame([], schema)
    output.write_dataframe(df)
