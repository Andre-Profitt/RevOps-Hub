# /transforms/commercial/implementation_status.py
"""
IMPLEMENTATION STATUS TRACKING
==============================
Tracks rollout state per tenant for the implementation flywheel.

Stages:
- ONBOARDING: Initial setup and contract signed
- CONNECTOR_SETUP: CRM and data source connections
- DASHBOARD_CONFIG: Dashboard customization
- AGENT_TRAINING: AI agent configuration and training
- LIVE: Production deployment complete

Outputs:
- /RevOps/Commercial/implementation_status: Current implementation state per tenant
- /RevOps/Commercial/implementation_history: Historical stage transitions
"""

from transforms.api import transform, Input, Output
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    TimestampType, BooleanType, ArrayType, MapType
)
from datetime import datetime, timedelta


# =============================================================================
# IMPLEMENTATION STAGES
# =============================================================================

class ImplementationStage:
    """Implementation stages with order and criteria."""
    ONBOARDING = "onboarding"
    CONNECTOR_SETUP = "connector_setup"
    DASHBOARD_CONFIG = "dashboard_config"
    AGENT_TRAINING = "agent_training"
    LIVE = "live"

    ORDER = [ONBOARDING, CONNECTOR_SETUP, DASHBOARD_CONFIG, AGENT_TRAINING, LIVE]

    @classmethod
    def get_order(cls, stage: str) -> int:
        """Get numeric order for a stage."""
        try:
            return cls.ORDER.index(stage) + 1
        except ValueError:
            return 0


# Stage completion criteria
STAGE_CRITERIA = {
    ImplementationStage.ONBOARDING: {
        "description": "Contract signed, workspace created",
        "required": ["workspace_created", "admin_user_added"],
    },
    ImplementationStage.CONNECTOR_SETUP: {
        "description": "CRM and data sources connected",
        "required": ["crm_connected", "first_sync_complete"],
    },
    ImplementationStage.DASHBOARD_CONFIG: {
        "description": "Core dashboards configured",
        "required": ["pipeline_dashboard_enabled", "hygiene_dashboard_enabled"],
    },
    ImplementationStage.AGENT_TRAINING: {
        "description": "AI agents configured and tested",
        "required": ["ops_agent_enabled", "agent_first_action"],
    },
    ImplementationStage.LIVE: {
        "description": "Production deployment complete",
        "required": ["go_live_approved", "cs_handoff_complete"],
    },
}


# =============================================================================
# IMPLEMENTATION STATUS (Current State)
# =============================================================================

@transform(
    customer_config=Input("/RevOps/Config/customer_settings"),
    sync_status=Input("/RevOps/Monitoring/sync_status"),
    build_metrics=Input("/RevOps/Monitoring/build_metrics"),
    feature_usage=Input("/RevOps/Telemetry/feature_usage"),
    agent_actions=Input("/RevOps/Agents/action_log"),
    previous_status=Input("/RevOps/Commercial/implementation_status"),
    output=Output("/RevOps/Commercial/implementation_status"),
)
def compute_implementation_status(
    ctx,
    customer_config: DataFrame,
    sync_status: DataFrame,
    build_metrics: DataFrame,
    feature_usage: DataFrame,
    agent_actions: DataFrame,
    previous_status: DataFrame,
    output,
):
    """
    Compute current implementation status per tenant.
    Evaluates stage criteria based on actual system state.
    """
    spark = ctx.spark_session
    now = datetime.utcnow()

    # Get customer list with creation dates
    customers = customer_config.select(
        "customer_id",
        "customer_name",
        "tier",
        F.coalesce(F.col("created_at"), F.lit(now - timedelta(days=30))).alias("onboarded_at"),
    )

    # ----- Check ONBOARDING criteria -----
    # Workspace created = customer exists in config
    onboarding_checks = customers.withColumn(
        "workspace_created", F.lit(True)
    ).withColumn(
        "admin_user_added",
        F.when(F.col("customer_id").isNotNull(), True).otherwise(False)
    )

    # ----- Check CONNECTOR_SETUP criteria -----
    if sync_status.count() > 0:
        connector_checks = sync_status.groupBy("customer_id").agg(
            F.max(F.when(F.col("sync_type") == "crm", True).otherwise(False)).alias("crm_connected"),
            F.max(F.when(
                (F.col("sync_type") == "crm") & (F.col("status") == "success"),
                True
            ).otherwise(False)).alias("first_sync_complete"),
            F.max("last_sync_at").alias("last_sync_at"),
        )
    else:
        connector_checks = spark.createDataFrame([], StructType([
            StructField("customer_id", StringType(), True),
            StructField("crm_connected", BooleanType(), True),
            StructField("first_sync_complete", BooleanType(), True),
            StructField("last_sync_at", TimestampType(), True),
        ]))

    # ----- Check DASHBOARD_CONFIG criteria -----
    if feature_usage.count() > 0:
        dashboard_checks = feature_usage.filter(
            F.col("feature_name").isin([
                "pipeline_health_dashboard",
                "pipeline_hygiene_dashboard",
                "forecasting_dashboard",
            ])
        ).groupBy("customer_id").agg(
            F.max(F.when(
                F.col("feature_name") == "pipeline_health_dashboard", True
            ).otherwise(False)).alias("pipeline_dashboard_enabled"),
            F.max(F.when(
                F.col("feature_name") == "pipeline_hygiene_dashboard", True
            ).otherwise(False)).alias("hygiene_dashboard_enabled"),
            F.countDistinct("feature_name").alias("dashboards_enabled_count"),
        )
    else:
        dashboard_checks = spark.createDataFrame([], StructType([
            StructField("customer_id", StringType(), True),
            StructField("pipeline_dashboard_enabled", BooleanType(), True),
            StructField("hygiene_dashboard_enabled", BooleanType(), True),
            StructField("dashboards_enabled_count", IntegerType(), True),
        ]))

    # ----- Check AGENT_TRAINING criteria -----
    if agent_actions.count() > 0:
        agent_checks = agent_actions.groupBy("customer_id").agg(
            F.lit(True).alias("ops_agent_enabled"),
            F.max(F.when(F.col("action_type").isNotNull(), True).otherwise(False)).alias("agent_first_action"),
            F.count("*").alias("total_agent_actions"),
            F.max("executed_at").alias("last_agent_action_at"),
        )
    else:
        agent_checks = spark.createDataFrame([], StructType([
            StructField("customer_id", StringType(), True),
            StructField("ops_agent_enabled", BooleanType(), True),
            StructField("agent_first_action", BooleanType(), True),
            StructField("total_agent_actions", IntegerType(), True),
            StructField("last_agent_action_at", TimestampType(), True),
        ]))

    # ----- Get previous go-live status (manual approval) -----
    if previous_status.count() > 0:
        golive_checks = previous_status.select(
            "customer_id",
            "go_live_approved",
            "go_live_date",
            "cs_handoff_complete",
            "cs_owner",
        )
    else:
        golive_checks = spark.createDataFrame([], StructType([
            StructField("customer_id", StringType(), True),
            StructField("go_live_approved", BooleanType(), True),
            StructField("go_live_date", TimestampType(), True),
            StructField("cs_handoff_complete", BooleanType(), True),
            StructField("cs_owner", StringType(), True),
        ]))

    # ----- Combine all checks -----
    status = onboarding_checks \
        .join(connector_checks, "customer_id", "left") \
        .join(dashboard_checks, "customer_id", "left") \
        .join(agent_checks, "customer_id", "left") \
        .join(golive_checks, "customer_id", "left")

    # Fill nulls with defaults
    status = status.fillna({
        "workspace_created": False,
        "admin_user_added": False,
        "crm_connected": False,
        "first_sync_complete": False,
        "pipeline_dashboard_enabled": False,
        "hygiene_dashboard_enabled": False,
        "dashboards_enabled_count": 0,
        "ops_agent_enabled": False,
        "agent_first_action": False,
        "total_agent_actions": 0,
        "go_live_approved": False,
        "cs_handoff_complete": False,
    })

    # ----- Determine current stage -----
    status = status.withColumn(
        "current_stage",
        F.when(
            (F.col("go_live_approved") == True) & (F.col("cs_handoff_complete") == True),
            F.lit(ImplementationStage.LIVE)
        ).when(
            (F.col("ops_agent_enabled") == True) & (F.col("agent_first_action") == True),
            F.lit(ImplementationStage.AGENT_TRAINING)
        ).when(
            (F.col("pipeline_dashboard_enabled") == True) & (F.col("hygiene_dashboard_enabled") == True),
            F.lit(ImplementationStage.DASHBOARD_CONFIG)
        ).when(
            (F.col("crm_connected") == True) & (F.col("first_sync_complete") == True),
            F.lit(ImplementationStage.CONNECTOR_SETUP)
        ).otherwise(F.lit(ImplementationStage.ONBOARDING))
    )

    # Calculate stage order for sorting
    status = status.withColumn(
        "stage_order",
        F.when(F.col("current_stage") == ImplementationStage.LIVE, 5)
        .when(F.col("current_stage") == ImplementationStage.AGENT_TRAINING, 4)
        .when(F.col("current_stage") == ImplementationStage.DASHBOARD_CONFIG, 3)
        .when(F.col("current_stage") == ImplementationStage.CONNECTOR_SETUP, 2)
        .otherwise(1)
    )

    # Calculate completion percentage
    status = status.withColumn(
        "completion_pct",
        (F.col("stage_order").cast("double") / 5.0) * 100
    )

    # Determine next stage and blockers
    status = status.withColumn(
        "next_stage",
        F.when(F.col("current_stage") == ImplementationStage.ONBOARDING, ImplementationStage.CONNECTOR_SETUP)
        .when(F.col("current_stage") == ImplementationStage.CONNECTOR_SETUP, ImplementationStage.DASHBOARD_CONFIG)
        .when(F.col("current_stage") == ImplementationStage.DASHBOARD_CONFIG, ImplementationStage.AGENT_TRAINING)
        .when(F.col("current_stage") == ImplementationStage.AGENT_TRAINING, ImplementationStage.LIVE)
        .otherwise(F.lit(None))
    )

    # Days in current stage
    status = status.withColumn(
        "days_in_stage",
        F.datediff(F.lit(now), F.coalesce(F.col("onboarded_at"), F.lit(now)))
    )

    # Is blocked flag (more than 14 days in non-live stage)
    status = status.withColumn(
        "is_blocked",
        (F.col("current_stage") != ImplementationStage.LIVE) & (F.col("days_in_stage") > 14)
    )

    # Add timestamps
    status = status.withColumn("evaluated_at", F.lit(now))

    # Select final columns
    result = status.select(
        "customer_id",
        "customer_name",
        "tier",
        "current_stage",
        "stage_order",
        "completion_pct",
        "next_stage",
        "days_in_stage",
        "is_blocked",
        "onboarded_at",
        # Stage completion flags
        "workspace_created",
        "admin_user_added",
        "crm_connected",
        "first_sync_complete",
        "last_sync_at",
        "pipeline_dashboard_enabled",
        "hygiene_dashboard_enabled",
        "dashboards_enabled_count",
        "ops_agent_enabled",
        "agent_first_action",
        "total_agent_actions",
        "last_agent_action_at",
        "go_live_approved",
        "go_live_date",
        "cs_handoff_complete",
        "cs_owner",
        "evaluated_at",
    )

    output.write_dataframe(result)


# =============================================================================
# IMPLEMENTATION HISTORY (Stage Transitions)
# =============================================================================

@transform(
    current_status=Input("/RevOps/Commercial/implementation_status"),
    previous_history=Input("/RevOps/Commercial/implementation_history"),
    output=Output("/RevOps/Commercial/implementation_history"),
)
def track_implementation_history(
    ctx,
    current_status: DataFrame,
    previous_history: DataFrame,
    output,
):
    """
    Track historical stage transitions for implementation timeline.
    """
    spark = ctx.spark_session
    now = datetime.utcnow()

    # Get current stages
    current_stages = current_status.select(
        "customer_id",
        "current_stage",
        "evaluated_at",
    ).withColumnRenamed("current_stage", "stage")

    # Schema for history
    history_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("stage", StringType(), True),
        StructField("entered_at", TimestampType(), True),
        StructField("exited_at", TimestampType(), True),
        StructField("duration_days", IntegerType(), True),
    ])

    if previous_history.count() > 0:
        # Get last recorded stage per customer
        window = Window.partitionBy("customer_id").orderBy(F.desc("entered_at"))
        last_stages = previous_history.withColumn(
            "row_num", F.row_number().over(window)
        ).filter(F.col("row_num") == 1).drop("row_num")

        # Find stage changes
        stage_changes = current_stages.join(
            last_stages.select("customer_id", F.col("stage").alias("prev_stage"), "entered_at"),
            "customer_id",
            "left"
        )

        # New transitions (stage changed)
        new_transitions = stage_changes.filter(
            (F.col("stage") != F.col("prev_stage")) | F.col("prev_stage").isNull()
        ).select(
            "customer_id",
            "stage",
            F.col("evaluated_at").alias("entered_at"),
            F.lit(None).cast(TimestampType()).alias("exited_at"),
            F.lit(None).cast(IntegerType()).alias("duration_days"),
        )

        # Update exit timestamps for old stages
        updated_history = previous_history.join(
            new_transitions.select("customer_id").distinct(),
            "customer_id",
            "left_semi"
        ).withColumn(
            "exited_at",
            F.when(F.col("exited_at").isNull(), F.lit(now)).otherwise(F.col("exited_at"))
        ).withColumn(
            "duration_days",
            F.when(
                F.col("duration_days").isNull(),
                F.datediff(F.col("exited_at"), F.col("entered_at"))
            ).otherwise(F.col("duration_days"))
        )

        # Combine with unchanged history
        unchanged_history = previous_history.join(
            new_transitions.select("customer_id").distinct(),
            "customer_id",
            "left_anti"
        )

        result = updated_history.unionByName(unchanged_history).unionByName(new_transitions)
    else:
        # First run - create initial history entries
        result = current_stages.select(
            "customer_id",
            "stage",
            F.col("evaluated_at").alias("entered_at"),
            F.lit(None).cast(TimestampType()).alias("exited_at"),
            F.lit(None).cast(IntegerType()).alias("duration_days"),
        )

    output.write_dataframe(result)


# =============================================================================
# IMPLEMENTATION SUMMARY (Aggregated View)
# =============================================================================

@transform(
    implementation_status=Input("/RevOps/Commercial/implementation_status"),
    output=Output("/RevOps/Commercial/implementation_summary"),
)
def compute_implementation_summary(ctx, implementation_status: DataFrame, output):
    """
    Compute aggregated implementation metrics for executive view.
    """
    spark = ctx.spark_session
    now = datetime.utcnow()

    # Count by stage
    stage_counts = implementation_status.groupBy("current_stage").agg(
        F.count("*").alias("customer_count"),
        F.avg("days_in_stage").alias("avg_days_in_stage"),
        F.sum(F.when(F.col("is_blocked"), 1).otherwise(0)).alias("blocked_count"),
    )

    # Count by tier
    tier_counts = implementation_status.groupBy("tier", "current_stage").agg(
        F.count("*").alias("customer_count"),
    )

    # Overall metrics
    total_customers = implementation_status.count()
    live_customers = implementation_status.filter(
        F.col("current_stage") == ImplementationStage.LIVE
    ).count()
    blocked_customers = implementation_status.filter(F.col("is_blocked") == True).count()

    avg_completion = implementation_status.agg(
        F.avg("completion_pct").alias("avg_completion_pct")
    ).collect()[0]["avg_completion_pct"] or 0

    # Create summary
    summary_data = [{
        "generated_at": now,
        "total_customers": total_customers,
        "live_customers": live_customers,
        "blocked_customers": blocked_customers,
        "avg_completion_pct": avg_completion,
        "live_pct": (live_customers / total_customers * 100) if total_customers > 0 else 0,
        "blocked_pct": (blocked_customers / total_customers * 100) if total_customers > 0 else 0,
    }]

    summary_df = spark.createDataFrame(summary_data)

    # Add stage breakdown as array
    stage_breakdown = stage_counts.collect()
    stage_json = {row["current_stage"]: row["customer_count"] for row in stage_breakdown}

    summary_df = summary_df.withColumn(
        "stage_breakdown",
        F.lit(str(stage_json))
    )

    output.write_dataframe(summary_df)
