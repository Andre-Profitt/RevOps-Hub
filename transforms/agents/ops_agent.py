"""
OPS AGENT - Autonomous Pipeline Operations
==========================================
AIOps agent that monitors pipeline events and executes automated remediation
actions. Designed to run as a Foundry Code Workbook or AIP Agent.

Phase 2: Autonomous Ops Agent Implementation

Capabilities:
- Monitor ops_agent_queue for pending actions
- Execute remediation actions (rebuild, notify, escalate)
- Track action status and outcomes
- Log decisions for audit and improvement
- Escalate to human operators when confidence is low

Action Types:
- TRIGGER_REBUILD: Restart failed or stale builds
- CHECK_UPSTREAM_DATA: Verify upstream data sources
- OPTIMIZE_TRANSFORM: Flag transforms for optimization
- REVIEW_DATA_QUALITY: Create data quality tickets
- ESCALATE_TO_HUMAN: Notify ops team for manual intervention
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    BooleanType, DoubleType, IntegerType
)
from datetime import datetime
import json
import uuid


# =============================================================================
# AGENT CONFIGURATION
# =============================================================================

AGENT_CONFIG = {
    "name": "RevOps Pipeline Ops Agent",
    "version": "1.0.0",
    "confidence_threshold": 0.7,  # Minimum confidence to auto-execute
    "max_concurrent_actions": 5,
    "escalation_timeout_minutes": 30,
    "retry_limit": 3,

    # Action-specific settings
    "actions": {
        "TRIGGER_REBUILD": {
            "auto_execute": True,
            "max_retries": 3,
            "cooldown_minutes": 15,
            "confidence_boost": 0.1
        },
        "CHECK_UPSTREAM_DATA": {
            "auto_execute": True,
            "max_retries": 1,
            "cooldown_minutes": 30,
            "confidence_boost": 0.0
        },
        "OPTIMIZE_TRANSFORM": {
            "auto_execute": False,  # Requires human review
            "max_retries": 0,
            "cooldown_minutes": 1440,  # 24 hours
            "confidence_boost": -0.2
        },
        "REVIEW_DATA_QUALITY": {
            "auto_execute": False,
            "max_retries": 0,
            "cooldown_minutes": 720,  # 12 hours
            "confidence_boost": -0.1
        }
    },

    # Escalation rules
    "escalation": {
        "critical_always_notify": True,
        "max_queue_depth": 20,
        "stale_action_hours": 2,
        "channels": ["slack", "email", "pagerduty"]
    }
}


def compute_action_confidence(event_type: str, severity: str,
                               failure_count: int, suggested_action: str) -> float:
    """
    Compute confidence score for automated action execution.

    Factors:
    - Event type severity
    - Historical success rate for this action
    - Number of previous failures
    - Action type risk level
    """
    base_confidence = 0.8

    # Adjust for severity
    if severity == "critical":
        base_confidence -= 0.1  # More cautious with critical
    elif severity == "info":
        base_confidence += 0.1

    # Adjust for failure history
    if failure_count > 2:
        base_confidence -= 0.2
    elif failure_count > 0:
        base_confidence -= 0.1

    # Action-specific adjustments
    action_config = AGENT_CONFIG["actions"].get(suggested_action, {})
    base_confidence += action_config.get("confidence_boost", 0.0)

    # Clamp to [0.0, 1.0]
    return max(0.0, min(1.0, base_confidence))


def should_auto_execute(action: str, confidence: float) -> bool:
    """Determine if action should be auto-executed."""
    action_config = AGENT_CONFIG["actions"].get(action, {})
    if not action_config.get("auto_execute", False):
        return False
    return confidence >= AGENT_CONFIG["confidence_threshold"]


# =============================================================================
# AGENT DECISION TRANSFORM
# =============================================================================

@transform(
    queue=Input("/RevOps/Monitoring/ops_agent_queue"),
    action_log=Input("/RevOps/Monitoring/agent_action_log"),
    output=Output("/RevOps/Monitoring/agent_decisions")
)
def make_agent_decisions(ctx, queue, action_log, output):
    """
    Process the ops agent queue and make decisions on each action.

    For each pending action:
    1. Compute confidence score
    2. Check if auto-execute is appropriate
    3. Generate decision record
    4. Flag for human review if needed
    """

    spark = ctx.spark_session
    queue_df = queue.dataframe()
    log_df = action_log.dataframe()

    # Get pending actions
    pending = queue_df.filter(F.col("action_status") == "pending")

    if pending.count() == 0:
        # No pending actions - write empty result
        empty_schema = StructType([
            StructField("decision_id", StringType(), False),
            StructField("event_id", StringType(), False),
            StructField("dataset_path", StringType(), False),
            StructField("suggested_action", StringType(), False),
            StructField("confidence", DoubleType(), False),
            StructField("decision", StringType(), False),
            StructField("decision_reason", StringType(), True),
            StructField("decision_ts", TimestampType(), False),
            StructField("requires_human_review", BooleanType(), False),
            StructField("escalation_channel", StringType(), True),
        ])
        output.write_dataframe(spark.createDataFrame([], empty_schema))
        return

    # Count previous failures per dataset
    failure_counts = log_df.filter(
        F.col("outcome") == "failed"
    ).groupBy("dataset_path").agg(
        F.count("*").alias("failure_count")
    )

    # Join with failure history
    pending_with_history = pending.join(
        failure_counts,
        on="dataset_path",
        how="left"
    ).fillna(0, subset=["failure_count"])

    # Collect and process decisions
    decisions = []
    decision_time = datetime.now()

    for row in pending_with_history.collect():
        event_id = row["event_id"]
        dataset_path = row["dataset_path"]
        suggested_action = row["suggested_action"]
        severity = row["severity"]
        failure_count = row["failure_count"]

        # Compute confidence
        confidence = compute_action_confidence(
            row["event_type"],
            severity,
            failure_count,
            suggested_action
        )

        # Determine decision
        auto_execute = should_auto_execute(suggested_action, confidence)

        if auto_execute:
            decision = "auto_execute"
            decision_reason = f"Confidence {confidence:.2f} above threshold"
            requires_human = False
            escalation_channel = None
        elif confidence >= 0.5:
            decision = "recommend"
            decision_reason = f"Confidence {confidence:.2f} - recommend with review"
            requires_human = True
            escalation_channel = "slack"
        else:
            decision = "escalate"
            decision_reason = f"Low confidence {confidence:.2f} - requires human decision"
            requires_human = True
            escalation_channel = "pagerduty" if severity == "critical" else "slack"

        # Force escalation for critical issues
        if severity == "critical" and AGENT_CONFIG["escalation"]["critical_always_notify"]:
            escalation_channel = "pagerduty"
            requires_human = True

        decisions.append({
            "decision_id": str(uuid.uuid4()),
            "event_id": event_id,
            "dataset_path": dataset_path,
            "suggested_action": suggested_action,
            "confidence": confidence,
            "decision": decision,
            "decision_reason": decision_reason,
            "decision_ts": decision_time,
            "requires_human_review": requires_human,
            "escalation_channel": escalation_channel
        })

    # Create output DataFrame
    decisions_schema = StructType([
        StructField("decision_id", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("dataset_path", StringType(), False),
        StructField("suggested_action", StringType(), False),
        StructField("confidence", DoubleType(), False),
        StructField("decision", StringType(), False),
        StructField("decision_reason", StringType(), True),
        StructField("decision_ts", TimestampType(), False),
        StructField("requires_human_review", BooleanType(), False),
        StructField("escalation_channel", StringType(), True),
    ])

    output.write_dataframe(spark.createDataFrame(decisions, decisions_schema))


# =============================================================================
# ACTION EXECUTION TRACKING
# =============================================================================

@transform(
    decisions=Input("/RevOps/Monitoring/agent_decisions"),
    output=Output("/RevOps/Monitoring/agent_action_log")
)
def log_agent_actions(ctx, decisions, output):
    """
    Log all agent actions for audit and ML improvement.

    Tracks:
    - What action was taken
    - When it was executed
    - Outcome (success/failed/pending)
    - Time to resolution
    """

    spark = ctx.spark_session
    df = decisions.dataframe()

    # Filter to auto-execute decisions
    auto_execute = df.filter(F.col("decision") == "auto_execute")

    # Create action log entries
    log_df = auto_execute.select(
        F.col("decision_id").alias("action_id"),
        F.col("event_id"),
        F.col("dataset_path"),
        F.col("suggested_action").alias("action_type"),
        F.col("confidence"),
        F.col("decision_ts").alias("initiated_ts"),
        F.lit("in_progress").alias("outcome"),
        F.lit(None).cast(TimestampType()).alias("completed_ts"),
        F.lit(None).cast(StringType()).alias("outcome_details"),
        F.col("decision_reason").alias("agent_reasoning")
    )

    output.write_dataframe(log_df)


# =============================================================================
# ESCALATION WORKFLOW
# =============================================================================

@transform(
    decisions=Input("/RevOps/Monitoring/agent_decisions"),
    output=Output("/RevOps/Monitoring/escalation_queue")
)
def build_escalation_queue(ctx, decisions, output):
    """
    Build a queue of items requiring human escalation.

    Groups by escalation channel and priority for efficient
    notification batching.
    """

    spark = ctx.spark_session
    df = decisions.dataframe()

    # Filter to items requiring escalation
    escalations = df.filter(
        F.col("requires_human_review") == True
    ).select(
        F.col("decision_id").alias("escalation_id"),
        F.col("event_id"),
        F.col("dataset_path"),
        F.col("suggested_action"),
        F.col("confidence"),
        F.col("decision"),
        F.col("decision_reason"),
        F.col("escalation_channel"),
        F.col("decision_ts").alias("escalation_ts"),
        F.lit("pending").alias("escalation_status"),
        F.lit(None).cast(StringType()).alias("assigned_to"),
        F.lit(None).cast(TimestampType()).alias("acknowledged_ts"),
        F.lit(None).cast(TimestampType()).alias("resolved_ts")
    ).withColumn(
        "urgency",
        F.when(F.col("escalation_channel") == "pagerduty", "high")
         .when(F.col("confidence") < 0.3, "high")
         .otherwise("normal")
    )

    output.write_dataframe(escalations)


# =============================================================================
# AGENT PERFORMANCE METRICS
# =============================================================================

@transform(
    action_log=Input("/RevOps/Monitoring/agent_action_log"),
    escalations=Input("/RevOps/Monitoring/escalation_queue"),
    output=Output("/RevOps/Monitoring/agent_performance")
)
def compute_agent_performance(ctx, action_log, escalations, output):
    """
    Compute performance metrics for the Ops Agent.

    Metrics:
    - Action success rate
    - Avg time to resolution
    - Escalation rate
    - False positive rate
    - Confidence calibration
    """

    spark = ctx.spark_session
    log_df = action_log.dataframe()
    esc_df = escalations.dataframe()

    # Aggregate action log metrics
    action_metrics = log_df.agg(
        F.count("*").alias("total_actions"),
        F.sum(F.when(F.col("outcome") == "success", 1).otherwise(0)).alias("successful_actions"),
        F.sum(F.when(F.col("outcome") == "failed", 1).otherwise(0)).alias("failed_actions"),
        F.avg("confidence").alias("avg_confidence")
    ).collect()[0]

    total_actions = action_metrics["total_actions"] or 0
    successful = action_metrics["successful_actions"] or 0
    failed = action_metrics["failed_actions"] or 0

    # Aggregate escalation metrics
    esc_metrics = esc_df.agg(
        F.count("*").alias("total_escalations"),
        F.sum(F.when(F.col("escalation_status") == "resolved", 1).otherwise(0)).alias("resolved_escalations"),
        F.sum(F.when(F.col("urgency") == "high", 1).otherwise(0)).alias("high_urgency_escalations")
    ).collect()[0]

    total_esc = esc_metrics["total_escalations"] or 0
    resolved_esc = esc_metrics["resolved_escalations"] or 0

    # Create performance summary
    metrics_time = datetime.now()

    performance_data = [{
        "metric_ts": metrics_time,
        "total_actions": total_actions,
        "successful_actions": successful,
        "failed_actions": failed,
        "success_rate": successful / total_actions if total_actions > 0 else 0.0,
        "total_escalations": total_esc,
        "resolved_escalations": resolved_esc,
        "escalation_rate": total_esc / (total_actions + total_esc) if (total_actions + total_esc) > 0 else 0.0,
        "avg_confidence": action_metrics["avg_confidence"] or 0.0,
        "agent_health_score": compute_agent_health(
            success_rate=successful / total_actions if total_actions > 0 else 1.0,
            escalation_rate=total_esc / (total_actions + total_esc) if (total_actions + total_esc) > 0 else 0.0
        )
    }]

    perf_schema = StructType([
        StructField("metric_ts", TimestampType(), False),
        StructField("total_actions", IntegerType(), False),
        StructField("successful_actions", IntegerType(), False),
        StructField("failed_actions", IntegerType(), False),
        StructField("success_rate", DoubleType(), False),
        StructField("total_escalations", IntegerType(), False),
        StructField("resolved_escalations", IntegerType(), False),
        StructField("escalation_rate", DoubleType(), False),
        StructField("avg_confidence", DoubleType(), False),
        StructField("agent_health_score", DoubleType(), False),
    ])

    output.write_dataframe(spark.createDataFrame(performance_data, perf_schema))


def compute_agent_health(success_rate: float, escalation_rate: float) -> float:
    """
    Compute overall agent health score.

    High success rate + low escalation rate = healthy agent
    """
    # Weight success rate more heavily
    health = (success_rate * 0.7) + ((1 - escalation_rate) * 0.3)
    return round(health * 100, 1)


# =============================================================================
# AGENT STATUS DASHBOARD DATA
# =============================================================================

@transform(
    performance=Input("/RevOps/Monitoring/agent_performance"),
    queue=Input("/RevOps/Monitoring/ops_agent_queue"),
    decisions=Input("/RevOps/Monitoring/agent_decisions"),
    output=Output("/RevOps/Monitoring/agent_status_dashboard")
)
def build_agent_status(ctx, performance, queue, decisions, output):
    """
    Build dashboard data for agent status monitoring.

    Provides:
    - Current agent status
    - Queue depth
    - Recent decisions
    - Performance trends
    """

    spark = ctx.spark_session
    perf_df = performance.dataframe()
    queue_df = queue.dataframe()
    decisions_df = decisions.dataframe()

    # Get latest performance
    latest_perf = perf_df.orderBy(F.col("metric_ts").desc()).limit(1)

    # Queue stats
    queue_stats = queue_df.agg(
        F.count("*").alias("queue_depth"),
        F.sum(F.when(F.col("severity") == "critical", 1).otherwise(0)).alias("critical_items"),
        F.sum(F.col("estimated_resolution_min")).alias("total_estimated_minutes")
    )

    # Decision stats (last 24 hours)
    recent_decisions = decisions_df.filter(
        F.col("decision_ts") > F.date_sub(F.current_timestamp(), 1)
    ).agg(
        F.count("*").alias("decisions_24h"),
        F.sum(F.when(F.col("decision") == "auto_execute", 1).otherwise(0)).alias("auto_executed_24h"),
        F.sum(F.when(F.col("decision") == "escalate", 1).otherwise(0)).alias("escalated_24h"),
        F.avg("confidence").alias("avg_confidence_24h")
    )

    # Cross join to create single status row
    status_df = latest_perf.crossJoin(queue_stats).crossJoin(recent_decisions)

    # Add agent status
    status_df = status_df.withColumn(
        "agent_status",
        F.when(F.col("agent_health_score") >= 80, "healthy")
         .when(F.col("agent_health_score") >= 60, "degraded")
         .otherwise("unhealthy")
    ).withColumn(
        "status_ts",
        F.current_timestamp()
    )

    output.write_dataframe(status_df)
