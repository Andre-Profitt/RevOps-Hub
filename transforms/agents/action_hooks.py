"""
ACTION HOOKS - Executable Actions for Ops Agent
================================================
Defines the executable actions that the Ops Agent can trigger.
Each hook connects to Foundry APIs or external services.

Phase 2: Action Hooks and Escalation Workflow

Action Types:
- TRIGGER_REBUILD: Call Foundry Build API to rebuild dataset
- CHECK_UPSTREAM_DATA: Verify upstream dataset health
- OPTIMIZE_TRANSFORM: Create JIRA ticket for optimization
- REVIEW_DATA_QUALITY: Create data quality review task
- ESCALATE_TO_HUMAN: Send notifications via configured channels
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    BooleanType, MapType
)
from datetime import datetime
import json
import uuid


# =============================================================================
# ACTION EXECUTION RESULTS
# =============================================================================

@transform(
    decisions=Input("/RevOps/Monitoring/agent_decisions"),
    output=Output("/RevOps/Monitoring/action_execution_results")
)
def execute_agent_actions(ctx, decisions, output):
    """
    Execute agent decisions that are marked for auto-execution.

    This transform simulates action execution - in production,
    this would call actual Foundry APIs and external services.

    For each action:
    1. Validate the action is still valid
    2. Execute the action via appropriate API
    3. Record the result
    """

    spark = ctx.spark_session
    df = decisions.dataframe()

    # Filter to auto-execute decisions
    to_execute = df.filter(F.col("decision") == "auto_execute")

    if to_execute.count() == 0:
        # No actions to execute
        empty_schema = StructType([
            StructField("execution_id", StringType(), False),
            StructField("decision_id", StringType(), False),
            StructField("event_id", StringType(), False),
            StructField("dataset_path", StringType(), False),
            StructField("action_type", StringType(), False),
            StructField("execution_ts", TimestampType(), False),
            StructField("execution_status", StringType(), False),
            StructField("execution_result", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("api_response", StringType(), True),
        ])
        output.write_dataframe(spark.createDataFrame([], empty_schema))
        return

    execution_time = datetime.now()
    results = []

    for row in to_execute.collect():
        decision_id = row["decision_id"]
        event_id = row["event_id"]
        dataset_path = row["dataset_path"]
        action_type = row["suggested_action"]

        # Execute action based on type
        result = execute_action(action_type, dataset_path)

        results.append({
            "execution_id": str(uuid.uuid4()),
            "decision_id": decision_id,
            "event_id": event_id,
            "dataset_path": dataset_path,
            "action_type": action_type,
            "execution_ts": execution_time,
            "execution_status": result["status"],
            "execution_result": result.get("result"),
            "error_message": result.get("error"),
            "api_response": json.dumps(result.get("api_response", {}))
        })

    results_schema = StructType([
        StructField("execution_id", StringType(), False),
        StructField("decision_id", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("dataset_path", StringType(), False),
        StructField("action_type", StringType(), False),
        StructField("execution_ts", TimestampType(), False),
        StructField("execution_status", StringType(), False),
        StructField("execution_result", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("api_response", StringType(), True),
    ])

    output.write_dataframe(spark.createDataFrame(results, results_schema))


def execute_action(action_type: str, dataset_path: str) -> dict:
    """
    Execute a specific action type.

    In production, this would make actual API calls.
    This implementation simulates the execution.
    """

    if action_type == "TRIGGER_REBUILD":
        return trigger_rebuild(dataset_path)
    elif action_type == "CHECK_UPSTREAM_DATA":
        return check_upstream_data(dataset_path)
    elif action_type == "OPTIMIZE_TRANSFORM":
        return create_optimization_ticket(dataset_path)
    elif action_type == "REVIEW_DATA_QUALITY":
        return create_quality_review(dataset_path)
    else:
        return {
            "status": "skipped",
            "result": f"Unknown action type: {action_type}",
            "error": None
        }


def trigger_rebuild(dataset_path: str) -> dict:
    """
    Trigger a dataset rebuild via Foundry Build API.

    In production:
    - POST to /api/v1/builds
    - Include dataset RID
    - Wait for build to start
    """
    # Simulated execution
    return {
        "status": "success",
        "result": f"Build triggered for {dataset_path}",
        "api_response": {
            "build_rid": f"ri.foundry.main.build.{uuid.uuid4()}",
            "status": "QUEUED",
            "message": "Build scheduled"
        }
    }


def check_upstream_data(dataset_path: str) -> dict:
    """
    Check upstream dataset health.

    In production:
    - Query Foundry for upstream dependencies
    - Check each upstream's build status
    - Report findings
    """
    return {
        "status": "success",
        "result": f"Upstream check completed for {dataset_path}",
        "api_response": {
            "upstream_count": 3,
            "healthy_count": 2,
            "stale_count": 1,
            "upstream_details": [
                {"path": "/RevOps/Raw/salesforce_opps", "status": "healthy"},
                {"path": "/RevOps/Raw/salesforce_accounts", "status": "healthy"},
                {"path": "/RevOps/Raw/hubspot_contacts", "status": "stale"}
            ]
        }
    }


def create_optimization_ticket(dataset_path: str) -> dict:
    """
    Create a JIRA ticket for transform optimization.

    In production:
    - POST to JIRA API
    - Create ticket with dataset details
    - Link to transform code
    """
    return {
        "status": "success",
        "result": f"Optimization ticket created for {dataset_path}",
        "api_response": {
            "ticket_id": f"REVOPS-{uuid.uuid4().int % 10000}",
            "ticket_url": "https://jira.example.com/browse/REVOPS-1234",
            "assigned_to": "unassigned"
        }
    }


def create_quality_review(dataset_path: str) -> dict:
    """
    Create a data quality review task.

    In production:
    - Create task in project management system
    - Notify data steward
    - Track resolution
    """
    return {
        "status": "success",
        "result": f"Quality review task created for {dataset_path}",
        "api_response": {
            "task_id": str(uuid.uuid4())[:8],
            "priority": "high",
            "assignee": "data-stewards@company.com"
        }
    }


# =============================================================================
# NOTIFICATION DISPATCH
# =============================================================================

@transform(
    escalations=Input("/RevOps/Monitoring/escalation_queue"),
    output=Output("/RevOps/Monitoring/notification_log")
)
def dispatch_notifications(ctx, escalations, output):
    """
    Dispatch notifications for escalated items.

    Supports multiple channels:
    - Slack: For normal urgency items
    - PagerDuty: For critical/high urgency items
    - Email: For batch notifications
    """

    spark = ctx.spark_session
    df = escalations.dataframe()

    # Filter to pending escalations
    pending = df.filter(F.col("escalation_status") == "pending")

    if pending.count() == 0:
        empty_schema = StructType([
            StructField("notification_id", StringType(), False),
            StructField("escalation_id", StringType(), False),
            StructField("channel", StringType(), False),
            StructField("sent_ts", TimestampType(), False),
            StructField("delivery_status", StringType(), False),
            StructField("message_content", StringType(), True),
            StructField("recipient", StringType(), True),
        ])
        output.write_dataframe(spark.createDataFrame([], empty_schema))
        return

    notifications = []
    sent_time = datetime.now()

    for row in pending.collect():
        escalation_id = row["escalation_id"]
        channel = row["escalation_channel"]
        dataset_path = row["dataset_path"]
        action = row["suggested_action"]
        confidence = row["confidence"]
        reason = row["decision_reason"]
        urgency = row["urgency"]

        # Build notification message
        message = build_notification_message(
            dataset_path, action, confidence, reason, urgency
        )

        # Get recipient based on channel
        recipient = get_recipient(channel, urgency)

        # Dispatch (simulated)
        delivery_status = dispatch_to_channel(channel, message, recipient)

        notifications.append({
            "notification_id": str(uuid.uuid4()),
            "escalation_id": escalation_id,
            "channel": channel,
            "sent_ts": sent_time,
            "delivery_status": delivery_status,
            "message_content": message,
            "recipient": recipient
        })

    notif_schema = StructType([
        StructField("notification_id", StringType(), False),
        StructField("escalation_id", StringType(), False),
        StructField("channel", StringType(), False),
        StructField("sent_ts", TimestampType(), False),
        StructField("delivery_status", StringType(), False),
        StructField("message_content", StringType(), True),
        StructField("recipient", StringType(), True),
    ])

    output.write_dataframe(spark.createDataFrame(notifications, notif_schema))


def build_notification_message(dataset_path: str, action: str,
                                confidence: float, reason: str, urgency: str) -> str:
    """Build human-readable notification message."""

    urgency_emoji = "🔴" if urgency == "high" else "🟡"

    return f"""{urgency_emoji} RevOps Ops Agent Escalation

Dataset: {dataset_path}
Suggested Action: {action.replace('_', ' ')}
Confidence: {confidence:.0%}
Reason: {reason}

Please review and take appropriate action.
"""


def get_recipient(channel: str, urgency: str) -> str:
    """Get notification recipient based on channel and urgency."""

    recipients = {
        "slack": {
            "high": "#revops-critical",
            "normal": "#revops-alerts"
        },
        "pagerduty": {
            "high": "revops-oncall",
            "normal": "revops-team"
        },
        "email": {
            "high": "revops-critical@company.com",
            "normal": "revops-team@company.com"
        }
    }

    return recipients.get(channel, {}).get(urgency, "unknown")


def dispatch_to_channel(channel: str, message: str, recipient: str) -> str:
    """
    Dispatch notification to specified channel.

    In production, this would make actual API calls to:
    - Slack API
    - PagerDuty API
    - Email service (SendGrid, SES, etc.)
    """
    # Simulated dispatch - always succeeds
    return "delivered"


# =============================================================================
# ESCALATION STATUS TRACKING
# =============================================================================

@transform(
    escalations=Input("/RevOps/Monitoring/escalation_queue"),
    notifications=Input("/RevOps/Monitoring/notification_log"),
    output=Output("/RevOps/Monitoring/escalation_tracker")
)
def track_escalations(ctx, escalations, notifications, output):
    """
    Track escalation status and resolution.

    Combines escalation queue with notification delivery
    to provide a complete view of escalation lifecycle.
    """

    spark = ctx.spark_session
    esc_df = escalations.dataframe()
    notif_df = notifications.dataframe()

    # Join escalations with notifications
    tracker = esc_df.join(
        notif_df.select(
            "escalation_id",
            "sent_ts",
            "delivery_status",
            "channel",
            "recipient"
        ),
        on="escalation_id",
        how="left"
    )

    # Calculate time since escalation
    tracker = tracker.withColumn(
        "hours_since_escalation",
        (F.unix_timestamp(F.current_timestamp()) -
         F.unix_timestamp(F.col("escalation_ts"))) / 3600
    ).withColumn(
        "is_overdue",
        F.col("hours_since_escalation") > 2  # 2 hour SLA
    ).withColumn(
        "urgency_level",
        F.when(F.col("is_overdue") & (F.col("escalation_status") == "pending"), "critical")
         .when(F.col("urgency") == "high", "high")
         .otherwise("normal")
    )

    output.write_dataframe(tracker)


# =============================================================================
# ACTION FEEDBACK LOOP
# =============================================================================

@transform(
    executions=Input("/RevOps/Monitoring/action_execution_results"),
    metrics=Input("/RevOps/Monitoring/build_metrics"),
    output=Output("/RevOps/Monitoring/action_feedback")
)
def collect_action_feedback(ctx, executions, metrics, output):
    """
    Collect feedback on action outcomes to improve agent decisions.

    Compares pre-action and post-action states to determine
    if the action was effective.

    This data feeds back into confidence calibration.
    """

    spark = ctx.spark_session
    exec_df = executions.dataframe()
    metrics_df = metrics.dataframe()

    # Join executions with current metrics
    feedback = exec_df.alias("e").join(
        metrics_df.alias("m"),
        F.col("e.dataset_path") == F.col("m.dataset_path"),
        how="left"
    ).select(
        F.col("e.execution_id"),
        F.col("e.event_id"),
        F.col("e.dataset_path"),
        F.col("e.action_type"),
        F.col("e.execution_ts"),
        F.col("e.execution_status"),
        F.col("m.status").alias("current_status"),
        F.col("m.is_healthy").alias("current_is_healthy"),
        F.col("m.last_build_ts").alias("latest_build_ts")
    )

    # Determine if action was effective
    feedback = feedback.withColumn(
        "action_effective",
        F.when(
            (F.col("action_type") == "TRIGGER_REBUILD") &
            (F.col("current_is_healthy") == True) &
            (F.col("latest_build_ts") > F.col("execution_ts")),
            True
        ).when(
            F.col("execution_status") == "success",
            True  # Assume effective if execution succeeded
        ).otherwise(False)
    ).withColumn(
        "feedback_ts",
        F.current_timestamp()
    )

    output.write_dataframe(feedback)
