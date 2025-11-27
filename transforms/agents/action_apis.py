"""
ACTION APIS - External Service Connectors
==========================================
API connectors for executing automated actions.
Provides interfaces to Foundry APIs, JIRA, Slack, PagerDuty, and other services.

Phase 3: Insight-to-Action Automation

Service Integrations:
- Foundry Build API: Trigger dataset rebuilds
- JIRA: Create tickets for manual review items
- Slack: Send notifications and alerts
- PagerDuty: Critical incident escalation
- Salesforce: Data quality feedback
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    BooleanType, IntegerType
)
from datetime import datetime
import json
import uuid


# =============================================================================
# API CONFIGURATION
# =============================================================================

API_CONFIG = {
    "foundry": {
        "base_url": "https://foundry.palantir.com/api",
        "endpoints": {
            "trigger_build": "/builds/v1/trigger",
            "get_build_status": "/builds/v1/status/{build_rid}",
            "get_dataset_metadata": "/datasets/v1/{dataset_rid}/metadata"
        },
        "timeout_seconds": 30,
        "retry_count": 3
    },
    "jira": {
        "base_url": "https://jira.company.com/rest/api/2",
        "project_key": "REVOPS",
        "issue_types": {
            "optimization": "Task",
            "data_quality": "Bug",
            "incident": "Incident"
        },
        "default_assignee": "unassigned"
    },
    "slack": {
        "channels": {
            "alerts": "#revops-alerts",
            "critical": "#revops-critical",
            "daily_digest": "#revops-daily"
        },
        "mention_groups": {
            "critical": "<!channel>",
            "high": "@revops-oncall",
            "normal": ""
        }
    },
    "pagerduty": {
        "service_id": "revops-pipeline",
        "escalation_policy": "revops-escalation",
        "urgency_mapping": {
            "critical": "high",
            "high": "high",
            "normal": "low"
        }
    }
}


# =============================================================================
# FOUNDRY BUILD API INTEGRATION
# =============================================================================

@transform(
    queue=Input("/RevOps/Monitoring/ops_agent_queue"),
    output=Output("/RevOps/Monitoring/api_build_requests")
)
def create_build_requests(ctx, queue, output):
    """
    Create Foundry Build API requests for TRIGGER_REBUILD actions.

    Prepares API payloads that would trigger dataset rebuilds.
    In production, these would be executed via HTTP calls.
    """

    spark = ctx.spark_session
    df = queue.dataframe()

    # Filter to rebuild actions
    rebuilds = df.filter(
        (F.col("suggested_action") == "TRIGGER_REBUILD") &
        (F.col("action_status") == "pending")
    )

    if rebuilds.count() == 0:
        empty_schema = StructType([
            StructField("request_id", StringType(), False),
            StructField("event_id", StringType(), False),
            StructField("dataset_path", StringType(), False),
            StructField("api_endpoint", StringType(), False),
            StructField("request_payload", StringType(), False),
            StructField("created_ts", TimestampType(), False),
            StructField("execution_status", StringType(), False),
        ])
        output.write_dataframe(spark.createDataFrame([], empty_schema))
        return

    requests = []
    request_time = datetime.now()

    for row in rebuilds.collect():
        event_id = row["event_id"]
        dataset_path = row["dataset_path"]

        # Build API payload
        payload = {
            "datasetRids": [dataset_path_to_rid(dataset_path)],
            "branch": "master",
            "fallbackBranches": ["master"],
            "abortOnFailure": True,
            "retryCount": 1
        }

        requests.append({
            "request_id": str(uuid.uuid4()),
            "event_id": event_id,
            "dataset_path": dataset_path,
            "api_endpoint": API_CONFIG["foundry"]["endpoints"]["trigger_build"],
            "request_payload": json.dumps(payload),
            "created_ts": request_time,
            "execution_status": "pending"
        })

    request_schema = StructType([
        StructField("request_id", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("dataset_path", StringType(), False),
        StructField("api_endpoint", StringType(), False),
        StructField("request_payload", StringType(), False),
        StructField("created_ts", TimestampType(), False),
        StructField("execution_status", StringType(), False),
    ])

    output.write_dataframe(spark.createDataFrame(requests, request_schema))


def dataset_path_to_rid(path: str) -> str:
    """
    Convert dataset path to RID.

    In production, this would query Foundry's dataset registry.
    Here we generate a placeholder RID.
    """
    # Hash the path to create a deterministic RID
    import hashlib
    hash_val = hashlib.md5(path.encode()).hexdigest()
    return f"ri.foundry.main.dataset.{hash_val}"


# =============================================================================
# JIRA INTEGRATION
# =============================================================================

@transform(
    decisions=Input("/RevOps/Monitoring/agent_decisions"),
    output=Output("/RevOps/Monitoring/jira_tickets")
)
def create_jira_tickets(ctx, decisions, output):
    """
    Create JIRA tickets for actions requiring human review.

    Generates ticket payloads for:
    - Optimization requests
    - Data quality reviews
    - Manual incident investigation
    """

    spark = ctx.spark_session
    df = decisions.dataframe()

    # Filter to decisions requiring JIRA tickets
    ticket_actions = ["OPTIMIZE_TRANSFORM", "REVIEW_DATA_QUALITY"]
    needs_ticket = df.filter(
        (F.col("suggested_action").isin(ticket_actions)) &
        (F.col("requires_human_review") == True)
    )

    if needs_ticket.count() == 0:
        empty_schema = StructType([
            StructField("ticket_id", StringType(), False),
            StructField("decision_id", StringType(), False),
            StructField("dataset_path", StringType(), False),
            StructField("ticket_type", StringType(), False),
            StructField("summary", StringType(), False),
            StructField("description", StringType(), False),
            StructField("priority", StringType(), False),
            StructField("labels", StringType(), False),
            StructField("created_ts", TimestampType(), False),
            StructField("jira_key", StringType(), True),
        ])
        output.write_dataframe(spark.createDataFrame([], empty_schema))
        return

    tickets = []
    ticket_time = datetime.now()

    for row in needs_ticket.collect():
        decision_id = row["decision_id"]
        dataset_path = row["dataset_path"]
        action = row["suggested_action"]
        confidence = row["confidence"]
        reason = row["decision_reason"]

        # Determine ticket type
        if action == "OPTIMIZE_TRANSFORM":
            ticket_type = "optimization"
            summary = f"[RevOps] Optimize transform: {dataset_path.split('/')[-1]}"
            priority = "Medium"
            labels = ["revops", "optimization", "auto-generated"]
        else:
            ticket_type = "data_quality"
            summary = f"[RevOps] Data quality review: {dataset_path.split('/')[-1]}"
            priority = "High"
            labels = ["revops", "data-quality", "auto-generated"]

        description = f"""
h2. Auto-Generated by RevOps Ops Agent

*Dataset:* {dataset_path}
*Action Required:* {action.replace('_', ' ')}
*Agent Confidence:* {confidence:.0%}
*Reason:* {reason}

h3. Context
This ticket was automatically created by the RevOps Ops Agent.
Please investigate and take appropriate action.

h3. Acceptance Criteria
* [ ] Investigate the issue
* [ ] Implement fix or optimization
* [ ] Verify dataset health is restored
* [ ] Close ticket with resolution notes
        """

        tickets.append({
            "ticket_id": str(uuid.uuid4()),
            "decision_id": decision_id,
            "dataset_path": dataset_path,
            "ticket_type": ticket_type,
            "summary": summary,
            "description": description,
            "priority": priority,
            "labels": json.dumps(labels),
            "created_ts": ticket_time,
            "jira_key": None  # Would be populated after API call
        })

    ticket_schema = StructType([
        StructField("ticket_id", StringType(), False),
        StructField("decision_id", StringType(), False),
        StructField("dataset_path", StringType(), False),
        StructField("ticket_type", StringType(), False),
        StructField("summary", StringType(), False),
        StructField("description", StringType(), False),
        StructField("priority", StringType(), False),
        StructField("labels", StringType(), False),
        StructField("created_ts", TimestampType(), False),
        StructField("jira_key", StringType(), True),
    ])

    output.write_dataframe(spark.createDataFrame(tickets, ticket_schema))


# =============================================================================
# SLACK INTEGRATION
# =============================================================================

@transform(
    escalations=Input("/RevOps/Monitoring/escalation_queue"),
    output=Output("/RevOps/Monitoring/slack_messages")
)
def create_slack_messages(ctx, escalations, output):
    """
    Create Slack messages for escalated items.

    Generates formatted Slack Block Kit messages for notifications.
    """

    spark = ctx.spark_session
    df = escalations.dataframe()

    # Filter to Slack escalations
    slack_esc = df.filter(
        (F.col("escalation_channel") == "slack") &
        (F.col("escalation_status") == "pending")
    )

    if slack_esc.count() == 0:
        empty_schema = StructType([
            StructField("message_id", StringType(), False),
            StructField("escalation_id", StringType(), False),
            StructField("channel", StringType(), False),
            StructField("message_blocks", StringType(), False),
            StructField("created_ts", TimestampType(), False),
            StructField("sent_status", StringType(), False),
        ])
        output.write_dataframe(spark.createDataFrame([], empty_schema))
        return

    messages = []
    message_time = datetime.now()

    for row in slack_esc.collect():
        escalation_id = row["escalation_id"]
        dataset_path = row["dataset_path"]
        action = row["suggested_action"]
        confidence = row["confidence"]
        reason = row["decision_reason"]
        urgency = row["urgency"]

        # Determine channel
        channel = API_CONFIG["slack"]["channels"]["critical"] if urgency == "high" \
            else API_CONFIG["slack"]["channels"]["alerts"]

        # Build Slack Block Kit message
        mention = API_CONFIG["slack"]["mention_groups"].get(urgency, "")
        blocks = build_slack_blocks(
            dataset_path, action, confidence, reason, urgency, mention
        )

        messages.append({
            "message_id": str(uuid.uuid4()),
            "escalation_id": escalation_id,
            "channel": channel,
            "message_blocks": json.dumps(blocks),
            "created_ts": message_time,
            "sent_status": "pending"
        })

    message_schema = StructType([
        StructField("message_id", StringType(), False),
        StructField("escalation_id", StringType(), False),
        StructField("channel", StringType(), False),
        StructField("message_blocks", StringType(), False),
        StructField("created_ts", TimestampType(), False),
        StructField("sent_status", StringType(), False),
    ])

    output.write_dataframe(spark.createDataFrame(messages, message_schema))


def build_slack_blocks(dataset_path: str, action: str, confidence: float,
                       reason: str, urgency: str, mention: str) -> list:
    """Build Slack Block Kit message blocks."""

    urgency_emoji = ":red_circle:" if urgency == "high" else ":large_yellow_circle:"

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{urgency_emoji} RevOps Agent Escalation"
            }
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Dataset:*\n`{dataset_path}`"},
                {"type": "mrkdwn", "text": f"*Action:*\n{action.replace('_', ' ')}"},
                {"type": "mrkdwn", "text": f"*Confidence:*\n{confidence:.0%}"},
                {"type": "mrkdwn", "text": f"*Urgency:*\n{urgency.upper()}"}
            ]
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Reason:* {reason}"}
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Acknowledge"},
                    "style": "primary",
                    "action_id": "acknowledge_escalation"
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View in Dashboard"},
                    "url": "https://revops.company.com/ops"
                }
            ]
        }
    ]

    if mention:
        blocks.insert(0, {
            "type": "section",
            "text": {"type": "mrkdwn", "text": mention}
        })

    return blocks


# =============================================================================
# PAGERDUTY INTEGRATION
# =============================================================================

@transform(
    escalations=Input("/RevOps/Monitoring/escalation_queue"),
    output=Output("/RevOps/Monitoring/pagerduty_incidents")
)
def create_pagerduty_incidents(ctx, escalations, output):
    """
    Create PagerDuty incidents for critical escalations.

    Generates incident payloads for high-urgency items.
    """

    spark = ctx.spark_session
    df = escalations.dataframe()

    # Filter to PagerDuty escalations
    pd_esc = df.filter(
        (F.col("escalation_channel") == "pagerduty") &
        (F.col("escalation_status") == "pending")
    )

    if pd_esc.count() == 0:
        empty_schema = StructType([
            StructField("incident_id", StringType(), False),
            StructField("escalation_id", StringType(), False),
            StructField("service_id", StringType(), False),
            StructField("title", StringType(), False),
            StructField("urgency", StringType(), False),
            StructField("body", StringType(), False),
            StructField("created_ts", TimestampType(), False),
            StructField("pd_incident_key", StringType(), True),
        ])
        output.write_dataframe(spark.createDataFrame([], empty_schema))
        return

    incidents = []
    incident_time = datetime.now()

    for row in pd_esc.collect():
        escalation_id = row["escalation_id"]
        dataset_path = row["dataset_path"]
        action = row["suggested_action"]
        confidence = row["confidence"]
        reason = row["decision_reason"]
        urgency = row["urgency"]

        # Map urgency
        pd_urgency = API_CONFIG["pagerduty"]["urgency_mapping"].get(urgency, "low")

        title = f"[RevOps] Pipeline Alert: {dataset_path.split('/')[-1]}"
        body = f"""
RevOps Ops Agent Critical Escalation

Dataset: {dataset_path}
Action Required: {action.replace('_', ' ')}
Agent Confidence: {confidence:.0%}
Reason: {reason}

This incident was automatically created due to a critical pipeline issue.
Please investigate immediately.
        """

        incidents.append({
            "incident_id": str(uuid.uuid4()),
            "escalation_id": escalation_id,
            "service_id": API_CONFIG["pagerduty"]["service_id"],
            "title": title,
            "urgency": pd_urgency,
            "body": body,
            "created_ts": incident_time,
            "pd_incident_key": None
        })

    incident_schema = StructType([
        StructField("incident_id", StringType(), False),
        StructField("escalation_id", StringType(), False),
        StructField("service_id", StringType(), False),
        StructField("title", StringType(), False),
        StructField("urgency", StringType(), False),
        StructField("body", StringType(), False),
        StructField("created_ts", TimestampType(), False),
        StructField("pd_incident_key", StringType(), True),
    ])

    output.write_dataframe(spark.createDataFrame(incidents, incident_schema))


# =============================================================================
# API EXECUTION SUMMARY
# =============================================================================

@transform(
    build_requests=Input("/RevOps/Monitoring/api_build_requests"),
    jira_tickets=Input("/RevOps/Monitoring/jira_tickets"),
    slack_messages=Input("/RevOps/Monitoring/slack_messages"),
    pd_incidents=Input("/RevOps/Monitoring/pagerduty_incidents"),
    output=Output("/RevOps/Monitoring/api_execution_summary")
)
def summarize_api_execution(ctx, build_requests, jira_tickets, slack_messages,
                            pd_incidents, output):
    """
    Summarize all API execution activity.

    Provides a unified view of all external API calls made by the agent.
    """

    spark = ctx.spark_session
    summary_time = datetime.now()

    # Count by service
    build_count = build_requests.dataframe().count()
    jira_count = jira_tickets.dataframe().count()
    slack_count = slack_messages.dataframe().count()
    pd_count = pd_incidents.dataframe().count()

    summary_data = [{
        "summary_ts": summary_time,
        "build_api_requests": build_count,
        "jira_tickets_created": jira_count,
        "slack_messages_sent": slack_count,
        "pagerduty_incidents": pd_count,
        "total_api_calls": build_count + jira_count + slack_count + pd_count
    }]

    summary_schema = StructType([
        StructField("summary_ts", TimestampType(), False),
        StructField("build_api_requests", IntegerType(), False),
        StructField("jira_tickets_created", IntegerType(), False),
        StructField("slack_messages_sent", IntegerType(), False),
        StructField("pagerduty_incidents", IntegerType(), False),
        StructField("total_api_calls", IntegerType(), False),
    ])

    output.write_dataframe(spark.createDataFrame(summary_data, summary_schema))
