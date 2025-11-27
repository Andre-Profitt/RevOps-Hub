# /transforms/monitoring/alerting.py
"""
ALERTING SYSTEM
===============
Evaluates alert conditions and sends notifications via Slack, email, and webhooks.

This module:
1. Evaluates alert rules against current metrics
2. Checks cooldown periods to avoid alert fatigue
3. Sends notifications to configured channels
4. Records alert history for tracking

Usage in transforms:
    from transforms.monitoring.alerting import AlertManager, evaluate_alerts

    # In a transform
    alert_manager = AlertManager(customer_config)
    alerts = evaluate_alerts(metrics_df, customer_config)
    alert_manager.send_alerts(alerts)
"""

import json
import hashlib
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from enum import Enum
import urllib.request
import urllib.error


# =============================================================================
# ALERT TYPES
# =============================================================================

class AlertSeverity(str, Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class AlertType(str, Enum):
    """Types of alerts."""
    STALE_PIPELINE = "stale_pipeline"
    BUILD_FAILURE = "build_failure"
    DATA_QUALITY_DROP = "data_quality_drop"
    SYNC_DELAY = "sync_delay"
    FORECAST_MISS = "forecast_miss"
    HEALTH_CRITICAL = "health_critical"
    HYGIENE_VIOLATION = "hygiene_violation"
    QUOTA_AT_RISK = "quota_at_risk"


@dataclass
class Alert:
    """Represents a triggered alert."""
    alert_id: str
    alert_type: AlertType
    severity: AlertSeverity
    title: str
    message: str
    customer_id: str
    triggered_at: datetime
    metric_name: Optional[str] = None
    metric_value: Optional[float] = None
    threshold: Optional[float] = None
    context: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["alert_type"] = self.alert_type.value
        d["severity"] = self.severity.value
        d["triggered_at"] = self.triggered_at.isoformat()
        return d

    @property
    def dedup_key(self) -> str:
        """Key for deduplication (same alert type + customer within cooldown)."""
        return f"{self.customer_id}:{self.alert_type.value}"


# =============================================================================
# ALERT EVALUATION
# =============================================================================

@dataclass
class AlertRule:
    """Rule for triggering an alert."""
    alert_type: AlertType
    metric_name: str
    operator: str  # gt, lt, gte, lte, eq
    threshold: float
    severity: AlertSeverity = AlertSeverity.WARNING
    title_template: str = ""
    message_template: str = ""
    enabled: bool = True

    def evaluate(self, value: float) -> bool:
        """Check if value triggers this rule."""
        if not self.enabled:
            return False

        ops = {
            "gt": lambda v, t: v > t,
            "lt": lambda v, t: v < t,
            "gte": lambda v, t: v >= t,
            "lte": lambda v, t: v <= t,
            "eq": lambda v, t: v == t,
        }
        op_func = ops.get(self.operator, ops["gt"])
        return op_func(value, self.threshold)


# Default alert rules
DEFAULT_ALERT_RULES = [
    AlertRule(
        alert_type=AlertType.STALE_PIPELINE,
        metric_name="stale_deal_pct",
        operator="gt",
        threshold=0.10,
        severity=AlertSeverity.WARNING,
        title_template="Stale Pipeline Alert",
        message_template="Pipeline staleness at {value:.1%}, exceeds {threshold:.1%} threshold. {count} deals need attention.",
    ),
    AlertRule(
        alert_type=AlertType.BUILD_FAILURE,
        metric_name="build_failed",
        operator="eq",
        threshold=1,
        severity=AlertSeverity.CRITICAL,
        title_template="Build Failure",
        message_template="Build {build_id} failed for dataset {dataset}. Error: {error}",
    ),
    AlertRule(
        alert_type=AlertType.DATA_QUALITY_DROP,
        metric_name="data_quality_score",
        operator="lt",
        threshold=0.90,
        severity=AlertSeverity.WARNING,
        title_template="Data Quality Alert",
        message_template="Data quality dropped to {value:.1%}, below {threshold:.1%} threshold.",
    ),
    AlertRule(
        alert_type=AlertType.SYNC_DELAY,
        metric_name="sync_delay_minutes",
        operator="gt",
        threshold=60,
        severity=AlertSeverity.WARNING,
        title_template="CRM Sync Delay",
        message_template="CRM sync delayed by {value:.0f} minutes (threshold: {threshold:.0f}min).",
    ),
    AlertRule(
        alert_type=AlertType.HEALTH_CRITICAL,
        metric_name="critical_deal_count",
        operator="gt",
        threshold=5,
        severity=AlertSeverity.WARNING,
        title_template="Critical Deals Alert",
        message_template="{value:.0f} deals in critical health status, representing ${amount:,.0f} at risk.",
    ),
    AlertRule(
        alert_type=AlertType.FORECAST_MISS,
        metric_name="forecast_variance_pct",
        operator="gt",
        threshold=0.20,
        severity=AlertSeverity.WARNING,
        title_template="Forecast Variance Alert",
        message_template="Forecast variance at {value:.1%}, exceeds {threshold:.1%} tolerance.",
    ),
    AlertRule(
        alert_type=AlertType.QUOTA_AT_RISK,
        metric_name="quota_attainment_projected",
        operator="lt",
        threshold=0.80,
        severity=AlertSeverity.WARNING,
        title_template="Quota Attainment Risk",
        message_template="Projected quota attainment at {value:.1%}, below {threshold:.1%} threshold.",
    ),
]


def evaluate_alerts(
    metrics: Dict[str, Any],
    customer_id: str,
    rules: List[AlertRule] = None,
) -> List[Alert]:
    """
    Evaluate metrics against alert rules.

    Args:
        metrics: Dictionary of metric name -> value
        customer_id: Customer identifier
        rules: Alert rules to evaluate (uses defaults if not provided)

    Returns:
        List of triggered alerts
    """
    if rules is None:
        rules = DEFAULT_ALERT_RULES

    alerts = []
    now = datetime.utcnow()

    for rule in rules:
        if rule.metric_name not in metrics:
            continue

        value = metrics[rule.metric_name]
        if not rule.evaluate(value):
            continue

        # Generate alert ID
        alert_id = hashlib.md5(
            f"{customer_id}:{rule.alert_type.value}:{now.isoformat()}".encode()
        ).hexdigest()[:12]

        # Format message with available context
        context = {
            "value": value,
            "threshold": rule.threshold,
            **metrics,
        }

        try:
            message = rule.message_template.format(**context)
        except KeyError:
            message = f"{rule.metric_name} = {value} (threshold: {rule.threshold})"

        alert = Alert(
            alert_id=alert_id,
            alert_type=rule.alert_type,
            severity=rule.severity,
            title=rule.title_template or rule.alert_type.value.replace("_", " ").title(),
            message=message,
            customer_id=customer_id,
            triggered_at=now,
            metric_name=rule.metric_name,
            metric_value=value,
            threshold=rule.threshold,
            context=context,
        )
        alerts.append(alert)

    return alerts


# =============================================================================
# NOTIFICATION CHANNELS
# =============================================================================

class NotificationChannel:
    """Base class for notification channels."""

    def send(self, alert: Alert) -> bool:
        """Send alert notification. Returns True on success."""
        raise NotImplementedError


class SlackChannel(NotificationChannel):
    """Send alerts to Slack via webhook."""

    def __init__(self, webhook_url: str, channel: Optional[str] = None):
        self.webhook_url = webhook_url
        self.channel = channel

    def send(self, alert: Alert) -> bool:
        """Send alert to Slack."""
        color_map = {
            AlertSeverity.INFO: "#36a64f",      # green
            AlertSeverity.WARNING: "#ff9800",   # orange
            AlertSeverity.CRITICAL: "#f44336",  # red
        }

        payload = {
            "attachments": [{
                "color": color_map.get(alert.severity, "#808080"),
                "title": alert.title,
                "text": alert.message,
                "fields": [
                    {"title": "Severity", "value": alert.severity.value.upper(), "short": True},
                    {"title": "Customer", "value": alert.customer_id, "short": True},
                ],
                "footer": "RevOps Hub Alerts",
                "ts": int(alert.triggered_at.timestamp()),
            }]
        }

        if self.channel:
            payload["channel"] = self.channel

        return self._post(payload)

    def _post(self, payload: Dict) -> bool:
        """Post payload to Slack webhook."""
        try:
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                self.webhook_url,
                data=data,
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=10) as response:
                return response.status == 200
        except (urllib.error.URLError, urllib.error.HTTPError) as e:
            print(f"Slack notification failed: {e}")
            return False


class EmailChannel(NotificationChannel):
    """Send alerts via email (placeholder - requires SMTP config)."""

    def __init__(self, recipients: List[str], from_addr: str = "alerts@revops-hub.io"):
        self.recipients = recipients
        self.from_addr = from_addr

    def send(self, alert: Alert) -> bool:
        """Send alert via email."""
        # In production, integrate with SendGrid, SES, or SMTP
        # For now, log the email that would be sent
        print(f"EMAIL ALERT to {self.recipients}:")
        print(f"  Subject: [{alert.severity.value.upper()}] {alert.title}")
        print(f"  Body: {alert.message}")
        return True


class WebhookChannel(NotificationChannel):
    """Send alerts to a generic webhook endpoint."""

    def __init__(self, url: str, headers: Optional[Dict[str, str]] = None):
        self.url = url
        self.headers = headers or {}

    def send(self, alert: Alert) -> bool:
        """Send alert to webhook."""
        payload = alert.to_dict()

        try:
            data = json.dumps(payload).encode("utf-8")
            headers = {"Content-Type": "application/json", **self.headers}
            req = urllib.request.Request(self.url, data=data, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as response:
                return response.status in (200, 201, 202)
        except (urllib.error.URLError, urllib.error.HTTPError) as e:
            print(f"Webhook notification failed: {e}")
            return False


class PagerDutyChannel(NotificationChannel):
    """Send alerts to PagerDuty."""

    EVENTS_API = "https://events.pagerduty.com/v2/enqueue"

    def __init__(self, routing_key: str):
        self.routing_key = routing_key

    def send(self, alert: Alert) -> bool:
        """Send alert to PagerDuty."""
        severity_map = {
            AlertSeverity.INFO: "info",
            AlertSeverity.WARNING: "warning",
            AlertSeverity.CRITICAL: "critical",
        }

        payload = {
            "routing_key": self.routing_key,
            "event_action": "trigger",
            "dedup_key": alert.dedup_key,
            "payload": {
                "summary": f"{alert.title}: {alert.message}",
                "severity": severity_map.get(alert.severity, "warning"),
                "source": f"revops-hub-{alert.customer_id}",
                "timestamp": alert.triggered_at.isoformat(),
                "custom_details": alert.context,
            },
        }

        try:
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                self.EVENTS_API,
                data=data,
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=10) as response:
                return response.status == 202
        except (urllib.error.URLError, urllib.error.HTTPError) as e:
            print(f"PagerDuty notification failed: {e}")
            return False


# =============================================================================
# ALERT MANAGER
# =============================================================================

@dataclass
class AlertState:
    """Tracks alert state for cooldown management."""
    last_triggered: Dict[str, datetime] = field(default_factory=dict)
    alert_counts: Dict[str, int] = field(default_factory=dict)


class AlertManager:
    """
    Manages alert evaluation, cooldowns, and notification dispatch.

    Usage:
        manager = AlertManager(customer_config)
        alerts = manager.evaluate_and_send(metrics)
    """

    def __init__(
        self,
        customer_id: str,
        channels: Optional[List[NotificationChannel]] = None,
        cooldown_minutes: int = 60,
        rules: Optional[List[AlertRule]] = None,
    ):
        self.customer_id = customer_id
        self.channels = channels or []
        self.cooldown = timedelta(minutes=cooldown_minutes)
        self.rules = rules or DEFAULT_ALERT_RULES
        self.state = AlertState()

    def add_channel(self, channel: NotificationChannel):
        """Add a notification channel."""
        self.channels.append(channel)

    def evaluate(self, metrics: Dict[str, Any]) -> List[Alert]:
        """Evaluate metrics and return triggered alerts."""
        return evaluate_alerts(metrics, self.customer_id, self.rules)

    def should_send(self, alert: Alert) -> bool:
        """Check if alert should be sent (respects cooldown)."""
        last = self.state.last_triggered.get(alert.dedup_key)
        if last is None:
            return True
        return (alert.triggered_at - last) >= self.cooldown

    def send_alerts(self, alerts: List[Alert]) -> Dict[str, bool]:
        """
        Send alerts to all configured channels.

        Returns dict of alert_id -> success status.
        """
        results = {}

        for alert in alerts:
            if not self.should_send(alert):
                results[alert.alert_id] = False
                continue

            success = True
            for channel in self.channels:
                if not channel.send(alert):
                    success = False

            if success:
                self.state.last_triggered[alert.dedup_key] = alert.triggered_at
                self.state.alert_counts[alert.dedup_key] = (
                    self.state.alert_counts.get(alert.dedup_key, 0) + 1
                )

            results[alert.alert_id] = success

        return results

    def evaluate_and_send(self, metrics: Dict[str, Any]) -> List[Alert]:
        """Evaluate metrics and send any triggered alerts."""
        alerts = self.evaluate(metrics)
        self.send_alerts(alerts)
        return alerts

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "AlertManager":
        """
        Create AlertManager from customer config.

        Expected config structure:
        {
            "customer_id": "acme-corp",
            "alerts": {
                "enabled": true,
                "slack_webhook_url": "https://...",
                "slack_channel": "#alerts",
                "email_recipients": ["ops@acme.com"],
                "pagerduty_service_key": "xxx",
                "cooldown_minutes": 60
            }
        }
        """
        customer_id = config.get("customer_id", "unknown")
        alert_config = config.get("alerts", {})

        if not alert_config.get("enabled", True):
            return cls(customer_id)

        channels = []

        # Slack
        if alert_config.get("slack_webhook_url"):
            channels.append(SlackChannel(
                alert_config["slack_webhook_url"],
                alert_config.get("slack_channel"),
            ))

        # Email
        if alert_config.get("email_recipients"):
            channels.append(EmailChannel(
                alert_config["email_recipients"],
                alert_config.get("email_from", "alerts@revops-hub.io"),
            ))

        # PagerDuty
        if alert_config.get("pagerduty_service_key"):
            channels.append(PagerDutyChannel(
                alert_config["pagerduty_service_key"],
            ))

        return cls(
            customer_id=customer_id,
            channels=channels,
            cooldown_minutes=alert_config.get("cooldown_minutes", 60),
        )


# =============================================================================
# SPARK INTEGRATION
# =============================================================================

def collect_alert_metrics(
    build_metrics_df,
    pipeline_health_df,
    data_quality_df,
    sync_status_df,
) -> Dict[str, Any]:
    """
    Collect metrics from DataFrames for alert evaluation.

    This function extracts key metrics from various monitoring datasets
    for use with the AlertManager.
    """
    from pyspark.sql import functions as F

    metrics = {}

    # Build metrics
    if build_metrics_df is not None:
        latest_build = build_metrics_df.orderBy(F.desc("build_timestamp")).first()
        if latest_build:
            metrics["build_failed"] = 1 if latest_build["status"] == "FAILED" else 0
            metrics["build_id"] = latest_build.get("build_id", "unknown")
            metrics["dataset"] = latest_build.get("dataset_path", "unknown")
            if latest_build["status"] == "FAILED":
                metrics["error"] = latest_build.get("error_message", "Unknown error")

    # Pipeline health
    if pipeline_health_df is not None:
        health_stats = pipeline_health_df.agg(
            F.sum(F.when(F.col("health_status") == "Critical", 1).otherwise(0)).alias("critical_count"),
            F.sum(F.when(F.col("health_status") == "Critical", F.col("amount")).otherwise(0)).alias("critical_amount"),
            F.sum(F.when(F.col("days_since_activity") > 14, 1).otherwise(0)).alias("stale_count"),
            F.count("*").alias("total_count"),
        ).first()

        if health_stats:
            metrics["critical_deal_count"] = health_stats["critical_count"] or 0
            metrics["amount"] = health_stats["critical_amount"] or 0
            total = health_stats["total_count"] or 1
            metrics["stale_deal_pct"] = (health_stats["stale_count"] or 0) / total

    # Data quality
    if data_quality_df is not None:
        quality = data_quality_df.agg(F.avg("quality_score")).first()
        if quality and quality[0]:
            metrics["data_quality_score"] = quality[0]

    # Sync status
    if sync_status_df is not None:
        sync = sync_status_df.orderBy(F.desc("last_sync_time")).first()
        if sync:
            last_sync = sync["last_sync_time"]
            if last_sync:
                delay = (datetime.utcnow() - last_sync).total_seconds() / 60
                metrics["sync_delay_minutes"] = delay

    return metrics
