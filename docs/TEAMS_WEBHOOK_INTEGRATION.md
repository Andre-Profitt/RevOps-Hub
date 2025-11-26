# Microsoft Teams Webhook Integration for Pipeline Hygiene Alerts

This document describes the webhook payload format for integrating Pipeline Hygiene alerts with Microsoft Teams.

---

## Overview

The `/RevOps/Analytics/hygiene_alert_feed` dataset generates webhook-ready alert payloads that can be consumed by Microsoft Teams incoming webhooks.

## Dataset Location

```
/RevOps/Analytics/hygiene_alert_feed
```

## Schema

| Column | Type | Description |
|--------|------|-------------|
| `alert_id` | STRING | Unique identifier: `{opportunity_id}_{alert_type}_{date}` |
| `alert_type` | STRING | One of: GONE_DARK, STALE_CLOSE_DATE, OVERDUE_COMMIT, MISSING_CHAMPION, SINGLE_THREADED, STALLED_STAGE, MISSING_NEXT_STEPS, HEAVY_DISCOUNT |
| `alert_severity` | STRING | CRITICAL, HIGH, MEDIUM, LOW |
| `recommended_action` | STRING | Human-readable action to take |
| `opportunity_id` | STRING | CRM opportunity ID |
| `opportunity_name` | STRING | Deal name |
| `account_name` | STRING | Customer name |
| `amount` | DOUBLE | Deal value |
| `stage_name` | STRING | Current sales stage |
| `close_date` | DATE | Expected close date |
| `health_score` | DOUBLE | Overall deal health (0-100) |
| `hygiene_score` | DOUBLE | Hygiene compliance score (0-100) |
| `owner_id` | STRING | Sales rep ID for routing |
| `owner_name` | STRING | Sales rep name |
| `days_since_activity` | INT | Days since last activity |
| `days_until_close` | INT | Days until close date |
| `days_in_current_stage` | INT | Days in current stage |
| `stakeholder_count` | INT | Number of engaged stakeholders |
| `discount_percent` | DOUBLE | Current discount percentage |
| `evaluated_date` | DATE | When the alert was generated |
| `generated_at` | TIMESTAMP | Exact generation timestamp |
| `priority_rank` | INT | Alert priority (1 = highest) |

---

## Teams Adaptive Card Payload

### Recommended Format (Adaptive Cards)

```json
{
  "type": "message",
  "attachments": [
    {
      "contentType": "application/vnd.microsoft.card.adaptive",
      "contentUrl": null,
      "content": {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "body": [
          {
            "type": "Container",
            "style": "attention",
            "items": [
              {
                "type": "TextBlock",
                "text": "🚨 CRITICAL Pipeline Hygiene Alert",
                "weight": "Bolder",
                "size": "Medium"
              }
            ]
          },
          {
            "type": "Container",
            "items": [
              {
                "type": "TextBlock",
                "text": "Acme Corporation",
                "weight": "Bolder",
                "size": "Large"
              },
              {
                "type": "TextBlock",
                "text": "Enterprise Platform Deal",
                "size": "Small",
                "isSubtle": true
              }
            ]
          },
          {
            "type": "FactSet",
            "facts": [
              { "title": "Amount", "value": "$1,200,000" },
              { "title": "Alert Type", "value": "Gone Dark" },
              { "title": "Owner", "value": "Sarah Chen" },
              { "title": "Days Since Activity", "value": "18" },
              { "title": "Health Score", "value": "48/100" },
              { "title": "Stage", "value": "Negotiation" }
            ]
          },
          {
            "type": "TextBlock",
            "text": "**Recommended Action:** Immediate outreach required - contact has gone dark for 14+ days",
            "wrap": true
          }
        ],
        "actions": [
          {
            "type": "Action.OpenUrl",
            "title": "View in Salesforce",
            "url": "https://yourorg.lightning.force.com/lightning/r/Opportunity/OPP-001/view"
          },
          {
            "type": "Action.OpenUrl",
            "title": "View in RevOps Hub",
            "url": "https://revops.yourcompany.com/?deal=OPP-001"
          }
        ]
      }
    }
  ]
}
```

### Legacy MessageCard Format

For older Teams integrations:

```json
{
  "@type": "MessageCard",
  "@context": "http://schema.org/extensions",
  "themeColor": "dc2626",
  "summary": "CRITICAL - Acme Corporation - Gone Dark",
  "sections": [
    {
      "activityTitle": "🚨 Pipeline Hygiene Alert",
      "activitySubtitle": "CRITICAL Severity",
      "activityImage": "https://adaptivecards.io/content/cats/3.png",
      "facts": [
        { "name": "Account", "value": "Acme Corporation" },
        { "name": "Amount", "value": "$1,200,000" },
        { "name": "Alert Type", "value": "Gone Dark" },
        { "name": "Owner", "value": "Sarah Chen" },
        { "name": "Days Since Activity", "value": "18" },
        { "name": "Health Score", "value": "48/100" }
      ],
      "markdown": true
    },
    {
      "text": "**Recommended Action:** Immediate outreach required - contact has gone dark for 14+ days"
    }
  ],
  "potentialAction": [
    {
      "@type": "OpenUri",
      "name": "View in Salesforce",
      "targets": [
        { "os": "default", "uri": "https://yourorg.lightning.force.com/lightning/r/Opportunity/OPP-001/view" }
      ]
    },
    {
      "@type": "OpenUri",
      "name": "View in RevOps Hub",
      "targets": [
        { "os": "default", "uri": "https://revops.yourcompany.com/?deal=OPP-001" }
      ]
    }
  ]
}
```

---

## Color/Style Mapping

### Adaptive Card Container Styles

| Severity | Style | Visual |
|----------|-------|--------|
| CRITICAL | `attention` | Red background |
| HIGH | `warning` | Yellow/orange background |
| MEDIUM | `accent` | Blue accent |
| LOW | `good` | Green accent |

### MessageCard Theme Colors

| Severity | themeColor |
|----------|------------|
| CRITICAL | `dc2626` |
| HIGH | `f97316` |
| MEDIUM | `eab308` |
| LOW | `22c55e` |

### Emoji Mapping

| Severity | Emoji |
|----------|-------|
| CRITICAL | 🚨 |
| HIGH | ⚠️ |
| MEDIUM | 🟡 |
| LOW | ✅ |

---

## Setting Up Teams Webhook

### Step 1: Create Incoming Webhook in Teams

1. Go to the Teams channel where you want alerts
2. Click `...` → `Connectors`
3. Find `Incoming Webhook` → `Configure`
4. Name it "RevOps Alerts" and upload an icon
5. Copy the webhook URL

### Step 2: Store Webhook URL Securely

In Foundry, store the webhook URL in a secure configuration:

```python
# Option 1: Environment variable (for Functions)
TEAMS_WEBHOOK_URL = os.environ.get("TEAMS_WEBHOOK_URL")

# Option 2: Foundry Secrets (recommended)
from foundry.secrets import get_secret
TEAMS_WEBHOOK_URL = get_secret("teams-webhook-url")
```

---

## Integration Options

### Option 1: Foundry Function (Recommended)

```python
import requests
from functions.api import function, Input
from ontology.objects import HygieneAlert

SEVERITY_STYLES = {
    "CRITICAL": ("attention", "🚨"),
    "HIGH": ("warning", "⚠️"),
    "MEDIUM": ("accent", "🟡"),
    "LOW": ("good", "✅")
}

@function
def send_teams_alert(alert: HygieneAlert, webhook_url: str) -> bool:
    """Send a single hygiene alert to Teams."""

    style, emoji = SEVERITY_STYLES.get(alert.alert_severity, ("default", "📋"))

    payload = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": [
                    {
                        "type": "Container",
                        "style": style,
                        "items": [{
                            "type": "TextBlock",
                            "text": f"{emoji} {alert.alert_severity} Pipeline Hygiene Alert",
                            "weight": "Bolder",
                            "size": "Medium"
                        }]
                    },
                    {
                        "type": "TextBlock",
                        "text": alert.account_name,
                        "weight": "Bolder",
                        "size": "Large"
                    },
                    {
                        "type": "FactSet",
                        "facts": [
                            {"title": "Amount", "value": f"${alert.amount:,.0f}"},
                            {"title": "Alert", "value": alert.primary_alert.replace("_", " ")},
                            {"title": "Owner", "value": alert.owner_name},
                            {"title": "Health", "value": f"{alert.health_score}/100"}
                        ]
                    },
                    {
                        "type": "TextBlock",
                        "text": f"**Action:** {alert.recommended_action}",
                        "wrap": True
                    }
                ]
            }
        }]
    }

    response = requests.post(webhook_url, json=payload)
    return response.status_code == 200
```

### Option 2: Scheduled Transform

```python
from transforms.api import transform, Input, Output
import requests

TEAMS_WEBHOOK_URL = "https://outlook.office.com/webhook/..."

@transform(
    alerts=Input("/RevOps/Analytics/hygiene_alert_feed"),
    output=Output("/RevOps/Logs/teams_notification_log")
)
def send_critical_alerts_to_teams(ctx, alerts, output):
    """Send critical alerts to Teams daily."""

    df = alerts.dataframe()

    # Filter to critical/high severity from today
    to_send = df.filter(
        df.alert_severity.isin(["CRITICAL", "HIGH"])
    ).orderBy("priority_rank").limit(10)

    results = []

    for row in to_send.collect():
        payload = build_teams_card(row)

        try:
            response = requests.post(TEAMS_WEBHOOK_URL, json=payload, timeout=10)
            status = "sent" if response.status_code == 200 else f"error:{response.status_code}"
        except Exception as e:
            status = f"error:{str(e)}"

        results.append({
            "alert_id": row.alert_id,
            "status": status,
            "sent_at": datetime.now().isoformat()
        })

    output.write_dataframe(ctx.spark_session.createDataFrame(results))


def build_teams_card(row):
    """Build Adaptive Card payload from alert row."""
    return {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                # ... card structure as shown above
            }
        }]
    }
```

### Option 3: Power Automate Flow

1. Create a new Flow triggered by HTTP request
2. Query Foundry SQL API for new alerts
3. For each alert, post to Teams channel
4. Schedule to run every 15 minutes

---

## Digest Format (Daily Summary)

For daily rollup instead of individual alerts:

```json
{
  "type": "message",
  "attachments": [{
    "contentType": "application/vnd.microsoft.card.adaptive",
    "content": {
      "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
      "type": "AdaptiveCard",
      "version": "1.4",
      "body": [
        {
          "type": "TextBlock",
          "text": "📊 Daily Pipeline Hygiene Report",
          "weight": "Bolder",
          "size": "Large"
        },
        {
          "type": "TextBlock",
          "text": "November 15, 2024",
          "isSubtle": true
        },
        {
          "type": "ColumnSet",
          "columns": [
            {
              "type": "Column",
              "width": "auto",
              "items": [
                { "type": "TextBlock", "text": "🚨 Critical", "weight": "Bolder" },
                { "type": "TextBlock", "text": "5", "size": "ExtraLarge", "color": "Attention" }
              ]
            },
            {
              "type": "Column",
              "width": "auto",
              "items": [
                { "type": "TextBlock", "text": "⚠️ High", "weight": "Bolder" },
                { "type": "TextBlock", "text": "8", "size": "ExtraLarge", "color": "Warning" }
              ]
            },
            {
              "type": "Column",
              "width": "auto",
              "items": [
                { "type": "TextBlock", "text": "📈 Score", "weight": "Bolder" },
                { "type": "TextBlock", "text": "71.5", "size": "ExtraLarge", "color": "Good" }
              ]
            }
          ]
        },
        {
          "type": "TextBlock",
          "text": "**Top Issues:**",
          "weight": "Bolder",
          "spacing": "Medium"
        },
        {
          "type": "TextBlock",
          "text": "• 4 deals gone dark ($1.48M at risk)\n• 12 single-threaded deals\n• 5 stale close dates",
          "wrap": true
        },
        {
          "type": "TextBlock",
          "text": "**Biggest Risk:** Acme Corporation ($1.2M) - 18 days dark",
          "wrap": true,
          "color": "Attention"
        }
      ],
      "actions": [
        {
          "type": "Action.OpenUrl",
          "title": "Open Dashboard",
          "url": "https://revops.yourcompany.com"
        }
      ]
    }
  }]
}
```

---

## Routing by Channel

### Channel Strategy

| Channel | Audience | Alert Types |
|---------|----------|-------------|
| #sales-alerts | All Sales | CRITICAL only |
| #sales-leadership | Managers | Daily digest |
| #sales-west | West Team | Team-specific alerts |
| DM to rep | Individual | Personal alerts |

### Implementation

```python
CHANNEL_WEBHOOKS = {
    "all": "https://outlook.office.com/webhook/all-sales/...",
    "leadership": "https://outlook.office.com/webhook/leadership/...",
    "west": "https://outlook.office.com/webhook/west-team/...",
    "east": "https://outlook.office.com/webhook/east-team/...",
}

def route_alert(alert):
    """Determine which channel(s) should receive this alert."""
    channels = []

    # Critical goes to all-sales
    if alert.alert_severity == "CRITICAL":
        channels.append("all")

    # Team-specific routing
    owner_team = get_owner_team(alert.owner_id)
    if owner_team in CHANNEL_WEBHOOKS:
        channels.append(owner_team)

    return channels
```

---

## Best Practices

1. **Rate Limiting**
   - Teams webhooks have a limit of ~4 messages/second
   - Batch alerts and add delays between sends
   - Use digest format for high-volume scenarios

2. **Deduplication**
   - Track sent alerts to avoid duplicates
   - Use `alert_id` as idempotency key
   - Skip alerts already sent in last 24 hours

3. **Timing**
   - Critical: Send immediately
   - High: Batch every 15 minutes
   - Daily digest: 8am local time

4. **Testing**
   - Use a test channel first
   - Validate card rendering in Teams
   - Test on mobile app too

---

## Related Datasets

| Dataset | Description |
|---------|-------------|
| `/RevOps/Analytics/pipeline_hygiene_alerts` | Full alert details per opportunity |
| `/RevOps/Analytics/pipeline_hygiene_summary` | Aggregate metrics |
| `/RevOps/Analytics/pipeline_hygiene_by_owner` | Per-rep coaching metrics |
| `/RevOps/Analytics/pipeline_hygiene_trends` | Historical trend data |
| `/RevOps/Analytics/hygiene_alert_feed` | Webhook-ready payloads |
