# RevOps Hub API Documentation

API reference for RevOps Hub. Available on Growth and Enterprise tiers.

## Overview

### Base URL

```
https://api.revops-hub.io/v1
```

### Authentication

RevOps Hub uses OAuth 2.0 client credentials flow:

```bash
# Get access token
curl -X POST https://api.revops-hub.io/oauth/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=YOUR_CLIENT_ID" \
  -d "client_secret=YOUR_CLIENT_SECRET"

# Response
{
  "access_token": "eyJhbGciOiJSUzI1NiIs...",
  "token_type": "Bearer",
  "expires_in": 3600
}
```

Use the token in subsequent requests:

```bash
curl https://api.revops-hub.io/v1/pipeline/health \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

### Rate Limits

| Tier | Requests/Hour | Burst |
|------|---------------|-------|
| Growth | 1,000 | 100 |
| Enterprise | 10,000 | 500 |

Rate limit headers:
- `X-RateLimit-Limit`: Max requests per hour
- `X-RateLimit-Remaining`: Remaining requests
- `X-RateLimit-Reset`: Unix timestamp when limit resets

### Response Format

All responses are JSON:

```json
{
  "data": { ... },
  "meta": {
    "request_id": "req_abc123",
    "timestamp": "2024-01-15T10:30:00Z"
  }
}
```

### Errors

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid date format",
    "details": {
      "field": "close_date",
      "expected": "YYYY-MM-DD"
    }
  },
  "meta": {
    "request_id": "req_abc123"
  }
}
```

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `UNAUTHORIZED` | 401 | Invalid or expired token |
| `FORBIDDEN` | 403 | Insufficient permissions |
| `NOT_FOUND` | 404 | Resource not found |
| `VALIDATION_ERROR` | 400 | Invalid request parameters |
| `RATE_LIMITED` | 429 | Too many requests |
| `SERVER_ERROR` | 500 | Internal server error |

---

## Pipeline API

### Get Pipeline Health Summary

```
GET /pipeline/health
```

Returns pipeline health metrics and deal counts by status.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `owner_id` | string | Filter by deal owner |
| `segment` | string | Filter by segment |
| `min_amount` | number | Minimum deal amount |

**Example:**

```bash
curl -X GET "https://api.revops-hub.io/v1/pipeline/health?segment=Enterprise" \
  -H "Authorization: Bearer TOKEN"
```

**Response:**

```json
{
  "data": {
    "total_pipeline": 5420000,
    "deal_count": 127,
    "health_distribution": {
      "healthy": { "count": 45, "amount": 2100000 },
      "monitor": { "count": 38, "amount": 1650000 },
      "at_risk": { "count": 29, "amount": 1120000 },
      "critical": { "count": 15, "amount": 550000 }
    },
    "avg_health_score": 67.3,
    "updated_at": "2024-01-15T10:00:00Z"
  }
}
```

### List Deals

```
GET /pipeline/deals
```

Returns list of deals with health scores.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `status` | string | `open` | `open`, `won`, `lost`, `all` |
| `health` | string | | Filter by health: `healthy`, `monitor`, `at_risk`, `critical` |
| `owner_id` | string | | Filter by owner |
| `segment` | string | | Filter by segment |
| `stage` | string | | Filter by stage |
| `min_amount` | number | | Minimum amount |
| `max_amount` | number | | Maximum amount |
| `sort` | string | `-amount` | Sort field (prefix `-` for desc) |
| `limit` | integer | 50 | Results per page (max 100) |
| `offset` | integer | 0 | Pagination offset |

**Example:**

```bash
curl -X GET "https://api.revops-hub.io/v1/pipeline/deals?health=critical&limit=10" \
  -H "Authorization: Bearer TOKEN"
```

**Response:**

```json
{
  "data": {
    "deals": [
      {
        "opportunity_id": "006xxx",
        "opportunity_name": "Acme Corp - Platform",
        "account_name": "Acme Corporation",
        "owner_name": "John Smith",
        "amount": 125000,
        "stage_name": "Negotiation",
        "close_date": "2024-02-15",
        "health_score": 35,
        "health_status": "critical",
        "win_probability": 0.28,
        "risk_factors": ["No recent activity", "Single-threaded"],
        "days_in_stage": 21
      }
    ],
    "pagination": {
      "total": 15,
      "limit": 10,
      "offset": 0,
      "has_more": true
    }
  }
}
```

### Get Deal Details

```
GET /pipeline/deals/{opportunity_id}
```

Returns detailed information for a single deal.

**Response:**

```json
{
  "data": {
    "opportunity_id": "006xxx",
    "opportunity_name": "Acme Corp - Platform",
    "account_id": "001xxx",
    "account_name": "Acme Corporation",
    "owner_id": "005xxx",
    "owner_name": "John Smith",
    "amount": 125000,
    "stage_name": "Negotiation",
    "probability": 0.75,
    "close_date": "2024-02-15",
    "created_date": "2023-11-01",
    "health": {
      "score": 35,
      "status": "critical",
      "factors": {
        "activity_recency": 0.2,
        "stage_velocity": 0.4,
        "engagement": 0.3,
        "multi_threading": 0.5
      }
    },
    "prediction": {
      "win_probability": 0.28,
      "confidence": "medium",
      "risk_factors": ["No recent activity", "Single-threaded"]
    },
    "activities": {
      "total": 12,
      "last_7_days": 0,
      "last_activity_date": "2024-01-01"
    },
    "contacts": [
      {
        "contact_id": "003xxx",
        "name": "Jane Doe",
        "title": "VP Engineering",
        "is_primary": true
      }
    ],
    "recommendations": [
      {
        "action_type": "REACTIVATE_STALE",
        "priority": 95,
        "reason": "No activity in 14 days"
      }
    ]
  }
}
```

### Get Hygiene Alerts

```
GET /pipeline/hygiene
```

Returns pipeline hygiene issues.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `alert_type` | string | Filter by type: `stale`, `past_close`, `missing_next_step` |
| `owner_id` | string | Filter by owner |
| `severity` | string | `warning` or `critical` |

**Response:**

```json
{
  "data": {
    "summary": {
      "total_alerts": 45,
      "critical": 12,
      "warning": 33
    },
    "alerts": [
      {
        "opportunity_id": "006xxx",
        "opportunity_name": "Acme Corp - Platform",
        "alert_type": "stale",
        "severity": "critical",
        "message": "No activity in 21 days",
        "amount": 125000,
        "owner_name": "John Smith"
      }
    ]
  }
}
```

---

## Forecast API

### Get Forecast Summary

```
GET /forecast/summary
```

Returns current forecast by category.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `period` | string | `current_quarter`, `next_quarter`, `current_year` |
| `owner_id` | string | Filter by owner |
| `segment` | string | Filter by segment |

**Response:**

```json
{
  "data": {
    "period": "Q1 2024",
    "quota": 5000000,
    "categories": {
      "closed": { "amount": 1250000, "count": 15 },
      "commit": { "amount": 875000, "count": 8 },
      "best_case": { "amount": 1420000, "count": 22 },
      "pipeline": { "amount": 3150000, "count": 67 }
    },
    "metrics": {
      "attainment": 0.25,
      "coverage": 3.4,
      "weighted_pipeline": 2890000,
      "forecast_accuracy": 0.82
    },
    "updated_at": "2024-01-15T10:00:00Z"
  }
}
```

### Get Forecast Trends

```
GET /forecast/trends
```

Returns historical forecast data for trend analysis.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `period` | string | `current_quarter` | Period to analyze |
| `granularity` | string | `weekly` | `daily`, `weekly`, `monthly` |

**Response:**

```json
{
  "data": {
    "period": "Q1 2024",
    "trends": [
      {
        "date": "2024-01-01",
        "closed": 800000,
        "commit": 1200000,
        "best_case": 1800000,
        "pipeline": 4500000
      },
      {
        "date": "2024-01-08",
        "closed": 950000,
        "commit": 1100000,
        "best_case": 1650000,
        "pipeline": 4200000
      }
    ]
  }
}
```

---

## Coaching API

### Get Rep Performance

```
GET /coaching/performance
```

Returns performance metrics for sales reps.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `rep_id` | string | Filter by rep ID |
| `manager_id` | string | Filter by manager |
| `period` | string | `current_quarter`, `last_quarter`, `ytd` |

**Response:**

```json
{
  "data": {
    "reps": [
      {
        "rep_id": "005xxx",
        "rep_name": "John Smith",
        "manager_name": "Jane Doe",
        "metrics": {
          "quota": 500000,
          "closed_won": 375000,
          "attainment": 0.75,
          "pipeline": 1250000,
          "coverage": 2.5,
          "win_rate": 0.32,
          "avg_deal_size": 45000,
          "avg_cycle_days": 38
        },
        "rank": 3,
        "trend": "improving"
      }
    ]
  }
}
```

### Get Coaching Insights

```
GET /coaching/insights/{rep_id}
```

Returns AI-generated coaching insights for a rep.

**Response:**

```json
{
  "data": {
    "rep_id": "005xxx",
    "rep_name": "John Smith",
    "insights": {
      "strengths": [
        "Strong discovery process - 85% of deals have documented pain points",
        "High activity volume - 40% above team average"
      ],
      "opportunities": [
        "Multi-threading - only 1.5 contacts per deal vs. 3.2 team avg",
        "Proposal timing - deals spending 2x benchmark time in Solution Design"
      ],
      "recommendations": [
        {
          "action": "Identify and engage economic buyer early",
          "impact": "Estimated 15% improvement in win rate",
          "priority": "high"
        }
      ]
    },
    "generated_at": "2024-01-15T10:00:00Z"
  }
}
```

---

## AI/ML API

### Get Deal Predictions

```
GET /ml/deal-scores
```

Returns AI-predicted win probabilities.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `min_probability` | number | Minimum win probability (0-1) |
| `score_band` | string | `high`, `medium`, `low` |

**Response:**

```json
{
  "data": {
    "deals": [
      {
        "opportunity_id": "006xxx",
        "opportunity_name": "Acme Corp - Platform",
        "win_probability": 0.73,
        "score_band": "high",
        "confidence": "high",
        "risk_factors": [],
        "scored_at": "2024-01-15T10:00:00Z"
      }
    ]
  }
}
```

### Get Next-Best-Actions

```
GET /ml/recommendations
```

Returns prioritized action recommendations.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `owner_id` | string | Filter by owner |
| `category` | string | Filter: `engagement`, `progression`, `hygiene`, `strategy` |
| `limit` | integer | Max results (default 20) |

**Response:**

```json
{
  "data": {
    "recommendations": [
      {
        "opportunity_id": "006xxx",
        "opportunity_name": "Acme Corp - Platform",
        "action_type": "SCHEDULE_MEETING",
        "priority": 95,
        "category": "engagement",
        "reason": "No activity in 14 days on high-value deal",
        "impact": "high",
        "suggested_action": "Schedule discovery meeting with Jane Doe"
      }
    ]
  }
}
```

### Get Churn Risk

```
GET /ml/churn-risk
```

*(Enterprise tier only)*

Returns customer churn risk predictions.

**Response:**

```json
{
  "data": {
    "customers": [
      {
        "account_id": "001xxx",
        "account_name": "Acme Corporation",
        "churn_probability": 0.72,
        "risk_tier": "high",
        "arr": 120000,
        "risk_factors": [
          "Declining product usage",
          "No recent customer success contact"
        ],
        "recommended_action": "Schedule QBR within 1 week"
      }
    ],
    "summary": {
      "total_arr_at_risk": 450000,
      "critical_count": 3,
      "high_count": 8
    }
  }
}
```

---

## Webhooks

### Configuring Webhooks

Register a webhook endpoint:

```bash
curl -X POST https://api.revops-hub.io/v1/webhooks \
  -H "Authorization: Bearer TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://your-server.com/webhook",
    "events": ["deal.health_changed", "alert.triggered"],
    "secret": "your-webhook-secret"
  }'
```

### Event Types

| Event | Description |
|-------|-------------|
| `deal.health_changed` | Deal health status changed |
| `deal.stage_changed` | Deal moved stages |
| `alert.triggered` | New alert triggered |
| `alert.resolved` | Alert resolved |
| `build.completed` | Data build completed |
| `build.failed` | Data build failed |

### Webhook Payload

```json
{
  "event": "deal.health_changed",
  "timestamp": "2024-01-15T10:30:00Z",
  "data": {
    "opportunity_id": "006xxx",
    "previous_status": "monitor",
    "new_status": "critical",
    "health_score": 35
  },
  "signature": "sha256=abc123..."
}
```

### Verifying Signatures

```python
import hmac
import hashlib

def verify_webhook(payload, signature, secret):
    expected = hmac.new(
        secret.encode(),
        payload.encode(),
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(f"sha256={expected}", signature)
```

---

## SDKs

### Python SDK

```bash
pip install revops-hub
```

```python
from revops_hub import Client

client = Client(
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_CLIENT_SECRET"
)

# Get pipeline health
health = client.pipeline.get_health(segment="Enterprise")
print(f"Total pipeline: ${health.total_pipeline:,}")

# List critical deals
deals = client.pipeline.list_deals(health="critical")
for deal in deals:
    print(f"{deal.name}: {deal.health_score}")

# Get recommendations
actions = client.ml.get_recommendations(limit=10)
for action in actions:
    print(f"[{action.priority}] {action.suggested_action}")
```

### JavaScript SDK

```bash
npm install @revops-hub/sdk
```

```javascript
import { RevOpsClient } from '@revops-hub/sdk';

const client = new RevOpsClient({
  clientId: 'YOUR_CLIENT_ID',
  clientSecret: 'YOUR_CLIENT_SECRET'
});

// Get forecast
const forecast = await client.forecast.getSummary({
  period: 'current_quarter'
});
console.log(`Coverage: ${forecast.metrics.coverage}x`);

// Get deal scores
const scores = await client.ml.getDealScores({
  scoreBand: 'low'
});
```

---

## Changelog

### v1.2.0 (2024-01)

- Added churn risk endpoint
- Added webhook support
- Improved rate limits for Enterprise tier

### v1.1.0 (2023-10)

- Added ML prediction endpoints
- Added coaching insights API
- Added forecast trends endpoint

### v1.0.0 (2023-07)

- Initial API release
- Pipeline and forecast endpoints
- Basic deal management
