# Data Ingestion Guide

This guide covers how to connect external data sources to the RevOps Hub platform.

## Overview

The RevOps Hub supports data ingestion from multiple source systems:

| Source Type | Supported Systems | Sync Method |
|-------------|-------------------|-------------|
| CRM | Salesforce, HubSpot | Batch + CDC |
| Marketing | Marketo, Pardot, HubSpot Marketing | Batch |
| Support | Zendesk, Jira Service Desk, Intercom | Batch |
| Product Analytics | Snowflake, BigQuery, Amplitude, Mixpanel | Batch |

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           EXTERNAL SOURCES                                   │
│  Salesforce │ HubSpot │ Zendesk │ Marketo │ Snowflake │ Product Analytics   │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         FOUNDRY CONNECTORS                                   │
│  • Salesforce Connector (OAuth)    • REST API Connectors                    │
│  • HubSpot Connector               • JDBC Connectors (Snowflake/BigQuery)   │
│  • Zendesk Connector               • S3/GCS File Imports                    │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            RAW DATASETS                                      │
│  /Salesforce/Raw/*  │  /HubSpot/Raw/*  │  /Zendesk/Raw/*  │  /Analytics/*   │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        INGESTION TRANSFORMS                                  │
│  transforms/ingestion/salesforce_sync.py                                    │
│  transforms/ingestion/hubspot_sync.py                                       │
│  transforms/ingestion/telemetry_feeds.py                                    │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         STAGING DATASETS                                     │
│  /RevOps/Staging/opportunities  │  /RevOps/Staging/accounts                 │
│  /RevOps/Staging/contacts       │  /RevOps/Staging/activities               │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Salesforce Setup

#### Prerequisites
- Salesforce org with API access enabled
- Connected App configured in Salesforce Setup
- OAuth credentials (Client ID, Client Secret)

#### Configuration Steps

1. **Create Salesforce Connection in Foundry**

   Navigate to Foundry Data Connection and create a new Salesforce source:

   ```
   Connection Name: salesforce-prod
   Instance URL: https://yourorg.my.salesforce.com
   Auth Type: OAuth 2.0
   Client ID: [from Connected App]
   Client Secret: [from Connected App]
   ```

2. **Configure Sync Objects**

   Enable sync for the following Salesforce objects:

   | Object | Sync Mode | Frequency | Primary Key |
   |--------|-----------|-----------|-------------|
   | Opportunity | Incremental | Hourly | Id |
   | Account | Incremental | Daily | Id |
   | Contact | Incremental | Daily | Id |
   | Task | Incremental | Hourly | Id |
   | Event | Incremental | Hourly | Id |
   | User | Full | Daily | Id |

3. **Create Raw Datasets**

   ```
   /Salesforce/Raw/Opportunity
   /Salesforce/Raw/Account
   /Salesforce/Raw/Contact
   /Salesforce/Raw/Task
   /Salesforce/Raw/Event
   /Salesforce/Raw/User
   ```

4. **Deploy Ingestion Transforms**

   Build the ingestion transforms in order:
   ```bash
   foundry-cli build /RevOps/Staging/opportunities
   foundry-cli build /RevOps/Staging/accounts
   foundry-cli build /RevOps/Staging/contacts
   foundry-cli build /RevOps/Staging/activities
   ```

5. **Configure Field Mapping (Optional)**

   Create `/RevOps/Config/salesforce_field_map` dataset with custom mappings:

   | sf_field | revops_field |
   |----------|--------------|
   | Custom_Field__c | custom_field |
   | MyCompetitor__c | competitor |

### 2. HubSpot Setup

#### Prerequisites
- HubSpot account with API access
- Private App created with required scopes

#### Required Scopes
- `crm.objects.deals.read`
- `crm.objects.companies.read`
- `crm.objects.contacts.read`
- `sales-email-read`
- `crm.objects.owners.read`

#### Configuration Steps

1. **Create HubSpot Connection**

   ```
   Connection Name: hubspot-prod
   API Key: [Private App Token]
   ```

2. **Configure Sync Objects**

   | Object | Endpoint | Frequency |
   |--------|----------|-----------|
   | Deals | /crm/v3/objects/deals | Hourly |
   | Companies | /crm/v3/objects/companies | Daily |
   | Contacts | /crm/v3/objects/contacts | Daily |
   | Owners | /crm/v3/owners | Daily |
   | Engagements | /engagements/v1/engagements | Hourly |

3. **Stage Mapping**

   HubSpot deal stages are automatically mapped to RevOps stages:

   | HubSpot Stage | RevOps Stage |
   |---------------|--------------|
   | appointmentscheduled | Discovery |
   | qualifiedtobuy | Solution Design |
   | presentationscheduled | Proposal |
   | decisionmakerboughtin | Negotiation |
   | contractsent | Verbal Commit |
   | closedwon | Closed Won |
   | closedlost | Closed Lost |

### 3. Support Data (Zendesk)

#### Configuration Steps

1. **Create Zendesk Connection**

   ```
   Connection Name: zendesk-prod
   Subdomain: yourcompany
   Auth Type: API Token
   Email: admin@yourcompany.com
   Token: [Zendesk API Token]
   ```

2. **Configure Sync**

   | Endpoint | Dataset | Frequency |
   |----------|---------|-----------|
   | /api/v2/tickets | /Zendesk/Raw/Tickets | Hourly |
   | /api/v2/organizations | /Zendesk/Raw/Organizations | Daily |
   | /api/v2/users | /Zendesk/Raw/Users | Daily |

3. **Account Mapping**

   Create `/RevOps/Reference/account_domain_mapping` to link tickets to accounts:

   | domain | account_id |
   |--------|------------|
   | acme.com | ACC-001 |
   | globex.com | ACC-002 |

### 4. Product Analytics

#### Snowflake Setup

1. **Configure Snowflake Connection**

   ```
   Connection Name: snowflake-analytics
   Account: your-account.snowflakecomputing.com
   Warehouse: ANALYTICS_WH
   Database: PRODUCT_ANALYTICS
   Schema: PUBLIC
   Auth: OAuth or Username/Password
   ```

2. **Sync Product Events**

   ```sql
   -- Source query for product events
   SELECT
       event_id,
       user_id,
       account_id,
       event_name,
       event_timestamp,
       properties
   FROM product_events
   WHERE event_timestamp >= DATEADD(day, -30, CURRENT_DATE())
   ```

## Scheduling

### Recommended Schedule

| Data Type | Frequency | Rationale |
|-----------|-----------|-----------|
| Opportunities | Hourly | Deal status changes frequently |
| Activities | Hourly | Recent activity drives health scores |
| Accounts | Daily | Master data changes infrequently |
| Contacts | Daily | Master data changes infrequently |
| Support Tickets | Hourly | Affects customer health in real-time |
| Product Usage | Daily | Aggregated daily metrics |
| Marketing | 4-hourly | Campaign attribution |

### Foundry Schedule Configuration

```yaml
# Example schedule.yaml
schedules:
  - name: hourly-crm-sync
    cron: "0 * * * *"
    datasets:
      - /RevOps/Staging/opportunities
      - /RevOps/Staging/activities

  - name: daily-master-sync
    cron: "0 6 * * *"
    datasets:
      - /RevOps/Staging/accounts
      - /RevOps/Staging/contacts

  - name: hourly-support-sync
    cron: "30 * * * *"
    datasets:
      - /RevOps/Telemetry/support_tickets
```

## Data Quality Checks

Each ingestion pipeline includes automatic data quality validation:

```python
from transforms.quality.data_quality_checks import check

# In your transform
result = (
    check(df)
    .assert_not_empty(min_rows=1)
    .assert_no_nulls("opportunity_id", "account_id")
    .assert_unique("opportunity_id")
    .assert_values_in("stage_name", VALID_STAGES)
    .finalize()
)
```

### Quality Alerts

Configure alerts for sync failures in `/RevOps/Config/quality_alerts`:

| Condition | Severity | Notification |
|-----------|----------|--------------|
| Sync failed | Critical | PagerDuty |
| >5% null values | High | Slack #data-quality |
| Row count drop >20% | Medium | Email |

## Troubleshooting

### Common Issues

#### 1. OAuth Token Expired

**Symptom:** Sync fails with 401 error

**Solution:** Refresh the OAuth token in Foundry Data Connection settings

#### 2. Rate Limiting

**Symptom:** Sync fails with 429 error

**Solution:**
- Reduce sync frequency
- Enable exponential backoff in connector settings
- Request API limit increase from vendor

#### 3. Schema Changes

**Symptom:** Transform fails with column not found

**Solution:**
- Review source system for schema changes
- Update field mapping in `/RevOps/Config/*_field_map`
- Add defensive null handling in transforms

#### 4. Duplicate Records

**Symptom:** Row count increases unexpectedly

**Solution:**
- Verify incremental sync using `LastModifiedDate`
- Check for deleted record handling
- Review deduplication logic in transforms

## Multi-CRM Setup

For organizations using both Salesforce and HubSpot:

### Deduplication Strategy

1. **Primary Source Designation**

   Configure which system is primary per object type:

   ```python
   PRIMARY_SOURCE = {
       "opportunities": "Salesforce",
       "marketing_activities": "HubSpot"
   }
   ```

2. **Account Matching**

   Create account matching rules in `/RevOps/Config/account_matching_rules`:

   | Match Type | Priority | Fields |
   |------------|----------|--------|
   | Exact | 1 | domain |
   | Fuzzy | 2 | name (>90% similarity) |
   | Manual | 3 | account_id mapping |

3. **Unified Datasets**

   The staging transforms automatically merge data:

   ```
   /RevOps/Staging/opportunities  ← Salesforce + HubSpot (deduped)
   /RevOps/Staging/accounts       ← Salesforce + HubSpot (deduped)
   ```

## Security Considerations

### Credential Management

- Store API credentials in Foundry Secrets Manager
- Never commit credentials to code repositories
- Rotate API tokens quarterly

### Data Access

- Raw datasets should have restricted access (Data Engineers only)
- Staging datasets can have broader read access
- PII fields should be masked in non-production environments

### Audit Logging

All data syncs are logged to `/RevOps/Audit/sync_log`:

| Field | Description |
|-------|-------------|
| sync_id | Unique sync identifier |
| source_system | Salesforce, HubSpot, etc. |
| object_type | Opportunity, Account, etc. |
| start_time | Sync start timestamp |
| end_time | Sync end timestamp |
| records_processed | Total records synced |
| records_failed | Failed record count |
| error_message | Error details if failed |

## Support

For ingestion issues:
1. Check `/RevOps/Monitoring/sync_status` dashboard
2. Review Foundry build logs
3. Contact Data Engineering team via #data-eng Slack channel
