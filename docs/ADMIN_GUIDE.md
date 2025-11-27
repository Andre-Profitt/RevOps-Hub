# RevOps Hub Administrator Guide

This guide covers administration, configuration, and maintenance of RevOps Hub.

## Table of Contents

1. [Initial Setup](#initial-setup)
2. [User Management](#user-management)
3. [Configuration](#configuration)
4. [CRM Integration](#crm-integration)
5. [Data Pipeline Management](#data-pipeline-management)
6. [Monitoring & Alerting](#monitoring--alerting)
7. [Security & Compliance](#security--compliance)
8. [Troubleshooting](#troubleshooting)

---

## Initial Setup

### Workspace Provisioning

New customer workspaces are provisioned using the provisioning script:

```bash
# Preview what will be created
python scripts/provision_workspace.py \
  --customer "Acme Corp" \
  --tier enterprise \
  --crm salesforce \
  --dry-run

# Execute provisioning
python scripts/provision_workspace.py \
  --customer "Acme Corp" \
  --tier enterprise \
  --crm salesforce \
  --timezone "America/Los_Angeles" \
  --output results.json
```

### What Gets Created

| Resource | Description |
|----------|-------------|
| Folder Structure | `/RevOps/*` folders in Foundry |
| Datasets | Placeholders for all data tables |
| Schedules | Automated build schedules |
| Permission Groups | `{customer}-revops-*` groups |
| Configuration | `configs/{customer}.json` file |

### Post-Provisioning Steps

1. **Configure CRM Connection**: Set up OAuth credentials
2. **Invite Admin User**: Add first customer admin
3. **Run Initial Sync**: Trigger first data load
4. **Verify Data**: Check data quality metrics
5. **Enable Alerts**: Configure notification channels

---

## User Management

### Role Assignment

Assign users to appropriate roles:

```
{customer}-revops-admins     → customer_admin role
{customer}-revops-analysts   → revops_analyst role
{customer}-revops-managers   → sales_manager role
{customer}-revops-reps       → sales_rep role
```

### Adding Users

1. Navigate to **Settings > Users**
2. Click **Add User**
3. Enter email and select role
4. User receives invitation email

### Bulk Import

Import users from CSV:

```csv
email,first_name,last_name,role,manager_email
john@acme.com,John,Smith,sales_rep,jane@acme.com
jane@acme.com,Jane,Doe,sales_manager,
```

### SSO Configuration

*(Enterprise tier only)*

Configure SAML 2.0 SSO:

```json
{
  "sso_config": {
    "enabled": true,
    "provider": "okta",
    "entity_id": "https://acme.okta.com/app/xxx",
    "sso_url": "https://acme.okta.com/app/xxx/sso/saml",
    "certificate": "-----BEGIN CERTIFICATE-----...",
    "attribute_mapping": {
      "email": "user.email",
      "name": "user.displayName",
      "role": "user.groups"
    }
  }
}
```

---

## Configuration

### Customer Settings

Edit customer configuration in `configs/{customer_id}.json`:

#### Thresholds

```json
{
  "thresholds": {
    "health": {
      "critical": 40,
      "at_risk": 60,
      "monitor": 80
    },
    "hygiene": {
      "days_stale_warning": 7,
      "days_stale_critical": 14,
      "days_past_close_warning": 7
    },
    "forecast": {
      "pipeline_coverage_target": 3.0,
      "win_rate_benchmark": 0.25
    }
  }
}
```

#### Feature Flags

Enable/disable features per tier:

```json
{
  "features": {
    "pipeline_health": true,
    "deal_predictions": true,
    "coaching_insights": true,
    "scenario_modeling": false,
    "api_access": true
  }
}
```

#### Custom Stages

Override default stage configuration:

```json
{
  "stages": [
    {"name": "Qualification", "order": 1, "probability": 0.10, "benchmark_days": 7},
    {"name": "Discovery", "order": 2, "probability": 0.20, "benchmark_days": 14},
    {"name": "Demo/POC", "order": 3, "probability": 0.40, "benchmark_days": 21},
    {"name": "Proposal", "order": 4, "probability": 0.60, "benchmark_days": 7},
    {"name": "Negotiation", "order": 5, "probability": 0.80, "benchmark_days": 14},
    {"name": "Closed Won", "order": 6, "probability": 1.00, "benchmark_days": 0, "is_closed": true, "is_won": true},
    {"name": "Closed Lost", "order": 7, "probability": 0.00, "benchmark_days": 0, "is_closed": true}
  ]
}
```

### Environment Variables

Required environment variables:

| Variable | Description | Required |
|----------|-------------|----------|
| `FOUNDRY_URL` | Foundry instance URL | Yes |
| `FOUNDRY_TOKEN` | Service account token | Yes |
| `SALESFORCE_CLIENT_ID` | OAuth client ID | If using SF |
| `SALESFORCE_CLIENT_SECRET` | OAuth client secret | If using SF |
| `HUBSPOT_API_KEY` | HubSpot API key | If using HS |
| `SLACK_WEBHOOK_URL` | Slack notifications | Optional |
| `SMTP_HOST` | Email server | Optional |

---

## CRM Integration

### Salesforce Setup

1. **Create Connected App**
   - Login to Salesforce Setup
   - Navigate to App Manager > New Connected App
   - Enable OAuth settings
   - Add callback URL: `https://your-instance/oauth/callback`
   - Select scopes: `api`, `refresh_token`, `offline_access`

2. **Configure Field Mapping**

   Edit `/RevOps/Config/salesforce_field_map`:

   ```json
   {
     "Opportunity": {
       "opportunity_id": "Id",
       "opportunity_name": "Name",
       "account_id": "AccountId",
       "owner_id": "OwnerId",
       "amount": "Amount",
       "stage_name": "StageName",
       "close_date": "CloseDate"
     }
   }
   ```

3. **Test Connection**
   ```bash
   python scripts/test_crm_connection.py --crm salesforce
   ```

### HubSpot Setup

1. **Create Private App**
   - Login to HubSpot
   - Navigate to Settings > Integrations > Private Apps
   - Create new app with required scopes

2. **Configure API Key**
   - Store API key in environment
   - Update customer config with HubSpot as primary CRM

### Sync Frequency

| Tier | Default Frequency | Adjustable |
|------|-------------------|------------|
| Starter | Daily | No |
| Growth | Hourly | Yes (15min - 24hr) |
| Enterprise | 15 minutes | Yes (5min - 24hr) |

---

## Data Pipeline Management

### Build Schedules

Default schedules:

| Schedule | Cron | Description |
|----------|------|-------------|
| `hourly-sync` | `0 * * * *` | CRM data sync |
| `daily-analytics` | `0 6 * * *` | Core analytics |
| `weekly-reports` | `0 8 * * 1` | Weekly rollups |

### Manual Builds

Trigger a build manually:

```bash
# Single dataset
foundry-cli build /RevOps/Analytics/pipeline_health_summary

# All analytics
foundry-cli build /RevOps/Analytics/*

# Full rebuild
python scripts/trigger_full_rebuild.py --customer acme-corp
```

### Monitoring Builds

View build status in Foundry or query:

```sql
SELECT
  dataset_path,
  status,
  duration_seconds,
  error_message
FROM `/RevOps/Monitoring/build_metrics`
WHERE build_timestamp > current_timestamp() - INTERVAL 24 HOURS
ORDER BY build_timestamp DESC
```

### Handling Failures

1. Check build logs in Foundry
2. Common issues:
   - **Schema mismatch**: Source field renamed/removed
   - **Timeout**: Large data volume, optimize query
   - **Auth failure**: Refresh CRM credentials
   - **Resource limits**: Request capacity increase

---

## Monitoring & Alerting

### System Health Dashboard

Access at **Settings > System Health**:

- Build success rate
- Data freshness
- Sync latency
- Active alerts

### Configuring Alerts

Edit alert configuration:

```json
{
  "alerts": {
    "enabled": true,
    "default_channels": ["email", "slack"],
    "slack_webhook_url": "https://hooks.slack.com/...",
    "slack_channel": "#revops-alerts",
    "email_recipients": ["ops@company.com"],
    "rules": [
      {
        "name": "stale_pipeline",
        "enabled": true,
        "severity": "warning",
        "threshold": 0.10,
        "cooldown_minutes": 60
      },
      {
        "name": "build_failure",
        "enabled": true,
        "severity": "critical",
        "cooldown_minutes": 30
      }
    ]
  }
}
```

### Alert Types

| Alert | Default Threshold | Severity |
|-------|-------------------|----------|
| `stale_pipeline` | 10% stale deals | Warning |
| `build_failure` | Any failure | Critical |
| `data_quality_drop` | Score < 90% | Warning |
| `sync_delay` | > 60 minutes | Warning |
| `forecast_miss` | > 20% variance | Warning |

### SLA Monitoring

Track SLA compliance:

```sql
SELECT
  customer_id,
  build_success_rate,
  freshness_rate,
  overall_sla_met
FROM `/RevOps/Ops/sla_tracking`
WHERE snapshot_time > current_timestamp() - INTERVAL 7 DAYS
```

---

## Security & Compliance

### Access Control

See [RBAC.md](./RBAC.md) for detailed access control documentation.

#### Key Principles

1. **Least Privilege**: Assign minimum required permissions
2. **Separation of Duties**: Admins shouldn't have rep access
3. **Regular Review**: Audit access quarterly
4. **Row-Level Security**: Reps see only their data

### Audit Logging

All access is logged to `/RevOps/Audit/access_log`:

```sql
SELECT *
FROM `/RevOps/Audit/access_log`
WHERE user_id = 'user-123'
  AND timestamp > current_timestamp() - INTERVAL 30 DAYS
ORDER BY timestamp DESC
```

### Data Retention

| Data Type | Default Retention | Configurable |
|-----------|-------------------|--------------|
| Operational data | 365 days | Yes |
| Audit logs | 2 years | No (compliance) |
| Build metrics | 90 days | Yes |
| Alert history | 90 days | Yes |

### Compliance Features

- **SOC 2**: Audit logs, access controls
- **GDPR**: Data deletion, export capabilities
- **HIPAA**: (Enterprise) Encryption, BAA available

---

## Troubleshooting

### Common Issues

#### Data Not Refreshing

1. Check build status in Foundry
2. Verify CRM credentials are valid
3. Check for upstream failures
4. Review error logs

```bash
# Check recent build failures
python scripts/check_build_status.py --hours 24
```

#### Missing Deals

1. Verify deal exists in CRM
2. Check sync filters (e.g., record type)
3. Review field mapping
4. Check for schema validation errors

```sql
-- Check for validation errors
SELECT *
FROM `/RevOps/Monitoring/contract_violations`
WHERE detected_at > current_timestamp() - INTERVAL 24 HOURS
```

#### Incorrect Health Scores

1. Verify stage benchmarks are configured
2. Check activity data is syncing
3. Review threshold settings
4. Ensure contact associations exist

#### Slow Performance

1. Check data volume vs. tier limits
2. Review query patterns
3. Consider data partitioning
4. Contact support for optimization

### Support Escalation

For issues requiring Anthropic support:

1. Gather diagnostic information:
   ```bash
   python scripts/generate_diagnostics.py --output diag.json
   ```

2. Include:
   - Customer ID
   - Error messages
   - Steps to reproduce
   - Diagnostic output

3. Contact: support@revops-hub.io

### Maintenance Windows

Scheduled maintenance occurs:
- **Weekly**: Sunday 2-4 AM (customer timezone)
- **Monthly**: First Sunday, extended window

Notifications sent 48 hours in advance.

---

## Appendix: CLI Commands

### Provisioning

```bash
# Provision new workspace
python scripts/provision_workspace.py --customer "Name" --tier starter

# List customer configs
python scripts/list_customers.py

# Update customer tier
python scripts/update_tier.py --customer cust-id --tier growth
```

### Data Management

```bash
# Trigger sync
python scripts/trigger_sync.py --customer cust-id --source salesforce

# Validate data contracts
python scripts/validate_contracts.py --customer cust-id

# Export data
python scripts/export_data.py --customer cust-id --dataset pipeline_health
```

### Monitoring

```bash
# Check system health
python scripts/health_check.py --customer cust-id

# Generate diagnostics
python scripts/generate_diagnostics.py --customer cust-id

# Test alerts
python scripts/test_alert.py --channel slack --message "Test alert"
```
