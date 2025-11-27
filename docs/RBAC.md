# Role-Based Access Control (RBAC)

This document defines the access control model for RevOps Hub, including roles, permissions, and data access policies.

## Overview

RevOps Hub implements a layered access control model:

1. **Platform Roles** - Global permissions within the RevOps Hub application
2. **Data Permissions** - Foundry dataset-level access
3. **Row-Level Security** - Filter data based on user attributes
4. **Feature Entitlements** - Tier-based feature availability

---

## Platform Roles

### Role Hierarchy

```
Platform Admin
    │
    ├── Customer Admin
    │       │
    │       ├── RevOps Manager
    │       │       │
    │       │       └── RevOps Analyst
    │       │
    │       └── Sales Manager
    │               │
    │               └── Sales Rep
    │
    └── Support Engineer (read-only cross-customer)
```

### Role Definitions

| Role | Description | Scope |
|------|-------------|-------|
| `platform_admin` | Full platform access, all customers | Global |
| `customer_admin` | Full access within their organization | Customer |
| `revops_manager` | Configure and analyze RevOps data | Customer |
| `revops_analyst` | View and analyze RevOps data | Customer |
| `sales_manager` | View team performance and pipeline | Team |
| `sales_rep` | View own performance and deals | Self |
| `support_engineer` | Read-only diagnostic access | Cross-customer |

### Role Permissions Matrix

| Permission | Platform Admin | Customer Admin | RevOps Manager | RevOps Analyst | Sales Manager | Sales Rep |
|------------|:--------------:|:--------------:|:--------------:|:--------------:|:-------------:|:---------:|
| View all customers | ✓ | | | | | |
| Manage customer config | ✓ | ✓ | | | | |
| Manage users | ✓ | ✓ | | | | |
| Configure thresholds | ✓ | ✓ | ✓ | | | |
| Configure alerts | ✓ | ✓ | ✓ | | | |
| View all analytics | ✓ | ✓ | ✓ | ✓ | | |
| View team analytics | ✓ | ✓ | ✓ | ✓ | ✓ | |
| View own analytics | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Export data | ✓ | ✓ | ✓ | | | |
| Trigger builds | ✓ | ✓ | ✓ | | | |
| View build logs | ✓ | ✓ | ✓ | | | |
| Access API | ✓ | ✓ | ✓ | | | |

---

## Data Permissions

### Permission Groups

Each customer workspace has the following Foundry permission groups:

```
{customer_id}-revops-admins     # Full access
{customer_id}-revops-analysts   # Read access to analytics
{customer_id}-revops-managers   # Team-level read access
{customer_id}-revops-reps       # Limited read with RLS
```

### Dataset Access Matrix

| Dataset Category | Admins | Analysts | Managers | Reps |
|-----------------|:------:|:--------:|:--------:|:----:|
| `/RevOps/Raw/*` | RW | | | |
| `/RevOps/Staging/*` | RW | R | | |
| `/RevOps/Enriched/*` | RW | R | R | |
| `/RevOps/Analytics/*` | RW | R | R | |
| `/RevOps/Dashboard/*` | RW | R | R | R* |
| `/RevOps/Coaching/*` | RW | R | R* | |
| `/RevOps/Config/*` | RW | | | |
| `/RevOps/Monitoring/*` | RW | R | | |
| `/RevOps/Telemetry/*` | RW | R | | |

**R** = Read, **RW** = Read/Write, **R*** = Read with row-level security

---

## Row-Level Security (RLS)

Row-level security filters data based on the user's identity and role.

### RLS Policies

#### Sales Rep Policy
Sales reps can only see their own data:

```python
def apply_rep_rls(df, user_id):
    """Filter data to user's owned records."""
    return df.filter(F.col("owner_id") == user_id)
```

**Applies to:**
- `pipeline_health_summary`
- `deal_predictions`
- `coaching_insights`
- `rep_performance`
- Dashboard KPIs

#### Sales Manager Policy
Sales managers see their direct reports' data:

```python
def apply_manager_rls(df, user_id, user_hierarchy):
    """Filter data to manager's team."""
    team_ids = user_hierarchy.get(user_id, [user_id])
    return df.filter(F.col("owner_id").isin(team_ids))
```

**Applies to:**
- `rep_performance`
- `coaching_insights`
- `activity_metrics`

### RLS Implementation

Row-level security is implemented at the application layer:

```python
# /webapp/utils/rls.py

from typing import List, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class RLSFilter:
    """Apply row-level security filters based on user role."""

    def __init__(self, user_id: str, role: str, team_members: List[str] = None):
        self.user_id = user_id
        self.role = role
        self.team_members = team_members or []

    def apply(self, df: DataFrame, owner_column: str = "owner_id") -> DataFrame:
        """Apply RLS filter to DataFrame."""
        if self.role in ("platform_admin", "customer_admin", "revops_manager", "revops_analyst"):
            # Full access - no filter
            return df

        if self.role == "sales_manager":
            # Team access
            allowed_ids = [self.user_id] + self.team_members
            return df.filter(F.col(owner_column).isin(allowed_ids))

        if self.role == "sales_rep":
            # Self only
            return df.filter(F.col(owner_column) == self.user_id)

        # Default: no access
        return df.filter(F.lit(False))
```

---

## Feature Entitlements

Features are gated by subscription tier. See `configs/schema.py` for the complete tier matrix.

### Tier Summary

| Feature | Starter | Growth | Enterprise |
|---------|:-------:|:------:|:----------:|
| Pipeline Health | ✓ | ✓ | ✓ |
| Pipeline Hygiene | ✓ | ✓ | ✓ |
| Forecast Summary | ✓ | ✓ | ✓ |
| Deal Predictions | | ✓ | ✓ |
| Coaching Insights | | ✓ | ✓ |
| Rep Performance | | ✓ | ✓ |
| Activity Analytics | | ✓ | ✓ |
| Scenario Modeling | | | ✓ |
| Territory Planning | | | ✓ |
| Custom Integrations | | | ✓ |
| API Access | | | ✓ |
| SSO | | | ✓ |
| Data Export | | | ✓ |

### Feature Gating

```python
# /webapp/utils/entitlements.py

from configs import load_customer_config, FeatureFlags

def check_feature(customer_id: str, feature: str) -> bool:
    """Check if customer has access to a feature."""
    config = load_customer_config(customer_id)
    return getattr(config.features, feature, False)

def require_feature(feature: str):
    """Decorator to require a feature for an endpoint."""
    def decorator(func):
        @wraps(func)
        def wrapper(request, *args, **kwargs):
            customer_id = get_customer_id(request)
            if not check_feature(customer_id, feature):
                raise FeatureNotAvailable(feature)
            return func(request, *args, **kwargs)
        return wrapper
    return decorator

# Usage
@require_feature("deal_predictions")
def get_predictions(request):
    ...
```

---

## Authentication

### SSO Integration (Enterprise)

Enterprise customers can configure SAML 2.0 or OIDC for single sign-on:

```json
{
  "customer_id": "acme-corp",
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

### API Authentication

API access uses OAuth 2.0 client credentials:

```bash
# Get access token
curl -X POST https://api.revops-hub.io/oauth/token \
  -d "grant_type=client_credentials" \
  -d "client_id=YOUR_CLIENT_ID" \
  -d "client_secret=YOUR_CLIENT_SECRET"

# Use token
curl https://api.revops-hub.io/v1/pipeline/health \
  -H "Authorization: Bearer ACCESS_TOKEN"
```

---

## Audit Logging

All access is logged for compliance:

```python
@dataclass
class AuditEvent:
    event_id: str
    timestamp: datetime
    customer_id: str
    user_id: str
    user_role: str
    action: str  # read, write, export, configure
    resource: str  # dataset path or feature name
    details: Dict[str, Any]
    ip_address: str
    user_agent: str
```

### Audit Log Dataset

Audit events are stored in `/RevOps/Audit/access_log`:

| Field | Type | Description |
|-------|------|-------------|
| `event_id` | STRING | Unique event identifier |
| `timestamp` | TIMESTAMP | Event time |
| `customer_id` | STRING | Customer identifier |
| `user_id` | STRING | User who performed action |
| `user_role` | STRING | User's role at time of action |
| `action` | STRING | Action performed |
| `resource` | STRING | Resource accessed |
| `row_count` | INTEGER | Rows accessed (for reads) |
| `success` | BOOLEAN | Whether action succeeded |
| `error_message` | STRING | Error details if failed |
| `ip_address` | STRING | Client IP |
| `user_agent` | STRING | Client user agent |

### Compliance Queries

```sql
-- All access by a specific user
SELECT *
FROM `/RevOps/Audit/access_log`
WHERE user_id = 'user-123'
  AND timestamp >= current_date() - INTERVAL 30 DAYS
ORDER BY timestamp DESC;

-- Failed access attempts
SELECT *
FROM `/RevOps/Audit/access_log`
WHERE success = false
  AND timestamp >= current_date() - INTERVAL 7 DAYS;

-- Data exports
SELECT *
FROM `/RevOps/Audit/access_log`
WHERE action = 'export'
  AND timestamp >= current_date() - INTERVAL 90 DAYS;
```

---

## Provisioning Workflow

When a new customer is onboarded:

1. **Create Permission Groups**
   ```bash
   python scripts/provision_workspace.py \
     --customer "Acme Corp" \
     --tier enterprise
   ```

2. **Assign Initial Admin**
   - Customer admin is assigned to `{customer_id}-revops-admins`
   - Admin receives welcome email with setup instructions

3. **Configure SSO** (Enterprise only)
   - Admin provides IdP metadata
   - RevOps Hub configures SAML/OIDC integration
   - Test authentication flow

4. **Import Users**
   - Users synced from CRM or uploaded via CSV
   - Role assignments based on CRM profile/role

5. **Configure Hierarchy**
   - Manager-report relationships imported
   - Used for RLS and coaching analytics

---

## Security Best Practices

### For Administrators

1. **Principle of Least Privilege**
   - Assign minimum required role
   - Review access quarterly

2. **Separate Admin Accounts**
   - Don't use admin accounts for daily work
   - Enable MFA for admin accounts

3. **Monitor Audit Logs**
   - Set up alerts for sensitive actions
   - Review failed access attempts

### For Developers

1. **Always Apply RLS**
   - Never bypass RLS for production queries
   - Test with different user roles

2. **Validate Permissions**
   - Check feature entitlements before rendering UI
   - Return appropriate errors for unauthorized access

3. **Secure API Keys**
   - Never commit credentials to source control
   - Rotate keys regularly

---

## Appendix: Permission Group Templates

### Foundry Markings

```yaml
# revops-admins marking
classification: INTERNAL
organization: {customer_id}
access_level: ADMIN
datasets:
  - /RevOps/**

# revops-analysts marking
classification: INTERNAL
organization: {customer_id}
access_level: READ
datasets:
  - /RevOps/Analytics/**
  - /RevOps/Dashboard/**
  - /RevOps/Coaching/**

# revops-managers marking
classification: INTERNAL
organization: {customer_id}
access_level: READ
datasets:
  - /RevOps/Analytics/pipeline_health_summary
  - /RevOps/Dashboard/**
  - /RevOps/Coaching/rep_performance
rls_policy: team_filter

# revops-reps marking
classification: INTERNAL
organization: {customer_id}
access_level: READ
datasets:
  - /RevOps/Dashboard/kpis
rls_policy: self_filter
```
