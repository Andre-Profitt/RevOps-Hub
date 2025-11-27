# Data Contracts

This document defines the required schema contracts for all data sources integrated with RevOps Hub.

## Overview

Data contracts ensure consistent data quality across all source systems. Each contract specifies:
- Required fields (must be present and non-null)
- Optional fields (may be null)
- Data types and formats
- Valid value ranges
- Business rules and constraints

## Contract Validation

Contracts are validated automatically in the ingestion transforms:

```python
from transforms.validation.schema_validator import validate_contract

# Validate incoming data against contract
validation_result = validate_contract(df, "opportunity")
if not validation_result.is_valid:
    raise DataContractViolation(validation_result.errors)
```

---

## Core Entity Contracts

### Opportunity Contract

**Dataset:** `/RevOps/Staging/opportunities`

| Field | Type | Required | Constraints | Description |
|-------|------|----------|-------------|-------------|
| `opportunity_id` | STRING | Yes | Unique, non-empty | Primary identifier |
| `opportunity_name` | STRING | Yes | Max 255 chars | Deal name |
| `account_id` | STRING | Yes | FK to accounts | Parent account |
| `account_name` | STRING | No | - | Denormalized account name |
| `owner_id` | STRING | Yes | FK to users | Deal owner |
| `owner_name` | STRING | No | - | Denormalized owner name |
| `amount` | DOUBLE | Yes | >= 0 | Deal value |
| `stage_name` | STRING | Yes | Valid stage enum | Current pipeline stage |
| `probability` | DOUBLE | No | 0.0 - 1.0 | Win probability |
| `close_date` | DATE | Yes | Not in distant past | Expected close date |
| `created_date` | DATE | Yes | - | Record creation date |
| `forecast_category` | STRING | No | Valid category enum | Forecast classification |
| `lead_source` | STRING | No | - | Opportunity source |
| `type` | STRING | No | - | Deal type |
| `description` | STRING | No | Max 10000 chars | Deal description |
| `next_step` | STRING | No | Max 1000 chars | Next action |
| `competitor` | STRING | No | - | Primary competitor |
| `is_closed` | BOOLEAN | Yes | - | Closed status |
| `is_won` | BOOLEAN | Yes | - | Won status |
| `last_activity_date` | DATE | No | - | Most recent activity |
| `last_modified_date` | TIMESTAMP | Yes | - | Last update timestamp |
| `segment` | STRING | No | Enterprise/Mid-Market/SMB/Velocity | Deal segment |
| `region` | STRING | No | - | Geographic region |

**Valid Stage Names:**
```
Prospecting, Discovery, Solution Design, Proposal,
Negotiation, Verbal Commit, Closed Won, Closed Lost
```

**Valid Forecast Categories:**
```
Pipeline, Best Case, Commit, Closed, Omitted
```

**Business Rules:**
- `is_won = true` implies `stage_name = 'Closed Won'`
- `is_closed = true` implies `stage_name IN ('Closed Won', 'Closed Lost')`
- `close_date` should not be more than 2 years in the past for open deals
- `amount` should not exceed $100M (flag for review)

---

### Account Contract

**Dataset:** `/RevOps/Staging/accounts`

| Field | Type | Required | Constraints | Description |
|-------|------|----------|-------------|-------------|
| `account_id` | STRING | Yes | Unique, non-empty | Primary identifier |
| `account_name` | STRING | Yes | Max 255 chars | Company name |
| `industry` | STRING | No | - | Industry classification |
| `segment` | STRING | No | Valid segment enum | Account segment |
| `annual_revenue` | DOUBLE | No | >= 0 | Annual revenue |
| `employee_count` | INTEGER | No | >= 0 | Employee count |
| `billing_country` | STRING | No | ISO 3166-1 alpha-2 | Country code |
| `billing_state` | STRING | No | - | State/Province |
| `owner_id` | STRING | No | FK to users | Account owner |
| `owner_name` | STRING | No | - | Denormalized owner name |
| `type` | STRING | No | - | Account type |
| `website` | STRING | No | Valid URL format | Company website |
| `created_date` | DATE | Yes | - | Record creation date |
| `last_modified_date` | TIMESTAMP | Yes | - | Last update timestamp |

**Valid Segments:**
```
Enterprise, Mid-Market, SMB, Velocity
```

**Segment Derivation Rules:**
```
Enterprise: annual_revenue >= $100M OR employee_count >= 1000
Mid-Market: annual_revenue >= $10M OR employee_count >= 100
SMB: annual_revenue >= $1M OR employee_count >= 10
Velocity: All others
```

---

### Contact Contract

**Dataset:** `/RevOps/Staging/contacts`

| Field | Type | Required | Constraints | Description |
|-------|------|----------|-------------|-------------|
| `contact_id` | STRING | Yes | Unique, non-empty | Primary identifier |
| `first_name` | STRING | No | Max 100 chars | First name |
| `last_name` | STRING | Yes | Max 100 chars | Last name |
| `email` | STRING | No | Valid email format | Email address |
| `phone` | STRING | No | - | Phone number |
| `title` | STRING | No | Max 255 chars | Job title |
| `department` | STRING | No | - | Department |
| `account_id` | STRING | Yes | FK to accounts | Parent account |
| `owner_id` | STRING | No | FK to users | Contact owner |
| `lead_source` | STRING | No | - | Lead source |
| `is_primary` | BOOLEAN | No | - | Primary contact flag |
| `created_date` | DATE | Yes | - | Record creation date |
| `last_modified_date` | TIMESTAMP | Yes | - | Last update timestamp |

**Business Rules:**
- Each account should have at least one contact
- `email` should be unique within an account (warn on duplicates)
- `is_primary = true` should exist for only one contact per account

---

### Activity Contract

**Dataset:** `/RevOps/Staging/activities`

| Field | Type | Required | Constraints | Description |
|-------|------|----------|-------------|-------------|
| `activity_id` | STRING | Yes | Unique, non-empty | Primary identifier |
| `activity_type` | STRING | Yes | Valid type enum | Activity type |
| `subject` | STRING | No | Max 500 chars | Activity subject |
| `description` | STRING | No | Max 10000 chars | Activity details |
| `activity_date` | DATE | Yes | - | Activity date |
| `duration_minutes` | INTEGER | No | >= 0 | Duration in minutes |
| `owner_id` | STRING | Yes | FK to users | Activity owner |
| `owner_name` | STRING | No | - | Denormalized owner name |
| `account_id` | STRING | No | FK to accounts | Related account |
| `opportunity_id` | STRING | No | FK to opportunities | Related opportunity |
| `contact_id` | STRING | No | FK to contacts | Related contact |
| `status` | STRING | No | - | Activity status |
| `priority` | STRING | No | - | Priority level |
| `is_completed` | BOOLEAN | Yes | - | Completion status |
| `created_date` | TIMESTAMP | Yes | - | Record creation |

**Valid Activity Types:**
```
Call, Email, Meeting, Demo, Task, Note
```

**Business Rules:**
- At least one of `account_id`, `opportunity_id`, or `contact_id` should be populated
- `activity_date` should not be in the future for completed activities
- Meetings and Demos should have `duration_minutes` populated

---

## Telemetry Contracts

### Product Usage Contract

**Dataset:** `/RevOps/Telemetry/product_usage`

| Field | Type | Required | Constraints | Description |
|-------|------|----------|-------------|-------------|
| `account_id` | STRING | Yes | FK to accounts | Account identifier |
| `usage_date` | DATE | Yes | - | Usage date |
| `daily_active_users` | INTEGER | Yes | >= 0 | DAU count |
| `total_events` | INTEGER | Yes | >= 0 | Event count |
| `features_used` | INTEGER | No | >= 0 | Unique features |
| `core_feature_events` | INTEGER | No | >= 0 | Core feature usage |
| `premium_feature_events` | INTEGER | No | >= 0 | Premium feature usage |
| `engagement_score` | DOUBLE | No | 0.0 - 10.0 | Calculated engagement |
| `usage_health` | STRING | No | Valid health enum | Health status |
| `last_activity` | TIMESTAMP | No | - | Most recent activity |

**Valid Health Status:**
```
Healthy, Monitor, At Risk, Critical
```

---

### Support Ticket Contract

**Dataset:** `/RevOps/Telemetry/support_tickets`

| Field | Type | Required | Constraints | Description |
|-------|------|----------|-------------|-------------|
| `ticket_id` | STRING | Yes | Unique | Ticket identifier |
| `account_id` | STRING | No | FK to accounts | Account identifier |
| `subject` | STRING | Yes | Max 500 chars | Ticket subject |
| `description` | STRING | No | - | Ticket description |
| `status` | STRING | Yes | - | Ticket status |
| `severity` | STRING | Yes | Valid severity enum | Ticket severity |
| `ticket_type` | STRING | No | - | Ticket category |
| `created_date` | TIMESTAMP | Yes | - | Creation timestamp |
| `updated_date` | TIMESTAMP | No | - | Last update |
| `resolved_date` | TIMESTAMP | No | - | Resolution timestamp |
| `resolution_hours` | DOUBLE | No | >= 0 | Time to resolution |
| `requester_email` | STRING | No | Valid email | Requester email |
| `assignee_name` | STRING | No | - | Assigned agent |
| `satisfaction_rating` | DOUBLE | No | 1.0 - 5.0 | CSAT score |

**Valid Severity Levels:**
```
Critical, High, Medium, Low
```

---

### Marketing Touch Contract

**Dataset:** `/RevOps/Telemetry/marketing_touches`

| Field | Type | Required | Constraints | Description |
|-------|------|----------|-------------|-------------|
| `touch_id` | STRING | Yes | Unique | Touch identifier |
| `contact_id` | STRING | No | FK to contacts | Contact identifier |
| `touch_type` | STRING | Yes | Valid type enum | Touch type |
| `touch_date` | TIMESTAMP | Yes | - | Touch timestamp |
| `campaign_id` | STRING | No | - | Campaign identifier |
| `asset_name` | STRING | No | - | Content/asset name |
| `is_engagement` | BOOLEAN | Yes | - | Engagement flag |
| `touch_score` | INTEGER | No | 0 - 100 | Touch value score |

**Valid Touch Types:**
```
Email Sent, Email Delivered, Email Bounced, Email Unsubscribed,
Email Opened, Email Clicked, Form Filled, Page Visit,
Webinar Registered, Webinar Attended, Content Downloaded
```

---

## User/Rep Contract

**Dataset:** `/RevOps/Reference/users`

| Field | Type | Required | Constraints | Description |
|-------|------|----------|-------------|-------------|
| `user_id` | STRING | Yes | Unique | User identifier |
| `user_name` | STRING | Yes | - | Full name |
| `email` | STRING | Yes | Valid email | Email address |
| `role` | STRING | Yes | - | User role |
| `manager_id` | STRING | No | FK to users | Manager |
| `region` | STRING | No | - | Geographic region |
| `segment` | STRING | No | - | Sales segment |
| `is_active` | BOOLEAN | Yes | - | Active status |
| `hire_date` | DATE | No | - | Start date |

---

## Validation Implementation

### Schema Validator Transform

```python
# transforms/validation/schema_validator.py

from dataclasses import dataclass
from typing import List, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

@dataclass
class ValidationResult:
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    stats: Dict[str, Any]

def validate_contract(df: DataFrame, contract_name: str) -> ValidationResult:
    """Validate DataFrame against named contract."""

    contracts = {
        "opportunity": OPPORTUNITY_CONTRACT,
        "account": ACCOUNT_CONTRACT,
        "contact": CONTACT_CONTRACT,
        "activity": ACTIVITY_CONTRACT,
    }

    contract = contracts.get(contract_name)
    if not contract:
        raise ValueError(f"Unknown contract: {contract_name}")

    errors = []
    warnings = []
    stats = {"total_rows": df.count()}

    # Check required fields
    for field in contract.required_fields:
        if field not in df.columns:
            errors.append(f"Missing required field: {field}")
        else:
            null_count = df.filter(F.col(field).isNull()).count()
            if null_count > 0:
                null_pct = null_count / stats["total_rows"] * 100
                errors.append(f"Required field '{field}' has {null_count} null values ({null_pct:.1f}%)")

    # Check data types
    for field, expected_type in contract.field_types.items():
        if field in df.columns:
            actual_type = df.schema[field].dataType
            if not types_compatible(actual_type, expected_type):
                warnings.append(f"Field '{field}' has type {actual_type}, expected {expected_type}")

    # Check enum constraints
    for field, valid_values in contract.enum_constraints.items():
        if field in df.columns:
            invalid_count = df.filter(
                ~F.col(field).isin(valid_values) & F.col(field).isNotNull()
            ).count()
            if invalid_count > 0:
                warnings.append(f"Field '{field}' has {invalid_count} invalid values")

    # Check uniqueness constraints
    for field in contract.unique_fields:
        if field in df.columns:
            total = df.count()
            distinct = df.select(field).distinct().count()
            if total != distinct:
                errors.append(f"Field '{field}' is not unique: {total - distinct} duplicates")

    return ValidationResult(
        is_valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        stats=stats
    )
```

### CI Integration

Add contract validation to your CI pipeline:

```yaml
# .github/workflows/validate-contracts.yml
name: Validate Data Contracts

on:
  pull_request:
    paths:
      - 'transforms/ingestion/**'

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Run Contract Validation
        run: |
          python scripts/validate_contracts.py --strict

      - name: Check for Breaking Changes
        run: |
          python scripts/check_contract_breaking_changes.py
```

---

## Contract Versioning

Contracts are versioned to track breaking changes:

| Version | Date | Changes |
|---------|------|---------|
| 1.0.0 | 2024-01-01 | Initial contracts |
| 1.1.0 | 2024-03-15 | Added `segment` to opportunities |
| 1.2.0 | 2024-06-01 | Added telemetry contracts |
| 2.0.0 | 2024-09-01 | Breaking: renamed `rep_id` to `owner_id` |

### Breaking Change Policy

- **Major version (X.0.0):** Breaking changes (field renames, type changes, removed fields)
- **Minor version (x.Y.0):** New optional fields, new contracts
- **Patch version (x.y.Z):** Documentation updates, constraint relaxation

Consumers should pin to major version and handle minor changes gracefully.

---

## Monitoring

Contract violations are tracked in `/RevOps/Monitoring/contract_violations`:

| Field | Description |
|-------|-------------|
| `violation_id` | Unique identifier |
| `contract_name` | Which contract was violated |
| `field_name` | Field with violation |
| `violation_type` | null_value, invalid_type, invalid_enum, duplicate |
| `violation_count` | Number of records affected |
| `sample_values` | Example invalid values |
| `detected_at` | When violation was detected |
| `source_system` | Origin system |
| `build_id` | Foundry build identifier |

Query recent violations:
```sql
SELECT *
FROM `/RevOps/Monitoring/contract_violations`
WHERE detected_at >= current_date() - INTERVAL 7 DAYS
ORDER BY violation_count DESC
```
