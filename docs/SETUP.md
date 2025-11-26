# RevOps Command Center - Foundry Setup Guide

## Overview

This guide walks you through setting up the RevOps Command Center in Palantir Foundry.

## Prerequisites

- Access to a Palantir Foundry environment
- Permission to create Code Repositories
- Permission to create Datasets
- Permission to configure Ontology (for Phase 2)

---

## Phase 0: Sample Data & Core Transforms

### Step 1: Create Project Structure

1. In Foundry, navigate to your project folder
2. Create the following folder structure:

```
/RevOps/
├── Sample/           # Sample data (generated)
├── Reference/        # Reference/config data
├── Enriched/         # Enriched datasets
├── Analytics/        # Aggregated analytics
└── Code/             # Code repository
```

### Step 2: Create Code Repository

1. Go to **Code Repositories** in Foundry
2. Click **+ New Repository**
3. Name it: `revops-command-center`
4. Select **Python (Transforms)**
5. Click **Create**

### Step 3: Add Transform Files

Copy each Python file from the `transforms/` folder into your Code Repository:

#### Sample Data Generators (run these FIRST, in order)

| File | Purpose | Output Dataset |
|------|---------|----------------|
| `sample_data/generate_opportunities.py` | Creates 500 sample opportunities | `/RevOps/Sample/opportunities` |
| `sample_data/generate_accounts.py` | Creates 40 sample accounts | `/RevOps/Sample/accounts` |
| `sample_data/generate_activities.py` | Creates activities for each opp | `/RevOps/Sample/activities` |
| `sample_data/generate_sales_reps.py` | Creates 10 sales reps | `/RevOps/Sample/sales_reps` |

#### Reference Data (run SECOND)

| File | Purpose | Output Dataset |
|------|---------|----------------|
| `reference/create_stage_benchmarks.py` | Stage duration benchmarks | `/RevOps/Reference/stage_benchmarks` |

#### Enrichment Transforms (run THIRD)

| File | Purpose | Output Dataset |
|------|---------|----------------|
| `enrichment/deal_health_score.py` | Calculates health scores | `/RevOps/Enriched/opportunity_health_scored` |

#### Analytics Transforms (run FOURTH)

| File | Purpose | Output Dataset |
|------|---------|----------------|
| `analytics/pipeline_summary.py` | Pipeline by rep | `/RevOps/Analytics/pipeline_summary` |
| `analytics/forecast_rollup.py` | Forecast aggregations | `/RevOps/Analytics/company_forecast`, `region_forecast`, `manager_forecast` |

### Step 4: Build the Transforms

1. In Code Repository, click **Checks** in the left sidebar
2. Create a new check or use the default
3. Click **Build** (play button)
4. Build order matters! Build in this order:
   1. `generate_opportunities` (creates `/RevOps/Sample/opportunities`)
   2. `generate_accounts` (creates `/RevOps/Sample/accounts`)
   3. `generate_sales_reps` (creates `/RevOps/Sample/sales_reps`)
   4. `create_stage_benchmarks` (creates `/RevOps/Reference/stage_benchmarks`)
   5. `generate_activities` (needs opportunities first)
   6. `deal_health_score` (needs opportunities + benchmarks)
   7. `pipeline_summary` (needs enriched opportunities + reps)
   8. `forecast_rollup` (needs enriched opportunities + reps)

### Step 5: Verify Data

After building, verify each dataset:

```sql
-- Check opportunities
SELECT COUNT(*),
       AVG(amount),
       COUNT(DISTINCT stage_name)
FROM `/RevOps/Sample/opportunities`

-- Should return: ~500 records, reasonable avg amount, 8 stages

-- Check health scores
SELECT health_category,
       COUNT(*) as count,
       AVG(health_score) as avg_score
FROM `/RevOps/Enriched/opportunity_health_scored`
WHERE is_closed = false
GROUP BY health_category

-- Should return: Healthy, Monitor, At Risk, Critical categories
```

---

## Phase 1: Basic Ontology Setup

### Step 1: Create Object Types

In Ontology Manager, create these object types backed by your datasets:

#### Opportunity Object Type

| Property | Type | Source Column |
|----------|------|---------------|
| opportunityId (PK) | String | opportunity_id |
| opportunityName | String | opportunity_name |
| accountId | String | account_id |
| accountName | String | account_name |
| amount | Double | amount |
| stageName | String | stage_name |
| probability | Double | probability |
| closeDate | Date | close_date |
| ownerName | String | owner_name |
| region | String | region |
| forecastCategory | String | forecast_category |
| healthScore | Integer | health_score |
| healthCategory | String | health_category |
| daysInStage | Integer | days_in_current_stage |

**Backing Dataset:** `/RevOps/Enriched/opportunity_health_scored`

#### Account Object Type

| Property | Type | Source Column |
|----------|------|---------------|
| accountId (PK) | String | account_id |
| accountName | String | account_name |
| segment | String | segment |
| industry | String | industry |
| isCustomer | Boolean | is_customer |
| currentArr | Double | current_arr |

**Backing Dataset:** `/RevOps/Sample/accounts`

#### SalesRep Object Type

| Property | Type | Source Column |
|----------|------|---------------|
| repId (PK) | String | rep_id |
| repName | String | rep_name |
| region | String | region |
| quota | Double | quarterly_quota |
| attainment | Double | ytd_attainment |

**Backing Dataset:** `/RevOps/Sample/sales_reps`

### Step 2: Create Link Types

| Link | From | To | Cardinality |
|------|------|-----|-------------|
| opportunityToAccount | Opportunity | Account | Many-to-One |
| opportunityToRep | Opportunity | SalesRep | Many-to-One |
| accountToRep | Account | SalesRep | Many-to-One |

---

## Phase 2: Workshop Dashboard

### Basic Pipeline Dashboard

Create a Workshop application with these widgets:

1. **Header Metrics** (Object Set Aggregate)
   - Total Pipeline: `SUM(amount)` where `is_closed = false`
   - Avg Health: `AVG(healthScore)` where `is_closed = false`
   - At Risk Amount: `SUM(amount)` where `healthCategory IN ('At Risk', 'Critical')`

2. **Pipeline by Health** (Bar Chart)
   - Group by: `healthCategory`
   - Value: `SUM(amount)`
   - Colors: Healthy=Green, Monitor=Yellow, At Risk=Orange, Critical=Red

3. **Opportunities Table** (Object Table)
   - Columns: Account, Amount, Stage, Health Score, Days in Stage, Owner
   - Sort by: Health Score (ascending)
   - Filter: Open opportunities only

4. **Pipeline by Stage** (Funnel Chart)
   - Stages in order
   - Value: Count and Amount

---

## Troubleshooting

### Common Issues

**"Dataset not found" error**
- Ensure parent folders exist in Foundry
- Check dataset path matches exactly (case-sensitive)

**"Column not found" error**
- Verify input dataset has been built
- Check column names match between transforms

**Build fails with "Input not ready"**
- Build dependencies first
- Check build order in Step 4

**Health scores all the same**
- Verify `stage_benchmarks` dataset exists and has data
- Check the join condition in `deal_health_score.py`

---

## Next Steps

After Phase 0-1 are working:

1. **Phase 2:** Add more analytics transforms (win/loss, forecast accuracy)
2. **Phase 3:** Configure AIP agents for insights
3. **Phase 4:** Build executive dashboards
4. **Phase 5:** Connect real Salesforce data

---

## File Inventory

```
revops-command-center/
├── transforms/
│   ├── sample_data/
│   │   ├── generate_opportunities.py    ✅
│   │   ├── generate_accounts.py         ✅
│   │   ├── generate_activities.py       ✅
│   │   └── generate_sales_reps.py       ✅
│   ├── reference/
│   │   └── create_stage_benchmarks.py   ✅
│   ├── enrichment/
│   │   └── deal_health_score.py         ✅
│   └── analytics/
│       ├── pipeline_summary.py          ✅
│       └── forecast_rollup.py           ✅
├── ontology/
│   └── ontology_config.yaml             ✅
└── docs/
    └── SETUP.md                         ✅ (this file)
```
