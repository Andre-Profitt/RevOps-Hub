# Transform Build Order (DAG)

This document describes the data pipeline dependency graph and recommended build order.
Generated from transform `@transform` decorators. Update by running:

```bash
python scripts/generate_manifest.py
```

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              RAW / SCENARIO DATA                            │
│  /RevOps/Scenario/opportunities, accounts, activities, sales_reps, leads    │
│  /RevOps/Raw/quotas, activities                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          TIER 1: REFERENCE DATA                             │
│  reference/stage_benchmarks.py → /RevOps/Reference/stage_benchmarks         │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          TIER 2: ENRICHMENT                                 │
│  enrichment/calculate_deal_health.py → /RevOps/Enriched/deal_health_scores  │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          TIER 3: CORE ANALYTICS                             │
│  analytics/rep_performance_analytics.py                                     │
│  analytics/pipeline_health_summary.py                                       │
│  analytics/pipeline_hygiene_analytics.py                                    │
│  analytics/process_bottleneck_analytics.py                                  │
│  analytics/customer_health_expansion.py                                     │
│  analytics/win_loss_analysis.py                                             │
│  analytics/territory_account_planning.py                                    │
│  analytics/scenario_modeling.py                                             │
│  analytics/capacity_hiring_planning.py                                      │
│  analytics/forecast_confidence.py                                           │
│  analytics/forecasting_hub.py                                               │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          TIER 4: DERIVED ANALYTICS                          │
│  analytics/leading_indicator_analytics.py                                   │
│  analytics/cross_functional_telemetry.py                                    │
│  analytics/qbr_pack_builder.py                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          TIER 5: DASHBOARD ROLLUPS                          │
│  dashboard/compute_dashboard_kpis.py → /RevOps/Dashboard/kpis               │
│  analytics/next_best_actions.py → /RevOps/Dashboard/next_best_actions       │
│  decisions/sales_interventions_schema.py → /RevOps/Decisions/...            │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       TIER 5b: CLOSED-LOOP ANALYTICS                        │
│  analytics/intervention_effectiveness.py                                    │
│  analytics/action_impact_tracker.py → /RevOps/Inbox/impact_tracker          │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Build Tiers

### Tier 0: Base Data Generation (Scenario/Sample)

These create synthetic data for development/demo. In production, skip these
and use actual CRM exports.

| Transform | Output Dataset |
|-----------|----------------|
| `scenario/generate_scenario_accounts.py` | `/RevOps/Scenario/accounts` |
| `scenario/generate_scenario_opportunities.py` | `/RevOps/Scenario/opportunities` |
| `scenario/generate_scenario_reps.py` | `/RevOps/Scenario/sales_reps` |
| `scenario/generate_scenario_activities.py` | `/RevOps/Scenario/activities` |
| `scenario/generate_process_events.py` | `/RevOps/Scenario/process_events` |
| `sample_data/generate_*.py` | `/RevOps/Sample/*` |

### Tier 1: Reference Data

Static reference tables with benchmarks and configuration.

| Transform | Output Dataset |
|-----------|----------------|
| `reference/stage_benchmarks.py` | `/RevOps/Reference/stage_benchmarks` |

### Tier 2: Enrichment

Adds calculated fields to raw data.

| Transform | Inputs | Output Dataset |
|-----------|--------|----------------|
| `enrichment/calculate_deal_health.py` | Scenario/opportunities, Reference/stage_benchmarks | `/RevOps/Enriched/deal_health_scores` |

### Tier 3: Core Analytics

Independent analytics that depend only on enriched data.

| Transform | Key Outputs |
|-----------|-------------|
| `analytics/rep_performance_analytics.py` | `/RevOps/Analytics/rep_performance` |
| `analytics/pipeline_health_summary.py` | `/RevOps/Analytics/pipeline_health_summary` |
| `analytics/pipeline_hygiene_analytics.py` | `/RevOps/Analytics/pipeline_hygiene_*` |
| `analytics/process_bottleneck_analytics.py` | `/RevOps/Analytics/process_bottlenecks` |
| `analytics/stage_velocity_analytics.py` | `/RevOps/Analytics/stage_velocity`, `/RevOps/Analytics/pipeline_funnel` |
| `analytics/customer_health_expansion.py` | `/RevOps/Analytics/customer_health_*`, `/RevOps/Analytics/churn_risk_*`, `/RevOps/Analytics/expansion_*` |
| `analytics/win_loss_analysis.py` | `/RevOps/Analytics/win_loss_*`, `/RevOps/Analytics/loss_reasons`, `/RevOps/Analytics/win_factors` |
| `analytics/territory_account_planning.py` | `/RevOps/Analytics/territory_*`, `/RevOps/Analytics/account_scores`, `/RevOps/Analytics/white_space_*` |
| `analytics/scenario_modeling.py` | `/RevOps/Analytics/scenario_*` |
| `analytics/capacity_hiring_planning.py` | `/RevOps/Analytics/*_capacity_*`, `/RevOps/Analytics/ramp_*`, `/RevOps/Analytics/hiring_*` |
| `analytics/forecast_confidence.py` | `/RevOps/Analytics/forecast_confidence` |
| `analytics/forecasting_hub.py` | `/RevOps/Analytics/forecast_*` |
| `analytics/coaching_analytics.py` | `/RevOps/Coaching/*` (6 datasets) |
| `analytics/compensation_analytics.py` | `/RevOps/Compensation/*` (3 datasets) |
| `analytics/dealdesk_analytics.py` | `/RevOps/DealDesk/*` (3 datasets) |
| `analytics/data_quality_analytics.py` | `/RevOps/DataQuality/*` (3 datasets) |
| `analytics/alerts_analytics.py` | `/RevOps/Alerts/*` (3 datasets) |

### Tier 4: Derived Analytics

Analytics that depend on Tier 3 outputs.

| Transform | Dependencies | Key Outputs |
|-----------|--------------|-------------|
| `analytics/leading_indicator_analytics.py` | rep_performance, deal_health | `/RevOps/Analytics/deal_predictions`, `/RevOps/Analytics/leading_indicators_summary` |
| `analytics/cross_functional_telemetry.py` | hygiene_alerts, deal_health | `/RevOps/Analytics/telemetry_summary`, `/RevOps/Analytics/funnel_handoff_metrics` |
| `analytics/qbr_pack_builder.py` | forecast_history, hygiene_summary | `/RevOps/Analytics/qbr_*` |

### Tier 5: Dashboard Rollups & Actions

Final aggregations consumed directly by the webapp and action recommendation engine.

| Transform | Dependencies | Key Outputs |
|-----------|--------------|-------------|
| `dashboard/compute_dashboard_kpis.py` | forecast_confidence, opportunities | `/RevOps/Dashboard/kpis` |
| `analytics/next_best_actions.py` | deal_health, sales_reps | `/RevOps/Dashboard/next_best_actions`, `/RevOps/Coaching/next_best_actions` |
| `decisions/sales_interventions_schema.py` | (none - schema only) | `/RevOps/Decisions/sales_interventions` |

### Tier 5b: Closed-Loop Analytics

Measures effectiveness of actions and interventions. Requires decisions to be logged.

| Transform | Dependencies | Key Outputs |
|-----------|--------------|-------------|
| `analytics/intervention_effectiveness.py` | sales_interventions, next_best_actions | `/RevOps/Analytics/intervention_effectiveness` |
| `analytics/action_impact_tracker.py` | next_best_actions, sales_interventions | `/RevOps/Inbox/impact_tracker` |

### Tier 6: Monitoring & Operations

Operational visibility and alerting transforms.

| Transform | Dependencies | Key Outputs |
|-----------|--------------|-------------|
| `monitoring/alert_evaluator.py` | build_metrics, pipeline_health, data_quality, sync_status | `/RevOps/Monitoring/alert_history`, `/RevOps/Monitoring/active_alerts`, `/RevOps/Monitoring/alert_summary` |
| `monitoring/ops_dashboard.py` | build_metrics, sync_status, data_quality, active_alerts | `/RevOps/Ops/system_health`, `/RevOps/Ops/build_performance`, `/RevOps/Ops/data_freshness`, `/RevOps/Ops/usage_metrics`, `/RevOps/Ops/sla_tracking` |

### Ingestion Transforms

Transforms for syncing data from source systems.

| Transform | Source System | Key Outputs |
|-----------|---------------|-------------|
| `ingestion/salesforce_sync.py` | Salesforce | `/RevOps/Staging/opportunities`, `/RevOps/Staging/accounts`, `/RevOps/Staging/contacts`, `/RevOps/Staging/activities` |
| `ingestion/hubspot_sync.py` | HubSpot | `/RevOps/Staging/opportunities`, `/RevOps/Staging/accounts`, `/RevOps/Staging/contacts`, `/RevOps/Staging/activities`, `/RevOps/Staging/marketing_activities` |
| `ingestion/telemetry_feeds.py` | Various | `/RevOps/Telemetry/product_usage`, `/RevOps/Telemetry/support_tickets`, `/RevOps/Telemetry/marketing_touches`, `/RevOps/Telemetry/unified_timeline` |

### Validation Transforms

Schema validation and data contract enforcement.

| Transform | Purpose |
|-----------|---------|
| `validation/schema_validator.py` | Contract enforcement for all entities |

### ML Transforms

AI/ML models for predictions and recommendations.

| Transform | Dependencies | Key Outputs |
|-----------|--------------|-------------|
| `ml/deal_scoring.py` | opportunities, activities, win_loss | `/RevOps/ML/deal_features`, `/RevOps/ML/deal_scores`, `/RevOps/ML/model_weights`, `/RevOps/ML/model_performance` |
| `ml/churn_prediction.py` | accounts, product_usage, support_tickets | `/RevOps/ML/churn_features`, `/RevOps/ML/churn_scores`, `/RevOps/ML/churn_cohorts` |
| `ml/next_best_action.py` | deal_scores, churn_scores, hygiene_alerts | `/RevOps/ML/deal_recommendations`, `/RevOps/ML/rep_recommendations`, `/RevOps/ML/account_recommendations` |

### Commercial Transforms

Usage metering and licensing.

| Transform | Dependencies | Key Outputs |
|-----------|--------------|-------------|
| `commercial/usage_metering.py` | opportunities, activities, api_logs, build_metrics | `/RevOps/Commercial/usage_daily`, `/RevOps/Commercial/usage_summary`, `/RevOps/Commercial/license_status`, `/RevOps/Commercial/billing_events` |

## Scheduling Recommendations

| Tier | Frequency | Notes |
|------|-----------|-------|
| 0 | Once (dev only) | Skip in production |
| 1 | Daily | Reference data rarely changes |
| 2-3 | Hourly or on CRM sync | Core analytics refresh |
| 4-5 | Hourly | Dashboard data |
| 6 | Every 15 min | Monitoring and alerting |
| Ingestion | Hourly (Starter), 15 min (Growth+) | CRM sync frequency varies by tier |

## Running a Full Build

```bash
# In Foundry Code Repositories, configure a schedule or run manually:

# Tier 0 (dev only)
foundry-cli build /RevOps/Scenario/*

# Tier 1
foundry-cli build /RevOps/Reference/stage_benchmarks

# Tier 2
foundry-cli build /RevOps/Enriched/deal_health_scores

# Tier 3 (can run in parallel)
foundry-cli build /RevOps/Analytics/rep_performance
foundry-cli build /RevOps/Analytics/pipeline_health_summary
# ... etc

# Tier 4-5 (after Tier 3 completes)
foundry-cli build /RevOps/Dashboard/kpis
```

## Monitoring

See `/RevOps/Monitoring/build_metrics` for:
- Last build timestamp per dataset
- Record counts
- Build duration
- Error rates

Query stale datasets:
```sql
SELECT dataset_path, last_build_ts, record_count
FROM `/RevOps/Monitoring/build_metrics`
WHERE last_build_ts < current_timestamp() - INTERVAL 2 HOURS
ORDER BY last_build_ts
```
