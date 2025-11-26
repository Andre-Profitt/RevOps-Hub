# Dashboard Data Contracts

This document defines the exact data structures required by each dashboard widget.

---

## Pipeline Health Dashboard

### Widget: KPI Cards

**Dataset**: `/RevOps/Analytics/dashboard_kpis`

```typescript
interface DashboardKPIs {
  // Identifiers
  snapshot_date: Date;
  quarter: string;           // "Q4 2024"
  team_filter: string;       // "All" | manager_name

  // Quota & Forecast
  quarter_target: number;    // 8500000
  ai_forecast: number;       // 7200000
  ai_forecast_confidence: number; // 0.85
  commit_amount: number;     // 6800000
  commit_confidence: number; // 0.85
  closed_won_amount: number; // 2800000

  // Gap Analysis
  gap_to_target: number;     // -1300000
  gap_pct: number;           // 0.15
  coverage_ratio: number;    // 2.0
  risk_level: "Low" | "Medium" | "High" | "Critical";

  // Trends (vs last week)
  forecast_change_1w: number;
  commit_change_1w: number;
}
```

**Transform**: `compute_dashboard_kpis.py`

```python
@transform(
    forecast=Input("/RevOps/Analytics/forecast_confidence"),
    history=Input("/RevOps/Analytics/forecast_history"),
    output=Output("/RevOps/Analytics/dashboard_kpis")
)
def compute(ctx, forecast, history, output):
    # Aggregate current forecast
    # Calculate week-over-week changes
    # Determine risk level
    pass
```

---

### Widget: Forecast Trend Chart

**Dataset**: `/RevOps/Analytics/forecast_history`

```typescript
interface ForecastHistory {
  snapshot_date: Date;
  week_number: number;       // 1-13
  quarter: string;

  // Cumulative values at snapshot
  cumulative_target: number;
  cumulative_closed: number;
  cumulative_commit: number;
  cumulative_forecast: number;

  // Point-in-time pipeline
  pipeline_commit: number;
  pipeline_best_case: number;
  pipeline_total: number;

  // Health at snapshot
  avg_pipeline_health: number;
}
```

**Transform**: `generate_forecast_history.py`

```python
@transform(
    opportunities=Input("/RevOps/Scenario/opportunities"),
    output=Output("/RevOps/Analytics/forecast_history")
)
def compute(ctx, opportunities, output):
    """
    Generate weekly snapshots of forecast state.
    For demo: Create synthetic history based on close dates.
    For prod: Would use actual historical snapshots.
    """
    # Generate 13 weeks of history
    # Show progression toward target
    pass
```

---

### Widget: Stage Velocity Table

**Dataset**: `/RevOps/Analytics/stage_velocity`

```typescript
interface StageVelocity {
  stage_name: string;
  stage_order: number;

  // Duration metrics
  avg_duration_days: number;
  median_duration_days: number;
  benchmark_days: number;
  deviation_days: number;

  // Trend (vs last period)
  duration_change_pct: number;
  trend_direction: "improving" | "stable" | "worsening";

  // Conversion
  conversion_rate: number;
  deals_in_stage: number;
  amount_in_stage: number;

  // Display helpers
  status_indicator: "green" | "yellow" | "red";
  trend_display: string;  // "▲ +2d" or "✓"
}
```

---

### Widget: Stuck Deals Table

**Dataset**: `/RevOps/Analytics/stuck_deals_view`

```typescript
interface StuckDealView {
  // Identity
  opportunity_id: string;
  account_name: string;
  opportunity_name: string;
  owner_name: string;

  // Deal info
  amount: number;
  stage_name: string;
  days_in_stage: number;
  health_score: number;
  close_date: Date;

  // Risk analysis
  urgency: "CRITICAL" | "HIGH" | "MEDIUM" | "LOW";
  risk_flags: string[];      // ["STUCK", "NO_EB", "GOING_DARK"]
  risk_flags_display: string; // "Stuck • No EB • Going Dark"
  days_until_close: number;
  slip_risk: "HIGH" | "MEDIUM" | "LOW";

  // Actions
  primary_action: "call" | "email" | "meeting" | "escalate";
  action_target: string;     // Contact name
  action_reason: string;     // "Follow up on proposal"

  // Display helpers
  urgency_icon: string;      // "🔴"
  amount_display: string;    // "$2.1M"
}
```

**Transform**: `prepare_stuck_deals_view.py`

```python
@transform(
    stuck_deals=Input("/RevOps/Analytics/deals_stuck_in_process"),
    health_scores=Input("/RevOps/Enriched/deal_health_scores"),
    output=Output("/RevOps/Analytics/stuck_deals_view")
)
def compute(ctx, stuck_deals, health_scores, output):
    """
    Prepare display-ready stuck deals data.
    Includes formatted values and suggested actions.
    """
    pass
```

---

### Widget: Health Distribution

**Dataset**: `/RevOps/Analytics/health_distribution`

```typescript
interface HealthDistribution {
  quarter: string;
  team_filter: string;
  snapshot_date: Date;

  // Distribution counts
  healthy_count: number;      // 80-100
  healthy_amount: number;
  healthy_pct: number;

  monitor_count: number;      // 60-79
  monitor_amount: number;
  monitor_pct: number;

  at_risk_count: number;      // 40-59
  at_risk_amount: number;
  at_risk_pct: number;

  critical_count: number;     // 0-39
  critical_amount: number;
  critical_pct: number;

  // Trend
  at_risk_change_1w: number;  // +3% more at-risk vs last week
  trend_direction: "improving" | "stable" | "worsening";
}
```

---

## Rep Coaching Dashboard

### Widget: Performance Summary

**Dataset**: `/RevOps/Analytics/rep_performance_view`

```typescript
interface RepPerformanceView {
  // Identity
  rep_id: string;
  rep_name: string;
  manager_name: string;
  region: string;
  segment: string;

  // Core metrics
  ytd_attainment: number;
  ytd_attainment_display: string;  // "78%"
  win_rate: number;
  avg_deal_size: number;
  avg_sales_cycle_days: number;

  // Rankings
  overall_rank: number;
  total_reps: number;
  rank_display: string;  // "#4/12"
  rank_change_qoq: number;

  // Team comparisons
  team_avg_win_rate: number;
  team_avg_deal_size: number;
  team_avg_cycle: number;

  // Deltas
  win_rate_vs_team: number;
  win_rate_vs_team_display: string;  // "+4%"
  cycle_vs_team: number;

  // Trends (vs prior quarter)
  attainment_change_qoq: number;
  win_rate_change_qoq: number;

  // Performance tier
  performance_tier: "Top Performer" | "On Track" | "Needs Improvement" | "At Risk";
}
```

---

### Widget: Win Rate Drivers

**Dataset**: `/RevOps/Analytics/rep_driver_comparison`

```typescript
interface RepDriverComparison {
  rep_id: string;
  metric_name: string;
  metric_label: string;

  // Values
  rep_value: number;
  top_performer_value: number;
  team_avg_value: number;

  // Formatting
  format_type: "decimal" | "percent" | "days";
  good_direction: "higher" | "lower";

  // Gap analysis
  gap_to_top: number;
  gap_pct: number;
  gap_impact: string;  // "15% win rate improvement potential"

  // Display
  bar_max: number;
  rep_bar_pct: number;     // 0-100 for bar width
  top_bar_pct: number;
}
```

---

### Widget: Activity Analysis

**Dataset**: `/RevOps/Analytics/rep_activity_metrics`

```typescript
interface RepActivityMetrics {
  rep_id: string;
  period: "30d" | "7d" | "today";

  // Activity counts
  total_activities: number;
  call_count: number;
  email_count: number;
  meeting_count: number;
  demo_count: number;

  // Daily metrics
  avg_daily_activities: number;
  team_avg_daily: number;
  top_performer_daily: number;

  // Distribution by day
  monday_avg: number;
  tuesday_avg: number;
  wednesday_avg: number;
  thursday_avg: number;
  friday_avg: number;

  // Patterns
  low_activity_days: string[];  // ["Thursday", "Friday"]
  activity_consistency_score: number;
}
```

**Transform**: `compute_rep_activity_metrics.py`

```python
@transform(
    activities=Input("/RevOps/Scenario/activities"),
    output=Output("/RevOps/Analytics/rep_activity_metrics")
)
def compute(ctx, activities, output):
    """
    Aggregate activities by rep with daily/weekly patterns.
    """
    pass
```

---

### Widget: Next Best Actions

**Dataset**: `/RevOps/Analytics/next_best_actions`

```typescript
interface NextBestAction {
  action_id: string;
  rep_id: string;

  // Target
  opportunity_id: string;
  account_name: string;
  contact_name: string;
  contact_role: string;

  // Action details
  action_type: "call" | "email" | "meeting" | "task";
  action_reason: string;
  priority_score: number;
  priority_rank: number;

  // Timing
  best_time_start: string;  // "2:00 PM"
  best_time_end: string;    // "4:00 PM"
  urgency: "today" | "this_week" | "soon";

  // Context
  deal_amount: number;
  deal_health: number;
  days_since_contact: number;
  risk_if_delayed: string;

  // Display
  action_icon: string;
  time_display: string;
}
```

**Transform**: `compute_next_best_actions.py`

```python
@transform(
    opportunities=Input("/RevOps/Enriched/deal_health_scores"),
    activities=Input("/RevOps/Analytics/rep_activity_metrics"),
    output=Output("/RevOps/Analytics/next_best_actions")
)
def compute(ctx, opportunities, activities, output):
    """
    Generate prioritized action list for each rep.

    Priority factors:
    1. Deal health (lower = more urgent)
    2. Days since last activity
    3. Deal amount
    4. Close date proximity
    5. Specific risk flags (no champion, going dark, etc.)
    """
    pass
```

---

### Widget: AI Coaching Insights

**Dataset**: `/RevOps/Analytics/rep_coaching_insights`

```typescript
interface RepCoachingInsights {
  rep_id: string;
  generated_at: Date;
  model_version: string;

  // Strengths (max 3)
  strengths: {
    title: string;
    detail: string;
    metric_value: string;
    metric_comparison: string;
  }[];

  // Improvements (max 3)
  improvements: {
    title: string;
    detail: string;
    gap_value: string;
    impact_estimate: string;
    suggested_action: string;
  }[];

  // Priority actions (max 3)
  priority_actions: {
    deal_name: string;
    deal_amount: number;
    action: string;
    reason: string;
    deadline: string;
  }[];

  // Summary
  overall_assessment: string;
  primary_focus_area: string;
}
```

**Note**: This can be:
1. Pre-computed by a batch transform using an LLM
2. Generated on-demand by AIP agent
3. Template-based from metrics (fallback)

---

## Shared Data Types

### Common Fields

```typescript
// Used across multiple tables
interface CommonFields {
  created_at: Date;
  updated_at: Date;
  data_freshness: Date;  // When source data was last refreshed
}

// Filter parameters
interface DashboardFilters {
  quarter: string;
  team: string;        // "All" or manager_name
  segment: string;     // "All" | "Enterprise" | "Mid-Market" | "SMB"
  region: string;      // "All" | specific region
}
```

### Display Formatters

```typescript
// Standard formatters to apply
const formatters = {
  currency: (v: number) => `$${(v/1000000).toFixed(1)}M`,
  currencyK: (v: number) => `$${(v/1000).toFixed(0)}K`,
  percent: (v: number) => `${(v * 100).toFixed(0)}%`,
  percentDelta: (v: number) => `${v >= 0 ? '+' : ''}${(v * 100).toFixed(0)}%`,
  days: (v: number) => `${v}d`,
  rank: (r: number, t: number) => `#${r}/${t}`,
};
```

---

## Transform Dependency Graph

```
                    ┌─────────────────────────┐
                    │  Scenario Data          │
                    │  (opportunities, reps)  │
                    └───────────┬─────────────┘
                                │
                    ┌───────────▼─────────────┐
                    │  deal_health_scores     │
                    │  (enrichment)           │
                    └───────────┬─────────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        │                       │                       │
        ▼                       ▼                       ▼
┌───────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ forecast_     │     │ pipeline_health │     │ rep_performance │
│ confidence    │     │ _summary        │     │                 │
└───────┬───────┘     └────────┬────────┘     └────────┬────────┘
        │                      │                       │
        ▼                      ▼                       ▼
┌───────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ dashboard_kpis│     │ stuck_deals_view│     │ rep_driver_     │
│               │     │                 │     │ comparison      │
└───────────────┘     └─────────────────┘     └─────────────────┘
        │                      │                       │
        └──────────────────────┴───────────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │   DASHBOARDS        │
                    └─────────────────────┘
```

---

## Build Order

1. **Scenario Data** (no dependencies)
   - `generate_scenario_opportunities.py`
   - `generate_scenario_reps.py`
   - `generate_process_events.py`
   - `stage_benchmarks.py`

2. **Enrichment** (depends on scenario)
   - `calculate_deal_health.py`

3. **Core Analytics** (depends on enrichment)
   - `pipeline_health_summary.py`
   - `forecast_confidence.py`
   - `rep_performance_analytics.py`
   - `process_bottleneck_analytics.py`

4. **Dashboard Views** (depends on analytics)
   - `compute_dashboard_kpis.py` (NEW)
   - `generate_forecast_history.py` (NEW)
   - `prepare_stuck_deals_view.py` (NEW)
   - `compute_rep_activity_metrics.py` (NEW)
   - `compute_next_best_actions.py` (NEW)

5. **AI Features** (depends on all above)
   - `generate_coaching_insights.py` (NEW - optional batch)
