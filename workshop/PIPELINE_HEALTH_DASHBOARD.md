# Pipeline Health Dashboard - Workshop Specification

## Overview

**Purpose**: Executive command center for pipeline visibility and forecast confidence
**Primary Users**: CRO, VP Sales, Sales Managers
**Update Frequency**: Real-time (on page load)

---

## Layout Structure

```
+------------------------------------------------------------------+
|  HEADER: Pipeline Health Dashboard    [Q4 2024 ▼] [Team: All ▼]  |
+------------------------------------------------------------------+
|                                                                   |
|  +------------+ +------------+ +------------+ +------------+      |
|  |   QUOTA    | | AI FORECAST| |   COMMIT   | |    GAP     |      |
|  |   $8.5M    | |   $7.2M    | |   $6.8M    | |  -$1.3M    |      |
|  |  [Target]  | | [Projected]| |[Committed] | | [Coverage] |      |
|  +------------+ +------------+ +------------+ +------------+      |
|                                                                   |
|  +----------------------------------------------------------+    |
|  |              FORECAST vs ACTUALS TREND                    |    |
|  |  [Line chart: Target / Commit / AI Forecast / Closed]    |    |
|  +----------------------------------------------------------+    |
|                                                                   |
|  +------------------------+  +-----------------------------+      |
|  |    PIPELINE FUNNEL     |  |      STAGE VELOCITY         |      |
|  |   [Funnel viz with $]  |  |  [Table: Avg/Bench/Trend]   |      |
|  +------------------------+  +-----------------------------+      |
|                                                                   |
|  +----------------------------------------------------------+    |
|  |           STUCK DEALS REQUIRING ACTION                    |    |
|  |  [Table: Deal/Amount/Stage/Days/Health/Flags/Actions]    |    |
|  +----------------------------------------------------------+    |
|                                                                   |
|  +------------------------+  +-----------------------------+      |
|  | HEALTH DISTRIBUTION    |  |   DEAL DETAIL + AI INSIGHTS |      |
|  | [Horizontal bar chart] |  |   [Selected deal details]   |      |
|  +------------------------+  +-----------------------------+      |
|                                                                   |
+------------------------------------------------------------------+
```

---

## Widget Specifications

### 1. Header Bar

**Widget Type**: `workshop-header` with filters

```yaml
header:
  title: "Pipeline Health Dashboard"
  filters:
    - name: "quarter_filter"
      type: "dropdown"
      options: ["Q1 2024", "Q2 2024", "Q3 2024", "Q4 2024"]
      default: "Q4 2024"
      binding: "{{quarter}}"
    - name: "team_filter"
      type: "dropdown"
      options_source: "/RevOps/Scenario/sales_reps.manager_name"
      include_all: true
      default: "All"
      binding: "{{team}}"
```

---

### 2. KPI Cards Row

**Widget Type**: `kpi-card` x 4

#### Card 1: Quota
```yaml
widget: kpi-card
title: "QUOTA"
value:
  source: "/RevOps/Analytics/forecast_confidence"
  filter: "category = 'TOTAL FORECAST'"
  field: "quarter_target"
  format: "$#,##0.0M"
subtitle: "Target"
color_logic: "neutral"
progress_bar:
  current: "{{closed_won_amount}}"
  target: "{{quarter_target}}"
```

#### Card 2: AI Forecast
```yaml
widget: kpi-card
title: "AI FORECAST"
value:
  source: "/RevOps/Analytics/forecast_confidence"
  filter: "category = 'TOTAL FORECAST'"
  field: "weighted_amount"
  format: "$#,##0.0M"
subtitle: "Projected"
trend:
  compare_to: "last_week"
  show_arrow: true
progress_bar:
  current: "{{weighted_amount}}"
  target: "{{quarter_target}}"
  color: "blue"
```

#### Card 3: Commit
```yaml
widget: kpi-card
title: "COMMIT"
value:
  source: "/RevOps/Analytics/forecast_confidence"
  filter: "category = 'Commit'"
  field: "weighted_amount"
  format: "$#,##0.0M"
subtitle_computed: "{{confidence_pct}}% confidence"
progress_bar:
  current: "{{weighted_amount}}"
  target: "{{amount}}"
  color: "green"
```

#### Card 4: Gap
```yaml
widget: kpi-card
title: "GAP"
value:
  source: "/RevOps/Analytics/forecast_confidence"
  filter: "category = 'TOTAL FORECAST'"
  field: "gap_to_target"
  format: "$#,##0.0M"
  negative_format: "-$#,##0.0M"
subtitle_computed: "{{gap_pct}}% to close"
secondary_metric:
  label: "Risk"
  value: "{{risk_assessment}}"
tertiary_metric:
  label: "Coverage"
  value: "{{pipeline_coverage}}x"
color_logic:
  positive: "red"
  negative: "green"
```

---

### 3. Forecast vs Actuals Trend

**Widget Type**: `time-series-chart`

```yaml
widget: time-series-chart
title: "FORECAST vs ACTUALS TREND"
height: 280
data_source: "/RevOps/Analytics/forecast_history"  # Would need this transform
x_axis:
  field: "week"
  format: "Week %W"
series:
  - name: "Target"
    field: "cumulative_target"
    color: "#ffffff"
    style: "dashed"
  - name: "Commit"
    field: "cumulative_commit"
    color: "#4ade80"
    style: "solid"
  - name: "AI Forecast"
    field: "cumulative_forecast"
    color: "#60a5fa"
    style: "solid"
  - name: "Closed Won"
    field: "cumulative_closed"
    color: "#fbbf24"
    style: "solid"
    fill: true
legend:
  position: "top-right"
  interactive: true
annotations:
  - type: "vertical-line"
    value: "current_week"
    label: "Today"
    color: "#ffffff"
    style: "dotted"
```

---

### 4. Pipeline Funnel

**Widget Type**: `funnel-chart`

```yaml
widget: funnel-chart
title: "PIPELINE FUNNEL"
height: 240
data_source: "/RevOps/Analytics/pipeline_health_summary"
filter: "view_type = 'by_stage'"
stages:
  - label: "Qualification"
    value_field: "total_amount"
    filter: "forecast_category = 'Prospecting'"
  - label: "Discovery"
    value_field: "total_amount"
    filter: "forecast_category = 'Discovery'"
  - label: "Solution"
    value_field: "total_amount"
    filter: "forecast_category = 'Solution Design'"
  - label: "Proposal"
    value_field: "total_amount"
    filter: "forecast_category = 'Proposal'"
  - label: "Negotiation"
    value_field: "total_amount"
    filter: "forecast_category = 'Negotiation'"
format: "$#,##0.0M"
colors:
  gradient_start: "#3b82f6"
  gradient_end: "#1e40af"
show_conversion_rates: true
```

---

### 5. Stage Velocity Table

**Widget Type**: `data-table`

```yaml
widget: data-table
title: "STAGE VELOCITY"
height: 240
data_source: "/RevOps/Analytics/process_bottlenecks"
columns:
  - field: "activity"
    header: "Stage"
    width: 120
  - field: "avg_duration"
    header: "Avg"
    format: "#d"
    width: 60
  - field: "benchmark_days"
    header: "Bnch"
    format: "#d"
    width: 60
  - field: "deviation_display"
    header: "Trend"
    width: 80
    computed: |
      IF(deviation_days > 3, "🔴 +" + deviation_days + "d",
      IF(deviation_days > 0, "⚠️ +" + deviation_days + "d",
      IF(deviation_days < 0, "▲ " + deviation_days + "d",
      "✓")))
    color_logic:
      red: "deviation_days > 3"
      yellow: "deviation_days > 0"
      green: "deviation_days <= 0"
sort_by: "stage_order"
footer:
  type: "custom"
  content: |
    Conversion Rates:
    Qual→Disc: {{qual_disc_rate}}%  Disc→Sol: {{disc_sol_rate}}%
    Sol→Prop: {{sol_prop_rate}}%    Prop→Neg: {{prop_neg_rate}}%
    Neg→Won: {{neg_won_rate}}%      Overall: {{overall_rate}}%
```

---

### 6. Stuck Deals Table

**Widget Type**: `interactive-table`

```yaml
widget: interactive-table
title: "STUCK DEALS REQUIRING ACTION"
height: 200
data_source: "/RevOps/Analytics/deals_stuck_in_process"
header_action:
  label: "[View All]"
  action: "navigate"
  target: "/deals/stuck"
columns:
  - field: "urgency_indicator"
    header: ""
    width: 30
    computed: |
      CASE urgency
        WHEN 'CRITICAL' THEN '🔴'
        WHEN 'HIGH' THEN '🟠'
        WHEN 'MEDIUM' THEN '🟡'
        ELSE '🟢'
      END
  - field: "account_name"
    header: "Deal"
    width: 140
    style: "bold"
    clickable: true
    action: "select_deal"
  - field: "amount"
    header: "Amount"
    format: "$#,##0K"
    width: 80
  - field: "stuck_activity"
    header: "Stage"
    width: 100
    style: "italic"
  - field: "duration_days"
    header: "Days"
    width: 50
  - field: "health_score"
    header: "Health"
    width: 70
    format: "#/100"
    color_logic:
      red: "health_score < 50"
      yellow: "health_score < 70"
      green: "health_score >= 70"
  - field: "risk_flags"
    header: "Risk Flags"
    width: 120
    style: "code"
row_click_action:
  type: "set_variable"
  variable: "selected_deal_id"
  value: "{{opportunity_id}}"
max_rows: 5
```

---

### 7. Health Distribution Chart

**Widget Type**: `horizontal-bar-chart`

```yaml
widget: horizontal-bar-chart
title: "HEALTH DISTRIBUTION"
height: 160
data_source: "/RevOps/Analytics/pipeline_health_summary"
filter: "view_type = 'overall'"
categories:
  - label: "Healthy"
    value: "{{healthy_amount}}"
    percentage: "{{healthy_pct}}"
    color: "#22c55e"
  - label: "Monitor"
    value: "{{monitor_amount}}"
    percentage: "{{monitor_pct}}"
    color: "#eab308"
  - label: "At Risk"
    value: "{{at_risk_amount}}"
    percentage: "{{at_risk_pct}}"
    color: "#f97316"
  - label: "Critical"
    value: "{{critical_amount}}"
    percentage: "{{critical_pct}}"
    color: "#ef4444"
show_percentages: true
footer:
  trend_indicator:
    compare_to: "last_week"
    metric: "at_risk_pct"
    format: "Trend: {{direction}}{{delta}}% vs last week"
```

---

### 8. Deal Detail + AI Insights Panel

**Widget Type**: `detail-panel`

```yaml
widget: detail-panel
title: "DEAL DETAIL + AI INSIGHTS"
height: 160
data_source: "/RevOps/Enriched/deal_health_scores"
filter: "opportunity_id = {{selected_deal_id}}"
empty_state: "Select a deal to view details"
layout:
  header:
    primary: "{{account_name}}"
    secondary: "{{opportunity_name}}"
  metrics_row:
    - label: ""
      value: "{{amount}}"
      format: "$#,##0,000"
    - label: ""
      value: "{{stage_name}}"
    - label: ""
      value: "{{days_in_current_stage}} days"
ai_insights:
  title: "🤖 AI ANALYSIS:"
  source: "aip_agent"
  agent: "deal_risk_analyzer"
  prompt_template: "Analyze deal {{opportunity_id}} and provide a 2-sentence summary of risk and recommended action."
  display: "quoted_text"
  style: "italic"
actions:
  - label: "View Full Analysis"
    action: "open_agent_chat"
    agent: "deal_risk_analyzer"
  - label: "Create Task"
    action: "create_task"
```

---

## Interactions & Actions

### Filter Propagation
```yaml
filters:
  quarter_filter:
    affects: ["all_widgets"]
    parameter: "close_date_range"
  team_filter:
    affects: ["all_widgets"]
    parameter: "manager_name"
```

### Deal Selection Flow
```yaml
interactions:
  - trigger: "row_click on stuck_deals_table"
    action: "set_variable"
    variable: "selected_deal_id"
  - trigger: "selected_deal_id changes"
    action: "refresh_widget"
    widgets: ["deal_detail_panel"]
```

### Quick Actions
```yaml
quick_actions:
  - icon: "phone"
    label: "Schedule Call"
    action: "create_activity"
    type: "call"
    deal_id: "{{selected_deal_id}}"
  - icon: "email"
    label: "Send Email"
    action: "create_activity"
    type: "email"
  - icon: "alert"
    label: "Escalate"
    action: "create_escalation"
    deal_id: "{{selected_deal_id}}"
```

---

## Color Palette (Dark Theme)

```css
:root {
  /* Background */
  --bg-primary: #0a0a0a;
  --bg-secondary: #141414;
  --bg-card: #1a1a1a;
  --bg-card-hover: #242424;

  /* Text */
  --text-primary: #ffffff;
  --text-secondary: #a3a3a3;
  --text-muted: #737373;

  /* Borders */
  --border-color: #2a2a2a;
  --border-focus: #525252;

  /* Status Colors */
  --status-healthy: #22c55e;
  --status-monitor: #eab308;
  --status-at-risk: #f97316;
  --status-critical: #ef4444;

  /* Accent */
  --accent-blue: #3b82f6;
  --accent-purple: #8b5cf6;

  /* Chart Colors */
  --chart-1: #3b82f6;
  --chart-2: #22c55e;
  --chart-3: #f59e0b;
  --chart-4: #ef4444;
}
```

---

## Data Requirements

### Required Datasets
1. `/RevOps/Analytics/forecast_confidence` - For KPI cards
2. `/RevOps/Analytics/pipeline_health_summary` - For funnel & distribution
3. `/RevOps/Analytics/process_bottlenecks` - For stage velocity
4. `/RevOps/Analytics/deals_stuck_in_process` - For stuck deals table
5. `/RevOps/Enriched/deal_health_scores` - For deal detail panel

### New Transforms Needed
1. **Forecast History** - Weekly snapshots for trend chart
2. **Stage Conversion Rates** - For funnel and velocity footer

---

## Mobile Responsive Behavior

```yaml
breakpoints:
  desktop: ">= 1200px"
    layout: "full"
  tablet: "768px - 1199px"
    layout: "stacked_2col"
    hide: ["funnel_chart"]
  mobile: "< 768px"
    layout: "single_column"
    hide: ["funnel_chart", "stage_velocity"]
    kpi_cards: "2x2_grid"
```
