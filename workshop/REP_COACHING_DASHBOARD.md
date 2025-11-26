# Rep Coaching Dashboard - Workshop Specification

## Overview

**Purpose**: Individual rep performance analysis and coaching guidance
**Primary Users**: Sales Managers, Reps (self-service), Enablement
**Update Frequency**: Real-time with weekly snapshot comparisons

---

## Layout Structure

```
+------------------------------------------------------------------+
|  HEADER: Rep Coaching Dashboard     [Rep: Sarah Williams ▼] [Q4] |
+------------------------------------------------------------------+
|                                                                   |
|  +----------------------------------------------------------+    |
|  |                  PERFORMANCE SUMMARY                      |    |
|  | +----------+ +----------+ +----------+ +----------+ +----+|    |
|  | |ATTAINMENT| | WIN RATE | | AVG DEAL | |SALES CYCLE| RANK||    |
|  | |   78%    | |   32%    | |  $145K   | |  68 days  | #4  ||    |
|  | | vs Q3:+12| | Team:28% | | Team:$125| | Team:72d  | +2  ||    |
|  | +----------+ +----------+ +----------+ +----------+ +----+|    |
|  +----------------------------------------------------------+    |
|                                                                   |
|  +------------------------+  +-----------------------------+      |
|  |   WIN RATE DRIVERS     |  |     ACTIVITY ANALYSIS       |      |
|  |                        |  |                             |      |
|  | Your Pattern vs Top 25%|  | Activity Mix (Last 30 Days) |      |
|  |                        |  | [Horizontal bars]           |      |
|  | Stakeholders  3.2  4.8 |  |                             |      |
|  | [====----] [========]  |  | Daily Activity Trend        |      |
|  |                        |  | [Line chart]                |      |
|  | Days to Meeting 4.2 2.1|  |                             |      |
|  | [======] [===]         |  | 📍 8.2/day                  |      |
|  |                        |  | 📊 Team: 10.5/day           |      |
|  | Champion ID   65%  85% |  | ⭐ Top: 14.2/day            |      |
|  | [=====] [========]     |  |                             |      |
|  |                        |  |                             |      |
|  | Econ Buyer    55%  78% |  |                             |      |
|  | [====] [=======]       |  |                             |      |
|  +------------------------+  +-----------------------------+      |
|                                                                   |
|  +----------------------------------------------------------+    |
|  |              🤖 AI COACHING INSIGHTS                      |    |
|  |                                                           |    |
|  | STRENGTHS IDENTIFIED:                                     |    |
|  | ✓ Strong deal qualification - 15% higher conversion       |    |
|  | ✓ Excellent proposal-to-negotiation velocity              |    |
|  | ✓ High customer engagement scores                         |    |
|  |                                                           |    |
|  | AREAS FOR IMPROVEMENT:                                    |    |
|  | ⚡ Increase executive engagement - 23% missing EB         |    |
|  | ⚡ Earlier champion identification - 12 days later        |    |
|  | ⚡ Activity consistency - 30% drop Thu/Fri                |    |
|  |                                                           |    |
|  | RECOMMENDED ACTIONS THIS WEEK:                            |    |
|  | 1. TechVentures ($420K): Identify champion - at risk      |    |
|  | 2. CloudFirst ($650K): Request exec intro - competition   |    |
|  | 3. DataSystems ($890K): Re-engage - 14 days dark          |    |
|  +----------------------------------------------------------+    |
|                                                                   |
|  +------------------------+  +-----------------------------+      |
|  |    PIPELINE REVIEW     |  |     NEXT BEST ACTIONS       |      |
|  |                        |  |                             |      |
|  | Deal         Health Act|  | 📞 CALL: John @ TechVentures|      |
|  | ------------ ------ ---|  |    "Follow up on pricing"   |      |
|  | TechVentures   55  📞  |  |    Best time: Today 2-4pm   |      |
|  | CloudFirst     52  📧  |  |                             |      |
|  | DataSystems    45  📅  |  |    [Start Call] [Defer]     |      |
|  +------------------------+  +-----------------------------+      |
|                                                                   |
+------------------------------------------------------------------+
```

---

## Widget Specifications

### 1. Header with Rep Selector

**Widget Type**: `workshop-header` with contextual filters

```yaml
header:
  title: "Rep Coaching Dashboard"
  subtitle_dynamic: "{{rep_name}}'s Performance"
  filters:
    - name: "rep_selector"
      type: "searchable-dropdown"
      data_source: "/RevOps/Scenario/sales_reps"
      display_field: "rep_name"
      value_field: "rep_id"
      default: "{{current_user_rep_id}}"  # Auto-select logged-in user
      binding: "{{selected_rep_id}}"
      allow_manager_override: true  # Managers can view their team
    - name: "quarter_selector"
      type: "dropdown"
      options: ["Q1 2024", "Q2 2024", "Q3 2024", "Q4 2024"]
      default: "Q4 2024"
      binding: "{{quarter}}"
  context_badge:
    show_when: "{{viewing_as_manager}}"
    text: "Viewing: {{rep_name}}"
    color: "purple"
```

---

### 2. Performance Summary Cards

**Widget Type**: `kpi-card-row`

```yaml
widget: kpi-card-row
title: "PERFORMANCE SUMMARY"
data_source: "/RevOps/Analytics/rep_performance"
filter: "rep_id = '{{selected_rep_id}}'"
cards:
  - id: "attainment"
    title: "ATTAINMENT"
    value: "{{ytd_attainment}}"
    format: "percent"
    trend:
      compare_to: "q3_attainment"
      format: "vs Q3: {{direction}}{{delta}}%"
    progress_bar:
      current: "{{ytd_attainment}}"
      target: 1.0
      color_logic:
        green: ">= 1.0"
        yellow: ">= 0.7"
        red: "< 0.7"

  - id: "win_rate"
    title: "WIN RATE"
    value: "{{win_rate}}"
    format: "percent"
    comparison:
      label: "Team"
      value: "{{team_avg_win_rate}}"
      format: "percent"
    indicator:
      type: "arrow"
      logic: "win_rate_vs_team"
      positive: ">= 0"

  - id: "avg_deal"
    title: "AVG DEAL"
    value: "{{avg_deal_size}}"
    format: "$#,##0K"
    comparison:
      label: "Team"
      value: "{{team_avg_deal_size}}"
      format: "$#,##0K"
    indicator:
      type: "arrow"
      value: "{{deal_size_vs_team_pct}}"

  - id: "sales_cycle"
    title: "SALES CYCLE"
    value: "{{avg_sales_cycle_days}}"
    format: "# days"
    comparison:
      label: "Team"
      value: "{{team_avg_cycle}}"
      format: "#d"
    indicator:
      type: "arrow"
      value: "{{cycle_vs_team}}"
      invert: true  # Lower is better

  - id: "rank"
    title: "RANK"
    value: "{{overall_rank}}"
    format: "#{{overall_rank}}/{{total_reps}}"
    trend:
      compare_to: "q3_rank"
      format: "{{direction}}{{delta}} from Q3"
      invert: true
```

---

### 3. Win Rate Drivers Panel

**Widget Type**: `comparison-bars`

```yaml
widget: comparison-bars
title: "WIN RATE DRIVERS"
subtitle: "Your Pattern vs Top Performers:"
height: 280
data_source: "/RevOps/Analytics/rep_performance"
filter: "rep_id = '{{selected_rep_id}}'"
top_performer_source: "/RevOps/Analytics/top_performer_patterns"
filter_top: "group = 'Top Performers (100%+ attainment)'"
metrics:
  - id: "stakeholders"
    label: "Stakeholders Engaged"
    your_value: "{{avg_stakeholders}}"
    top_value: "{{top_avg_stakeholders}}"
    format: "#.#"
    bar_max: 6
    good_direction: "higher"

  - id: "days_to_meeting"
    label: "Days to First Meeting"
    your_value: "{{avg_days_to_meeting}}"
    top_value: "{{top_avg_days_to_meeting}}"
    format: "#.#"
    bar_max: 7
    good_direction: "lower"

  - id: "champion_rate"
    label: "Champion Identified"
    your_value: "{{champion_identification_rate}}"
    top_value: "{{top_champion_rate}}"
    format: "percent"
    bar_max: 100
    good_direction: "higher"

  - id: "eb_engagement"
    label: "Economic Buyer Engaged"
    your_value: "{{economic_buyer_rate}}"
    top_value: "{{top_eb_rate}}"
    format: "percent"
    bar_max: 100
    good_direction: "higher"

display:
  your_bar_color: "#3b82f6"
  top_bar_color: "#22c55e"
  labels:
    your: "You"
    top: "Top 25%"
  show_gap_indicator: true
  gap_threshold: 20  # Show alert if >20% behind
```

---

### 4. Activity Analysis Panel

**Widget Type**: `activity-dashboard`

```yaml
widget: activity-dashboard
title: "ACTIVITY ANALYSIS"
height: 280
data_source: "/RevOps/Analytics/rep_activities"  # Need to create
filter: "rep_id = '{{selected_rep_id}}' AND date >= {{30_days_ago}}"

sections:
  - id: "activity_mix"
    title: "Activity Mix (Last 30 Days)"
    type: "horizontal-stacked-bar"
    categories:
      - label: "Calls"
        field: "call_count"
        color: "#3b82f6"
        icon: "phone"
      - label: "Emails"
        field: "email_count"
        color: "#8b5cf6"
        icon: "mail"
      - label: "Meetings"
        field: "meeting_count"
        color: "#22c55e"
        icon: "calendar"
      - label: "Demos"
        field: "demo_count"
        color: "#f59e0b"
        icon: "presentation"

  - id: "daily_trend"
    title: "Daily Activity Trend"
    type: "line-chart"
    x_axis: "date"
    y_axis: "activity_count"
    show_weekday_labels: true
    highlight_low_days: true
    low_threshold: 5

  - id: "benchmarks"
    title: ""
    type: "metric-list"
    metrics:
      - icon: "📍"
        label: "You"
        value: "{{avg_daily_activities}}"
        format: "#.# activities/day"
      - icon: "📊"
        label: "Team avg"
        value: "{{team_avg_daily}}"
        format: "#.#/day"
      - icon: "⭐"
        label: "Top performer"
        value: "{{top_daily}}"
        format: "#.#/day"
```

---

### 5. AI Coaching Insights Panel

**Widget Type**: `ai-insights-card`

```yaml
widget: ai-insights-card
title: "🤖 AI COACHING INSIGHTS"
height: 260
ai_source:
  agent: "rep_coaching_assistant"
  prompt_template: |
    Analyze rep {{rep_name}} (ID: {{rep_id}}) performance.
    Win rate: {{win_rate}}, Avg stakeholders: {{avg_stakeholders}}
    Team avg stakeholders: {{team_avg_stakeholders}}

    Identify:
    1. Top 3 strengths (with specific metrics)
    2. Top 3 areas for improvement (with specific gaps)
    3. Top 3 recommended actions for THIS WEEK (with specific deals)

  refresh_trigger: "on_rep_change"
  cache_duration: "1 hour"

display:
  sections:
    - id: "strengths"
      title: "STRENGTHS IDENTIFIED:"
      icon: "✓"
      color: "#22c55e"
      max_items: 3

    - id: "improvements"
      title: "AREAS FOR IMPROVEMENT:"
      icon: "⚡"
      color: "#f59e0b"
      max_items: 3

    - id: "actions"
      title: "RECOMMENDED ACTIONS THIS WEEK:"
      icon: "number"
      color: "#3b82f6"
      max_items: 3
      show_deal_links: true

fallback:
  type: "static"
  source: "/RevOps/Analytics/rep_performance"
  filter: "rep_id = '{{selected_rep_id}}'"
  template: |
    STRENGTHS: Based on metrics...
    IMPROVEMENTS: {{primary_coaching_need}}
    ACTIONS: {{recommended_action}}
```

---

### 6. Pipeline Review Table

**Widget Type**: `compact-table`

```yaml
widget: compact-table
title: "PIPELINE REVIEW"
height: 160
data_source: "/RevOps/Enriched/deal_health_scores"
filter: "owner_id = '{{selected_rep_id}}' AND is_closed = false"
sort_by: "health_score ASC"
max_rows: 5
columns:
  - field: "account_name"
    header: "Deal"
    width: 140
    truncate: 18
    clickable: true
    action: "select_deal"

  - field: "health_score"
    header: "Health"
    width: 60
    format: "#"
    color_logic:
      red: "< 50"
      yellow: "< 70"
      green: ">= 70"

  - field: "next_action_icon"
    header: "Action"
    width: 50
    computed: |
      CASE priority_action
        WHEN 'call' THEN '📞'
        WHEN 'email' THEN '📧'
        WHEN 'meeting' THEN '📅'
        ELSE '📋'
      END
    clickable: true
    action: "execute_action"

row_click_action:
  type: "set_variable"
  variable: "selected_deal_id"
  value: "{{opportunity_id}}"
```

---

### 7. Next Best Actions Panel

**Widget Type**: `action-card`

```yaml
widget: action-card
title: "NEXT BEST ACTIONS"
height: 160
data_source: "/RevOps/Analytics/next_best_actions"  # Need to create
filter: "rep_id = '{{selected_rep_id}}'"
sort_by: "priority_score DESC"
display_first: 1

layout:
  icon:
    type: "dynamic"
    field: "action_type"
    mapping:
      call: "📞"
      email: "📧"
      meeting: "📅"
      task: "📋"

  header:
    template: "{{action_type_label}}: {{contact_name}} @ {{account_name}}"

  body:
    template: "\"{{action_reason}}\""
    style: "italic"

  metadata:
    template: "Best time: {{best_time}}"

  actions:
    - label: "Start Call"
      action: "initiate_call"
      primary: true
      show_when: "action_type = 'call'"
    - label: "Compose Email"
      action: "compose_email"
      primary: true
      show_when: "action_type = 'email'"
    - label: "Schedule"
      action: "open_calendar"
      primary: true
      show_when: "action_type = 'meeting'"
    - label: "Defer"
      action: "defer_action"
      secondary: true
      prompt: "Defer until when?"
      options: ["Tomorrow", "Next Week", "Custom"]
```

---

## Manager View Extensions

When a manager is viewing:

```yaml
manager_mode:
  trigger: "user_role = 'manager' AND selected_rep_id != current_user_rep_id"
  additions:
    - widget: "comparison-to-peers"
      position: "below performance_summary"
      shows: "Selected rep vs team distribution"

    - widget: "coaching-history"
      position: "tab in ai_insights"
      shows: "Previous coaching notes and progress"

    - widget: "create-coaching-note"
      position: "action in header"
      action: "Opens coaching note modal"

  filters:
    - name: "team_comparison"
      type: "toggle"
      label: "Show team comparison"
      default: true
```

---

## Color Palette (Dark Theme)

```css
:root {
  /* Same as Pipeline Dashboard for consistency */
  --bg-primary: #0a0a0a;
  --bg-secondary: #141414;
  --bg-card: #1a1a1a;

  /* Performance indicators */
  --perf-excellent: #22c55e;
  --perf-good: #84cc16;
  --perf-average: #eab308;
  --perf-below: #f97316;
  --perf-poor: #ef4444;

  /* Comparison bars */
  --bar-you: #3b82f6;
  --bar-top: #22c55e;
  --bar-team: #6b7280;

  /* AI Insights */
  --ai-strength: #22c55e;
  --ai-improve: #f59e0b;
  --ai-action: #3b82f6;
}
```

---

## Data Requirements

### Required Datasets
1. `/RevOps/Analytics/rep_performance` - Core metrics
2. `/RevOps/Analytics/top_performer_patterns` - Benchmark data
3. `/RevOps/Enriched/deal_health_scores` - Pipeline review

### New Transforms Needed

```python
# 1. Rep Activity Analytics
@transform(
    activities=Input("/RevOps/Scenario/activities"),
    output=Output("/RevOps/Analytics/rep_activities")
)
def compute_rep_activities(ctx, activities, output):
    # Aggregate activities by rep, type, day
    pass

# 2. Next Best Actions
@transform(
    opportunities=Input("/RevOps/Enriched/deal_health_scores"),
    activities=Input("/RevOps/Analytics/rep_activities"),
    output=Output("/RevOps/Analytics/next_best_actions")
)
def compute_next_best_actions(ctx, opportunities, activities, output):
    # Score and prioritize actions for each rep
    pass
```

---

## Mobile Responsive Behavior

```yaml
breakpoints:
  desktop: ">= 1200px"
    layout: "full"
  tablet: "768px - 1199px"
    layout: "stacked"
    activity_analysis: "collapsed_default"
  mobile: "< 768px"
    layout: "single_column"
    hide: ["win_rate_drivers"]
    ai_insights: "expandable_accordion"
    performance_summary: "2x2_grid + swipe"
```
