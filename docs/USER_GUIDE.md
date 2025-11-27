# RevOps Hub User Guide

Welcome to RevOps Hub! This guide will help you get started with the platform and make the most of its features.

## Table of Contents

1. [Getting Started](#getting-started)
2. [Dashboard Overview](#dashboard-overview)
3. [Pipeline Management](#pipeline-management)
4. [Forecasting](#forecasting)
5. [Coaching & Performance](#coaching--performance)
6. [AI-Powered Insights](#ai-powered-insights)
7. [Alerts & Notifications](#alerts--notifications)
8. [Best Practices](#best-practices)

---

## Getting Started

### Logging In

1. Navigate to your RevOps Hub instance URL
2. Enter your email and password (or use SSO if configured)
3. You'll land on your personalized dashboard

### Navigation

The main navigation includes:

| Section | Description | Who Uses It |
|---------|-------------|-------------|
| **Home** | Overview KPIs and recent activity | Everyone |
| **Pipeline** | Deal health and hygiene | Sales Managers, RevOps |
| **Forecast** | Revenue projections | Sales Leaders, Finance |
| **Coaching** | Rep performance insights | Managers, RevOps |
| **Scenarios** | What-if modeling | RevOps, Finance |
| **Alerts** | Active issues requiring attention | Everyone |

### Understanding Your Dashboard

Your home dashboard shows:

- **Pipeline Value**: Total open pipeline
- **Forecast Accuracy**: How well predictions match outcomes
- **Health Distribution**: Deals by health status
- **Top Actions**: AI-recommended next steps

---

## Pipeline Management

### Pipeline Health View

The Pipeline Health dashboard shows all open deals with their health scores:

#### Health Scores Explained

| Score | Status | Meaning |
|-------|--------|---------|
| 80-100 | Healthy | Deal on track, no concerns |
| 60-79 | Monitor | Minor issues, keep watching |
| 40-59 | At Risk | Multiple warning signs |
| 0-39 | Critical | Urgent intervention needed |

#### Health Factors

Deal health is calculated based on:

1. **Stage Velocity**: Is the deal progressing at expected pace?
2. **Activity Recency**: When was last customer engagement?
3. **Multi-threading**: Are multiple stakeholders engaged?
4. **Close Date**: Is the expected close date realistic?
5. **Amount**: Has the deal value changed unexpectedly?

### Pipeline Hygiene

The Hygiene view highlights deals that need attention:

#### Common Issues

- **Stale Deals**: No activity in 14+ days
- **Past Close Date**: Close date has passed
- **Missing Next Step**: No defined next action
- **Single-threaded**: Only one contact engaged
- **Missing Amount**: Deal value not set

#### Taking Action

Click any hygiene alert to:
1. View deal details
2. See recommended actions
3. Update the deal directly in your CRM

---

## Forecasting

### Forecast Categories

RevOps Hub uses standard forecast categories:

| Category | Definition | Typical Stage |
|----------|------------|---------------|
| **Pipeline** | In active pursuit | Early stages |
| **Best Case** | Could close this period | Mid stages |
| **Commit** | High confidence close | Late stages |
| **Closed** | Deal won | Closed Won |
| **Omitted** | Not counting | Closed Lost, On Hold |

### Understanding Coverage

**Pipeline Coverage** = Total Pipeline ÷ Quota

- **3x or higher**: Strong coverage, healthy funnel
- **2-3x**: Adequate, watch for slippage
- **Under 2x**: At risk, need more pipeline

### Forecast Trends

The trend chart shows how your forecast has changed:

- **Increasing**: Pipeline growing or deals advancing
- **Flat**: Stable forecast
- **Decreasing**: Deals slipping or being lost

### Weekly Forecast Review

Use the forecast comparison to track week-over-week changes:

1. **Ups**: Deals that improved (new, upgraded, advanced)
2. **Downs**: Deals that degraded (pushed, reduced, lost)
3. **Net Change**: Overall movement

---

## Coaching & Performance

### Rep Performance Dashboard

*(Available on Growth and Enterprise tiers)*

View performance metrics for your team:

#### Key Metrics

- **Quota Attainment**: Closed-won vs. quota
- **Pipeline Coverage**: Open pipeline vs. remaining quota
- **Win Rate**: Won deals ÷ total closed
- **Avg Deal Size**: Average closed-won amount
- **Cycle Time**: Days from creation to close

### Performance Drivers

The drivers chart shows which behaviors correlate with success:

- **Activity Volume**: Total activities logged
- **Meeting Frequency**: Customer meetings per deal
- **Response Time**: Speed of follow-up
- **Multi-threading**: Contacts per deal

### Coaching Insights

AI-generated insights highlight:

1. **Strengths**: What the rep does well
2. **Opportunities**: Areas for improvement
3. **Recommendations**: Specific actions to take

---

## AI-Powered Insights

### Deal Scoring

*(Available on Growth and Enterprise tiers)*

Each deal receives an AI-generated win probability:

| Score | Meaning |
|-------|---------|
| 70%+ | High probability - prioritize closing |
| 40-69% | Medium - needs attention |
| Under 40% | Low - identify blockers |

#### Score Factors

The model considers:
- Historical patterns from similar deals
- Current engagement levels
- Rep track record
- Account history
- Stage progression speed

### Next-Best-Actions

AI recommendations appear on your dashboard:

#### Action Types

- **Engagement**: Schedule meeting, follow up
- **Progression**: Send proposal, advance stage
- **Strategy**: Multi-thread, get executive sponsor
- **Hygiene**: Update close date, add next step
- **Rescue**: Re-engage stale deal

#### Prioritization

Actions are prioritized by:
1. **Impact**: Potential revenue affected
2. **Urgency**: Time sensitivity
3. **Probability**: Likelihood of success

### Churn Predictions

*(Enterprise tier only)*

Identifies customers at risk of churning:

| Risk Level | Action Required |
|------------|-----------------|
| Critical | Executive escalation within 24 hours |
| High | Schedule QBR within 1 week |
| Medium | Proactive outreach |
| Low | Maintain regular cadence |

---

## Alerts & Notifications

### Alert Types

| Alert | Severity | Meaning |
|-------|----------|---------|
| Build Failure | Critical | Data pipeline failed |
| Stale Pipeline | Warning | Too many inactive deals |
| Data Quality | Warning | Data issues detected |
| Sync Delay | Warning | CRM sync behind schedule |
| Forecast Miss | Warning | Significant forecast variance |

### Managing Alerts

1. Click the bell icon to view active alerts
2. Acknowledge alerts to mark them as seen
3. Resolve alerts when the issue is fixed

### Notification Settings

Configure how you receive alerts:

- **Email**: Daily digest or immediate
- **Slack**: Channel or direct message
- **In-app**: Badge and notification center

---

## Best Practices

### Daily Workflow

1. **Check Dashboard**: Review KPIs and health status
2. **Address Alerts**: Handle any urgent issues
3. **Work Actions**: Complete AI-recommended tasks
4. **Update Deals**: Keep CRM current

### Weekly Routine

1. **Forecast Review**: Update deal forecasts
2. **Pipeline Scrub**: Address hygiene issues
3. **Team Coaching**: Review rep performance
4. **Planning**: Identify gaps and actions

### Tips for Success

#### For Sales Reps

- Log all activities in CRM
- Keep next steps updated
- Engage multiple contacts per deal
- Update close dates proactively

#### For Sales Managers

- Review team health daily
- Use coaching insights for 1:1s
- Address critical deals immediately
- Celebrate wins and improvements

#### For RevOps

- Monitor data quality metrics
- Configure alerts appropriately
- Adjust thresholds for your business
- Share insights with leadership

---

## Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `G + H` | Go to Home |
| `G + P` | Go to Pipeline |
| `G + F` | Go to Forecast |
| `G + C` | Go to Coaching |
| `?` | Show shortcuts |
| `/` | Search |

---

## Getting Help

### In-App Help

- Click the `?` icon for contextual help
- Use the search bar to find topics

### Support

- **Email**: support@revops-hub.io
- **Slack**: #revops-help (if configured)
- **Documentation**: docs.revops-hub.io

### Feedback

We love hearing from users! Share feedback:
- Use the feedback button in-app
- Email product@revops-hub.io

---

## Glossary

| Term | Definition |
|------|------------|
| **ARR** | Annual Recurring Revenue |
| **Coverage** | Pipeline value relative to quota |
| **Health Score** | AI-calculated deal quality (0-100) |
| **Hygiene** | Data quality and completeness |
| **Multi-threading** | Engaging multiple contacts |
| **NBA** | Next-Best-Action recommendation |
| **Pipeline** | Total value of open opportunities |
| **Quota** | Sales target for a period |
| **Win Rate** | Percentage of deals won |
