# RevOps Hub

A comprehensive Revenue Operations intelligence platform built on Palantir Foundry, demonstrating advanced sales analytics, deal health scoring, process mining, and AI-powered insights.

## Project Overview

This project transforms raw CRM data into actionable intelligence for sales leadership. Instead of passive dashboards that require drilling, it **pushes** insights to stakeholders before they know to ask.

### Key Capabilities

1. **Deal Health Scoring** - Composite health scores based on velocity, activity, stakeholders, competition, and discount levels
2. **Forecast Confidence** - Realistic forecast assessments adjusted for deal health
3. **Rep Performance Analytics** - Pattern analysis identifying what top performers do differently
4. **Process Mining** - Bottleneck detection and cycle time analysis
5. **AIP Agents** - Natural language interfaces for Deal Risk, Forecasting, Coaching, and Briefings

## Demo Scenario: Q4 Crunch Time

The included scenario data tells the story of TechFlow Solutions fighting to close Q4:

- **$13M target**, currently at $8.2M closed with $4.8M to go
- **Hero Deals** with names and drama (Acme Corporation going dark, GlobalTech on track)
- **Rep Performance** patterns (Jessica's 4.2 stakeholders vs Kevin's 1.3)
- **Process Bottlenecks** (Legal Review at 12 days vs 5 day target)
- **Competitive Pressure** (TechRival winning on price in Mid-Market)

## Web Application

The project includes a modern React dashboard built with Next.js 14, TypeScript, and Tailwind CSS.

### Quick Start

```bash
cd webapp
npm install
npm run dev
# Open http://localhost:3000
```

### Features
- **Pipeline Health Dashboard** - Executive view with KPIs, forecast trends, stuck deals
- **Rep Coaching Dashboard** - Individual performance analysis with AI insights
- **Dark Theme** - Professional design for all-day use
- **Foundry Integration** - Connects to live Foundry data or uses mock data

See [webapp/README.md](webapp/README.md) for full documentation.

## Project Structure

```
revops-hub/
├── webapp/                     # React dashboard application
│   ├── src/
│   │   ├── app/               # Next.js pages
│   │   ├── components/        # Reusable UI components
│   │   ├── lib/               # Utilities and Foundry API
│   │   └── data/              # Mock data
│   └── package.json
├── transforms/
│   ├── scenario/               # Story-driven sample data generators
│   │   ├── generate_scenario_opportunities.py
│   │   ├── generate_scenario_reps.py
│   │   └── generate_process_events.py
│   ├── reference/              # Reference data (benchmarks, configs)
│   │   └── stage_benchmarks.py
│   ├── enrichment/             # Data enrichment transforms
│   │   └── calculate_deal_health.py
│   └── analytics/              # Analytical transforms
│       ├── pipeline_health_summary.py
│       ├── forecast_confidence.py
│       ├── rep_performance_analytics.py
│       └── process_bottleneck_analytics.py
├── aip/
│   └── agents.yaml             # AIP Agent Studio configurations
├── docs/
│   ├── SCENARIO_DESIGN.md      # Full scenario narrative and design
│   ├── DEMO_QUERIES.sql        # SQL queries for live demos
│   └── DEMO_SCRIPT.md          # Complete demo walkthrough script
└── README.md
```

## Getting Started

### 1. Deploy Transforms to Foundry

Upload the transforms to a Code Repository in Foundry:

```
Repository: /RevOps/Transforms
```

### 2. Build Scenario Data

Build transforms in this order:

1. `/RevOps/Reference/stage_benchmarks`
2. `/RevOps/Scenario/opportunities`
3. `/RevOps/Scenario/sales_reps`
4. `/RevOps/Scenario/process_events`

### 3. Build Enrichment & Analytics

5. `/RevOps/Enriched/deal_health_scores`
6. `/RevOps/Analytics/pipeline_health_summary`
7. `/RevOps/Analytics/forecast_confidence`
8. `/RevOps/Analytics/swing_deals`
9. `/RevOps/Analytics/rep_performance`
10. `/RevOps/Analytics/top_performer_patterns`
11. `/RevOps/Analytics/process_bottlenecks`
12. `/RevOps/Analytics/deals_stuck_in_process`
13. `/RevOps/Analytics/rework_analysis`

### 4. Run Demo Queries

Open Quiver or SQL workbench and run queries from `docs/DEMO_QUERIES.sql`:

```sql
-- Executive View: Q4 Forecast Summary
SELECT
    SUM(CASE WHEN stage_name = 'Closed Won' THEN amount ELSE 0 END) as closed_won,
    SUM(CASE WHEN forecast_category = 'Commit' THEN amount ELSE 0 END) as commit
FROM `/RevOps/Scenario/opportunities`
WHERE close_date >= '2024-10-01' AND close_date <= '2024-12-31';
```

### 5. Configure AIP Agents

Copy prompts from `aip/agents.yaml` into AIP Agent Studio:

- **Deal Risk Analyzer** - Identifies at-risk deals with specific actions
- **Forecast Confidence Advisor** - Realistic forecast assessments
- **Rep Coaching Assistant** - Identifies coaching opportunities
- **Morning Briefing** - Daily personalized briefings
- **Competitive Intelligence** - Win/loss pattern analysis
- **Process Analyzer** - Bottleneck detection

## Key Insights Demonstrated

### The Stakeholder Insight

```
Jessica Taylor: 38% win rate, 4.2 avg stakeholders
Kevin Brown: 15% win rate, 1.3 avg stakeholders

Deals with 4+ stakeholders: 45% win rate
Deals with 1 stakeholder: 12% win rate

Multi-threading is a 3.7x improvement.
```

### The Legal Bottleneck

```
Legal Review: 12 days avg (target: 5 days)
6 deals stuck, $2.8M delayed
Root cause: 2 people handling 15 deals/week
```

### The Acme Crisis

```
Acme Corporation: $1.2M, Health 48 (was 78)
- Champion gone dark (8 days no contact)
- Single-threaded (2 stakeholders)
- Stuck in Negotiation (18 days)
Action: CRO executive outreach by Tuesday
```

## Demo Flow

See `docs/DEMO_SCRIPT.md` for a complete 25-minute demo walkthrough:

1. **Executive View** (5 min) - Forecast summary, win rates
2. **Deal Crisis** (7 min) - Acme reveal, commit at risk
3. **Pattern Discovery** (6 min) - Jessica vs Kevin, stakeholder correlation
4. **Competitive Intel** (4 min) - TechRival analysis
5. **Process Bottlenecks** (4 min) - Legal review delays
6. **AIP Agents** (4 min) - Natural language demo

## Technical Details

### Health Score Calculation

```python
health_score = (
    velocity_score * 0.25 +      # Days in stage vs benchmark
    activity_score * 0.25 +      # Days since last activity
    stakeholder_score * 0.25 +   # Multi-threading level
    competition_score * 0.15 +   # Competitive involvement
    discount_score * 0.10        # Discount level
)
```

### Health Categories

| Score | Category | Action |
|-------|----------|--------|
| 80-100 | Healthy | Continue approach |
| 60-79 | Monitor | Watch closely |
| 40-59 | At Risk | Manager intervention |
| 0-39 | Critical | Executive involvement |

### Process Mining Events

Each opportunity generates events showing:
- Stage transitions with timestamps
- Duration vs benchmark
- Bottleneck flags
- Rework patterns

## Extending the Solution

### Connect Real Salesforce Data

Replace scenario generators with Salesforce sync:

```python
@transform(
    salesforce_opps=Input("/Salesforce/Opportunity"),
    output=Output("/RevOps/Staging/opportunities")
)
def sync_opportunities(ctx, salesforce_opps, output):
    # Map Salesforce fields to RevOps schema
    ...
```

### Add Custom Health Components

Modify `calculate_deal_health.py` to add company-specific risk factors:
- Product fit scoring
- Budget confirmation status
- Procurement timeline

### Build Workshop Dashboards

Use the analytics datasets to build Workshop apps:
- Executive command center
- Rep coaching views
- Pipeline health monitor

## License

Portfolio demonstration project. All code provided as-is for educational purposes.
