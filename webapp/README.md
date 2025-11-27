# RevOps Command Center - Web Application

A modern React dashboard for Revenue Operations intelligence, built with Next.js 14, TypeScript, and Tailwind CSS.

## Features

### Core Dashboards
- **Pipeline Health Dashboard** - Executive view of forecast, deal health, and bottlenecks
- **Rep Coaching Dashboard** - Individual performance analysis with AI-powered insights
- **QBR Dashboard** - Quarterly business review with executive summary

### Planning & Analysis
- **Territory & Account Planning** - Propensity scoring and white space analysis
- **Capacity & Hiring Planning** - Rep capacity, ramp analysis, and hiring ROI modeling
- **Scenario Modeling** - What-if analysis for win rate, deal size, and cycle time
- **Customer Health & Expansion** - Churn risk and expansion opportunity tracking

### Operations
- **Forecasting Hub** - Multi-methodology forecast comparison (AI, bottom-up, historical)
- **Win/Loss Analysis** - Competitive intelligence and loss pattern analysis
- **Deal Desk** - Approval workflows and discount management
- **Compensation & Attainment** - Quota tracking and commission calculations
- **Data Quality Admin** - Data completeness, validity, and sync monitoring
- **Alerts & Notifications** - Configurable alert rules with severity tracking

### Technical
- **Real-time Data** - Connects to Palantir Foundry for live data (or uses mock data in development)
- **Secure Architecture** - Server-side API proxy keeps credentials secure
- **Responsive** - Works on desktop, tablet, and mobile

## Quick Start

```bash
# Install dependencies
npm install

# Run development server
npm run dev

# Open http://localhost:3000
```

## Project Structure

```
webapp/
├── src/
│   ├── app/                    # Next.js pages
│   │   ├── page.tsx           # Pipeline Health Dashboard
│   │   ├── coaching/
│   │   │   └── page.tsx       # Rep Coaching Dashboard
│   │   ├── layout.tsx         # Root layout
│   │   └── globals.css        # Global styles
│   ├── components/
│   │   ├── ui/                # Reusable UI components
│   │   │   ├── KPICard.tsx
│   │   │   ├── HealthIndicator.tsx
│   │   │   ├── DataTable.tsx
│   │   │   ├── ComparisonBars.tsx
│   │   │   └── AIInsightsCard.tsx
│   │   └── charts/            # Chart components
│   │       ├── ForecastTrendChart.tsx
│   │       ├── HealthDistributionChart.tsx
│   │       └── PipelineFunnel.tsx
│   ├── lib/
│   │   ├── utils.ts           # Utility functions
│   │   └── foundry.ts         # Foundry API integration
│   ├── data/
│   │   └── mockData.ts        # Mock data for development
│   └── types/
│       └── index.ts           # TypeScript types
├── package.json
├── tailwind.config.ts
└── tsconfig.json
```

## Connecting to Foundry

### Architecture

The app uses a **server-side proxy** (`/api/foundry`) to communicate with Foundry. This keeps your API credentials secure—they never leave the server and are never exposed to browser JavaScript.

```
Browser → /api/foundry (Next.js API Route) → Palantir Foundry
                ↑
        Credentials stay here (server-only)
```

### Development (Mock Data)

By default, the app uses mock data. No configuration needed:

```bash
npm run dev
# Opens at http://localhost:3000 with mock data
```

### Production (Live Foundry Data)

Create a `.env.local` file with **server-only** environment variables (no `NEXT_PUBLIC_` prefix):

```env
# Server-only credentials (NEVER use NEXT_PUBLIC_ for secrets!)
FOUNDRY_URL=https://your-stack.palantirfoundry.com
FOUNDRY_TOKEN=your-api-token

# Client-side flag to enable live mode
NEXT_PUBLIC_USE_MOCK_DATA=false
```

> ⚠️ **Security Note**: Using `NEXT_PUBLIC_FOUNDRY_TOKEN` would expose your credentials to every browser session. Always use server-only env vars for secrets.

The app will automatically switch to fetching live data through the `/api/foundry` proxy.

### Required Foundry Datasets

The app queries 60+ datasets organized by domain. Each domain module in `src/lib/foundry/` documents its specific requirements.

#### Dashboard & Pipeline (`src/lib/foundry/pipeline.ts`)
| Dataset Path | Description |
|-------------|-------------|
| `/RevOps/Dashboard/kpis` | Pre-computed KPI values (quota, forecast, commit) |
| `/RevOps/Analytics/forecast_history` | Weekly forecast snapshots for trend chart |
| `/RevOps/Pipeline/stuck_deals` | Deals requiring attention with risk flags |
| `/RevOps/Analytics/health_distribution` | Pipeline health breakdown by category |
| `/RevOps/Analytics/bottlenecks` | Process bottleneck analysis |
| `/RevOps/Analytics/competitive_losses` | Competitive loss tracking |
| `/RevOps/Analytics/stage_velocity` | Stage duration and conversion metrics |
| `/RevOps/Analytics/pipeline_funnel` | Funnel visualization data |

#### Hygiene (`src/lib/foundry/hygiene.ts`)
| Dataset Path | Description |
|-------------|-------------|
| `/RevOps/Hygiene/alerts` | Hygiene alerts by opportunity |
| `/RevOps/Hygiene/summary` | Aggregate hygiene metrics |
| `/RevOps/Hygiene/by_owner` | Hygiene breakdown by rep |
| `/RevOps/Hygiene/trends` | Historical hygiene trends |

#### AI Predictions (`src/lib/foundry/predictions.ts`)
| Dataset Path | Description |
|-------------|-------------|
| `/RevOps/AI/deal_predictions` | ML win probability and slip risk |
| `/RevOps/AI/leading_indicators_summary` | Aggregate leading indicators |

#### Telemetry (`src/lib/foundry/telemetry.ts`)
| Dataset Path | Description |
|-------------|-------------|
| `/RevOps/Telemetry/summary` | Cross-functional metrics summary |
| `/RevOps/Telemetry/funnel_handoffs` | Marketing→Sales handoff rates |
| `/RevOps/Telemetry/cross_team_activity` | Multi-team engagement per deal |

#### Rep Coaching (`src/lib/foundry/coaching.ts`)
| Dataset Path | Description |
|-------------|-------------|
| `/RevOps/Coaching/rep_performance` | Individual rep metrics |
| `/RevOps/Coaching/driver_comparisons` | Performance driver analysis |
| `/RevOps/Coaching/activity_metrics` | Activity volume and trends |
| `/RevOps/Coaching/next_best_actions` | AI-prioritized actions |
| `/RevOps/Coaching/coaching_insights` | AI coaching recommendations |

#### QBR (`src/lib/foundry/qbr.ts`)
| Dataset Path | Description |
|-------------|-------------|
| `/RevOps/QBR/executive_summary` | Quarterly executive summary |
| `/RevOps/QBR/rep_performance` | Rep performance for quarter |
| `/RevOps/QBR/win_loss_analysis` | Quarterly win/loss breakdown |

#### Territory (`src/lib/foundry/territory.ts`)
| Dataset Path | Description |
|-------------|-------------|
| `/RevOps/Territory/summary` | Territory planning summary |
| `/RevOps/Territory/account_scores` | Propensity scores by account |
| `/RevOps/Territory/balance` | Territory workload balance |
| `/RevOps/Territory/white_space` | White space opportunities |

#### Capacity (`src/lib/foundry/capacity.ts`)
| Dataset Path | Description |
|-------------|-------------|
| `/RevOps/Capacity/summary` | Capacity planning overview |
| `/RevOps/Capacity/rep_capacity` | Individual rep capacity |
| `/RevOps/Capacity/team_summary` | Team-level capacity |
| `/RevOps/Capacity/ramp_cohorts` | Ramp cohort analysis |

#### Scenarios (`src/lib/foundry/scenarios.ts`)
| Dataset Path | Description |
|-------------|-------------|
| `/RevOps/Scenarios/summary` | Scenario modeling summary |
| `/RevOps/Scenarios/win_rate` | Win rate improvement scenarios |
| `/RevOps/Scenarios/deal_size` | Deal size improvement scenarios |
| `/RevOps/Scenarios/cycle_time` | Cycle time reduction scenarios |

#### Customer Health (`src/lib/foundry/customers.ts`)
| Dataset Path | Description |
|-------------|-------------|
| `/RevOps/Customers/health_summary` | Customer health overview |
| `/RevOps/Customers/health` | Individual customer health scores |
| `/RevOps/Customers/expansion_opportunities` | Expansion pipeline |
| `/RevOps/Customers/churn_risk` | Churn risk accounts |

#### Forecasting (`src/lib/foundry/forecasting.ts`)
| Dataset Path | Description |
|-------------|-------------|
| `/RevOps/Forecast/summary` | Multi-method forecast summary |
| `/RevOps/Forecast/by_segment` | Forecast breakdown by segment |
| `/RevOps/Forecast/history` | Historical forecast snapshots |
| `/RevOps/Forecast/accuracy` | Forecast accuracy by method |

#### Win/Loss (`src/lib/foundry/winloss.ts`)
| Dataset Path | Description |
|-------------|-------------|
| `/RevOps/WinLoss/summary` | Win/loss summary metrics |
| `/RevOps/WinLoss/loss_reasons` | Loss reason analysis |
| `/RevOps/WinLoss/win_factors` | Win factor correlation |
| `/RevOps/WinLoss/competitive_battles` | Competitive win rates |
| `/RevOps/WinLoss/by_segment` | Win/loss by segment |

#### Deal Desk (`src/lib/foundry/dealdesk.ts`)
| Dataset Path | Description |
|-------------|-------------|
| `/RevOps/DealDesk/summary` | Approval queue summary |
| `/RevOps/DealDesk/approvals` | Pending approvals |
| `/RevOps/DealDesk/discount_analysis` | Discount patterns |

#### Compensation (`src/lib/foundry/compensation.ts`)
| Dataset Path | Description |
|-------------|-------------|
| `/RevOps/Compensation/summary` | Attainment summary |
| `/RevOps/Compensation/rep_attainment` | Individual attainment |
| `/RevOps/Compensation/attainment_trend` | Historical attainment |

#### Data Quality (`src/lib/foundry/data-quality.ts`)
| Dataset Path | Description |
|-------------|-------------|
| `/RevOps/DataQuality/summary` | Data quality overview |
| `/RevOps/DataQuality/metrics` | Quality metrics by field |
| `/RevOps/DataQuality/sync_status` | Integration sync status |

#### Alerts (`src/lib/foundry/alerts.ts`)
| Dataset Path | Description |
|-------------|-------------|
| `/RevOps/Alerts/summary` | Alert counts by severity |
| `/RevOps/Alerts/active` | Active alerts |
| `/RevOps/Alerts/rules` | Alert rule definitions |

## Customization

### Colors

Edit `tailwind.config.ts` to modify the color palette:

```typescript
colors: {
  'status-healthy': '#22c55e',
  'status-monitor': '#eab308',
  'status-at-risk': '#f97316',
  'status-critical': '#ef4444',
  // ...
}
```

### Health Score Thresholds

Edit `src/lib/utils.ts`:

```typescript
export function getHealthCategory(score: number) {
  if (score >= 80) return 'healthy'
  if (score >= 60) return 'monitor'
  if (score >= 40) return 'at-risk'
  return 'critical'
}
```

## Tech Stack

- **Framework**: Next.js 14 (App Router)
- **Language**: TypeScript
- **Styling**: Tailwind CSS
- **Charts**: Recharts
- **Icons**: Lucide React
- **State**: React hooks (useState, useEffect)

## Scripts

| Command | Description |
|---------|-------------|
| `npm run dev` | Start development server |
| `npm run build` | Build for production |
| `npm run start` | Start production server |
| `npm run lint` | Run ESLint |
| `npm run type-check` | Run TypeScript compiler check |

## Deployment

### Vercel (Recommended)

```bash
npm install -g vercel
vercel
```

### Docker

```dockerfile
FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build
CMD ["npm", "start"]
```

### Static Export

For static hosting (GitHub Pages, S3, etc.):

```bash
# Add to next.config.js
output: 'export'

# Build
npm run build
# Output in /out directory
```

## Screenshots

### Pipeline Health Dashboard
- KPI cards with trend indicators
- Forecast vs Actuals trend chart
- Pipeline funnel visualization
- Stuck deals table with health scores
- Health distribution breakdown

### Rep Coaching Dashboard
- Performance summary metrics
- Win rate driver comparisons
- Activity analysis with trends
- AI coaching insights
- Next best actions queue

## License

Portfolio demonstration project.
