# RevOps Command Center - Web Application

A modern React dashboard for Revenue Operations intelligence, built with Next.js 14, TypeScript, and Tailwind CSS.

## Features

- **Pipeline Health Dashboard** - Executive view of forecast, deal health, and bottlenecks
- **Rep Coaching Dashboard** - Individual performance analysis with AI-powered insights
- **Real-time Data** - Connects to Palantir Foundry for live data (or uses mock data in development)
- **Dark Theme** - Professional dark mode design optimized for all-day use
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

### Development (Mock Data)

By default, the app uses mock data. No configuration needed.

### Production (Live Foundry Data)

Create a `.env.local` file:

```env
NEXT_PUBLIC_FOUNDRY_URL=https://your-stack.palantirfoundry.com
NEXT_PUBLIC_FOUNDRY_TOKEN=your-api-token
NEXT_PUBLIC_USE_MOCK_DATA=false
```

The app will automatically switch to fetching live data from Foundry.

### Required Foundry Datasets

The app expects these datasets to exist:

| Dataset Path | Description |
|-------------|-------------|
| `/RevOps/Dashboard/kpis` | Pre-computed KPI values |
| `/RevOps/Analytics/forecast_history` | Weekly forecast snapshots |
| `/RevOps/Analytics/stuck_deals_view` | Deals requiring attention |
| `/RevOps/Analytics/health_distribution` | Pipeline health breakdown |
| `/RevOps/Analytics/rep_performance_view` | Rep metrics |
| `/RevOps/Dashboard/next_best_actions` | Prioritized actions |

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
