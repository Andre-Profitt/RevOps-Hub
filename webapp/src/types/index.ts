// Dashboard KPIs
export interface DashboardKPIs {
  quarterTarget: number
  closedWonAmount: number
  aiForecast: number
  aiForecastConfidence: number
  commitAmount: number
  commitConfidence: number
  gapToTarget: number
  gapPct: number
  coverageRatio: number
  riskLevel: 'Low' | 'Medium' | 'High' | 'Critical'
  forecastChange1w: number
  commitChange1w: number
}

// Forecast trend data point
export interface ForecastTrendPoint {
  week: string
  weekNumber: number
  target: number
  closed: number
  commit: number
  forecast: number
}

// Stage velocity
export interface StageVelocity {
  stageName: string
  stageOrder: number
  avgDuration: number
  benchmark: number
  deviation: number
  trendDirection: 'improving' | 'stable' | 'worsening'
  conversionRate: number
  dealsInStage: number
  amountInStage: number
}

// Pipeline funnel stage
export interface FunnelStage {
  name: string
  amount: number
  count: number
  conversionRate?: number
}

// Stuck deal
export interface StuckDeal {
  id: string
  accountName: string
  opportunityName: string
  ownerName: string
  amount: number
  stageName: string
  daysInStage: number
  healthScore: number
  closeDate: string
  urgency: 'CRITICAL' | 'HIGH' | 'MEDIUM' | 'LOW'
  riskFlags: string[]
  primaryAction: 'call' | 'email' | 'meeting' | 'escalate'
}

// Health distribution
export interface HealthDistribution {
  healthy: { count: number; amount: number; pct: number }
  monitor: { count: number; amount: number; pct: number }
  atRisk: { count: number; amount: number; pct: number }
  critical: { count: number; amount: number; pct: number }
  trend: number // Change in at-risk % vs last week
}

// Sales rep performance
export interface RepPerformance {
  repId: string
  repName: string
  managerName: string
  region: string
  segment: string

  // Core metrics
  ytdAttainment: number
  winRate: number
  avgDealSize: number
  avgSalesCycle: number

  // Rankings
  overallRank: number
  totalReps: number
  rankChange: number

  // Team comparisons
  teamAvgWinRate: number
  teamAvgDealSize: number
  teamAvgCycle: number

  // Tier
  performanceTier: 'Top Performer' | 'On Track' | 'Needs Improvement' | 'At Risk'
}

// Win rate driver comparison
export interface DriverComparison {
  metricName: string
  metricLabel: string
  repValue: number
  topPerformerValue: number
  teamAvgValue: number
  format: 'decimal' | 'percent' | 'days'
  goodDirection: 'higher' | 'lower'
  gapToTop: number
  gapImpact: string
}

// Activity metrics
export interface ActivityMetrics {
  totalActivities: number
  calls: number
  emails: number
  meetings: number
  demos: number
  avgDaily: number
  teamAvgDaily: number
  topPerformerDaily: number
  dailyTrend: { day: string; count: number }[]
}

// Next best action
export interface NextBestAction {
  actionId: string
  opportunityId: string
  accountName: string
  contactName: string
  actionType: 'call' | 'email' | 'meeting' | 'task'
  actionReason: string
  priorityRank: number
  urgency: 'today' | 'this_week' | 'soon'
  dealAmount: number
  dealHealth: number
  bestTime: string
}

// AI Coaching insight
export interface CoachingInsight {
  type: 'strength' | 'improvement' | 'action'
  title: string
  detail: string
  metric?: string
  comparison?: string
  action?: string
  dealName?: string
  dealAmount?: number
}

// Deal for pipeline review
export interface PipelineDeal {
  id: string
  accountName: string
  amount: number
  healthScore: number
  primaryAction: 'call' | 'email' | 'meeting'
}

// Sales rep for selector
export interface SalesRep {
  id: string
  name: string
  team: string
  region?: string
  segment?: string
}

// =====================================================
// Foundry Schema Mapping Reference
// =====================================================
// The Foundry transforms output snake_case field names.
// The foundry.ts fetchers map these to camelCase for React.
//
// Example mappings:
//   quarter_target -> quarterTarget
//   closed_won_amount -> closedWonAmount
//   ai_forecast -> aiForecast
//   health_score_override -> healthScore
//   stage_name -> stageName
