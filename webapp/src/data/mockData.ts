import type {
  DashboardKPIs,
  ForecastTrendPoint,
  StageVelocity,
  FunnelStage,
  StuckDeal,
  HealthDistribution,
  RepPerformance,
  DriverComparison,
  ActivityMetrics,
  NextBestAction,
  CoachingInsight,
  PipelineDeal,
} from '@/types'

// =====================================================
// PIPELINE HEALTH DASHBOARD DATA
// =====================================================

export const dashboardKPIs: DashboardKPIs = {
  quarterTarget: 13000000,
  closedWonAmount: 8200000,
  aiForecast: 12400000,
  aiForecastConfidence: 0.71,
  commitAmount: 4200000,
  commitConfidence: 0.85,
  gapToTarget: -600000,
  gapPct: 0.05,
  coverageRatio: 2.1,
  riskLevel: 'Medium',
  forecastChange1w: 0.02,
  commitChange1w: -0.03,
}

export const forecastTrend: ForecastTrendPoint[] = [
  { week: 'W1', weekNumber: 1, target: 1000000, closed: 0, commit: 4500000, forecast: 3800000 },
  { week: 'W2', weekNumber: 2, target: 2000000, closed: 450000, commit: 5200000, forecast: 4500000 },
  { week: 'W3', weekNumber: 3, target: 3000000, closed: 1200000, commit: 5800000, forecast: 5200000 },
  { week: 'W4', weekNumber: 4, target: 4000000, closed: 2100000, commit: 6200000, forecast: 6000000 },
  { week: 'W5', weekNumber: 5, target: 5500000, closed: 3500000, commit: 6800000, forecast: 7200000 },
  { week: 'W6', weekNumber: 6, target: 7000000, closed: 5200000, commit: 7400000, forecast: 8500000 },
  { week: 'W7', weekNumber: 7, target: 8500000, closed: 8200000, commit: 8200000, forecast: 9800000 },
  { week: 'W8', weekNumber: 8, target: 10000000, closed: 8200000, commit: 9500000, forecast: 11000000 },
  { week: 'W9', weekNumber: 9, target: 11500000, closed: 8200000, commit: 10500000, forecast: 11800000 },
  { week: 'W10', weekNumber: 10, target: 13000000, closed: 8200000, commit: 11200000, forecast: 12400000 },
]

export const stageVelocity: StageVelocity[] = [
  { stageName: 'Qualification', stageOrder: 1, avgDuration: 8, benchmark: 7, deviation: 1, trendDirection: 'stable', conversionRate: 0.68, dealsInStage: 24, amountInStage: 3200000 },
  { stageName: 'Discovery', stageOrder: 2, avgDuration: 14, benchmark: 12, deviation: 2, trendDirection: 'worsening', conversionRate: 0.65, dealsInStage: 18, amountInStage: 2800000 },
  { stageName: 'Solution', stageOrder: 3, avgDuration: 21, benchmark: 18, deviation: 3, trendDirection: 'worsening', conversionRate: 0.64, dealsInStage: 12, amountInStage: 2100000 },
  { stageName: 'Proposal', stageOrder: 4, avgDuration: 11, benchmark: 10, deviation: 1, trendDirection: 'stable', conversionRate: 0.73, dealsInStage: 8, amountInStage: 1600000 },
  { stageName: 'Negotiation', stageOrder: 5, avgDuration: 18, benchmark: 14, deviation: 4, trendDirection: 'worsening', conversionRate: 0.45, dealsInStage: 6, amountInStage: 1200000 },
]

export const pipelineFunnel: FunnelStage[] = [
  { name: 'Qualification', amount: 18200000, count: 45, conversionRate: 0.68 },
  { name: 'Discovery', amount: 12400000, count: 32, conversionRate: 0.65 },
  { name: 'Solution', amount: 8100000, count: 21, conversionRate: 0.64 },
  { name: 'Proposal', amount: 5200000, count: 14, conversionRate: 0.73 },
  { name: 'Negotiation', amount: 3800000, count: 8, conversionRate: 0.45 },
]

export const stuckDeals: StuckDeal[] = [
  {
    id: 'OPP-001',
    accountName: 'Acme Corporation',
    opportunityName: 'Enterprise Platform Deal',
    ownerName: 'Sarah Chen',
    amount: 1200000,
    stageName: 'Negotiation',
    daysInStage: 18,
    healthScore: 48,
    closeDate: '2024-12-15',
    urgency: 'CRITICAL',
    riskFlags: ['Gone Dark', 'Single-threaded', 'Stalled'],
    primaryAction: 'call',
  },
  {
    id: 'OPP-002',
    accountName: 'GlobalTech Industries',
    opportunityName: 'Digital Transformation',
    ownerName: 'David Kim',
    amount: 890000,
    stageName: 'Solution Design',
    daysInStage: 35,
    healthScore: 52,
    closeDate: '2024-12-20',
    urgency: 'HIGH',
    riskFlags: ['Going Dark', 'Long Cycle'],
    primaryAction: 'meeting',
  },
  {
    id: 'OPP-003',
    accountName: 'CloudFirst Enterprises',
    opportunityName: 'Cloud Migration',
    ownerName: 'Emily Rodriguez',
    amount: 650000,
    stageName: 'Proposal',
    daysInStage: 28,
    healthScore: 55,
    closeDate: '2024-12-18',
    urgency: 'HIGH',
    riskFlags: ['Competitor', 'No Champion'],
    primaryAction: 'call',
  },
  {
    id: 'OPP-004',
    accountName: 'TechVentures Inc',
    opportunityName: 'Analytics Platform',
    ownerName: 'Sarah Chen',
    amount: 420000,
    stageName: 'Discovery',
    daysInStage: 31,
    healthScore: 61,
    closeDate: '2024-12-30',
    urgency: 'MEDIUM',
    riskFlags: ['No Champion'],
    primaryAction: 'email',
  },
  {
    id: 'OPP-005',
    accountName: 'DataSystems Pro',
    opportunityName: 'Data Warehouse',
    ownerName: 'James Wilson',
    amount: 280000,
    stageName: 'Solution Design',
    daysInStage: 25,
    healthScore: 58,
    closeDate: '2024-12-22',
    urgency: 'MEDIUM',
    riskFlags: ['Heavy Discount'],
    primaryAction: 'meeting',
  },
]

export const healthDistribution: HealthDistribution = {
  healthy: { count: 18, amount: 4500000, pct: 45 },
  monitor: { count: 12, amount: 3200000, pct: 32 },
  atRisk: { count: 6, amount: 1500000, pct: 15 },
  critical: { count: 3, amount: 800000, pct: 8 },
  trend: 3, // 3% more at-risk vs last week
}

// =====================================================
// REP COACHING DASHBOARD DATA
// =====================================================

export const repPerformance: RepPerformance = {
  repId: 'REP-001',
  repName: 'Sarah Williams',
  managerName: 'Michael Torres',
  region: 'West',
  segment: 'Enterprise',
  ytdAttainment: 0.78,
  winRate: 0.32,
  avgDealSize: 145000,
  avgSalesCycle: 68,
  overallRank: 4,
  totalReps: 12,
  rankChange: 2,
  teamAvgWinRate: 0.28,
  teamAvgDealSize: 125000,
  teamAvgCycle: 72,
  performanceTier: 'On Track',
}

export const driverComparisons: DriverComparison[] = [
  {
    metricName: 'stakeholders',
    metricLabel: 'Stakeholders Engaged',
    repValue: 3.2,
    topPerformerValue: 4.8,
    teamAvgValue: 2.8,
    format: 'decimal',
    goodDirection: 'higher',
    gapToTop: 1.6,
    gapImpact: '-15% win rate impact',
  },
  {
    metricName: 'daysToMeeting',
    metricLabel: 'Days to First Meeting',
    repValue: 4.2,
    topPerformerValue: 2.1,
    teamAvgValue: 3.8,
    format: 'days',
    goodDirection: 'lower',
    gapToTop: -2.1,
    gapImpact: 'Slower engagement',
  },
  {
    metricName: 'championRate',
    metricLabel: 'Champion Identified',
    repValue: 0.65,
    topPerformerValue: 0.85,
    teamAvgValue: 0.58,
    format: 'percent',
    goodDirection: 'higher',
    gapToTop: 0.2,
    gapImpact: '-20% late-stage conversion',
  },
  {
    metricName: 'ebEngagement',
    metricLabel: 'Economic Buyer Engaged',
    repValue: 0.55,
    topPerformerValue: 0.78,
    teamAvgValue: 0.52,
    format: 'percent',
    goodDirection: 'higher',
    gapToTop: 0.23,
    gapImpact: 'Higher slip risk',
  },
]

export const activityMetrics: ActivityMetrics = {
  totalActivities: 143,
  calls: 45,
  emails: 78,
  meetings: 12,
  demos: 8,
  avgDaily: 8.2,
  teamAvgDaily: 10.5,
  topPerformerDaily: 14.2,
  dailyTrend: [
    { day: 'M', count: 12 },
    { day: 'T', count: 15 },
    { day: 'W', count: 11 },
    { day: 'T', count: 8 },
    { day: 'F', count: 6 },
    { day: 'M', count: 14 },
    { day: 'T', count: 12 },
    { day: 'W', count: 10 },
  ],
}

export const coachingStrengths: CoachingInsight[] = [
  {
    type: 'strength',
    title: 'Strong deal qualification',
    detail: 'Your Discovery→Solution conversion is 15% above team average',
    metric: '72% conversion',
    comparison: 'Team: 57%',
  },
  {
    type: 'strength',
    title: 'Excellent proposal velocity',
    detail: 'You get proposals out 2.3 days faster than average',
    metric: '8.7 days avg',
    comparison: 'Team: 11 days',
  },
  {
    type: 'strength',
    title: 'High engagement scores',
    detail: 'Customer response rates are above team benchmarks',
    metric: '68% response rate',
  },
]

export const coachingImprovements: CoachingInsight[] = [
  {
    type: 'improvement',
    title: 'Executive engagement',
    detail: '23% of your deals have no Economic Buyer contact',
    metric: 'Top performers: only 8%',
    action: 'Add EB outreach to TechVentures, CloudFirst this week',
  },
  {
    type: 'improvement',
    title: 'Earlier champion identification',
    detail: 'You identify champions 12 days later than top performers on average',
    metric: 'Your avg: Day 28',
    comparison: 'Top: Day 16',
    action: 'Focus discovery calls on identifying internal advocates',
  },
  {
    type: 'improvement',
    title: 'Activity consistency',
    detail: 'Your activity drops 30% on Thursdays and Fridays',
    metric: 'Thu/Fri avg: 7 activities',
    comparison: 'Mon-Wed avg: 13',
  },
]

export const coachingActions: CoachingInsight[] = [
  {
    type: 'action',
    dealName: 'TechVentures',
    dealAmount: 420000,
    title: 'Identify champion',
    detail: 'Champion not identified • Deal at risk of stalling',
  },
  {
    type: 'action',
    dealName: 'CloudFirst',
    dealAmount: 650000,
    title: 'Request exec intro',
    detail: 'Competitor involved • Need executive sponsorship',
  },
  {
    type: 'action',
    dealName: 'DataSystems',
    dealAmount: 890000,
    title: 'Re-engage',
    detail: '14 days since last meaningful contact',
  },
]

export const pipelineDeals: PipelineDeal[] = [
  { id: 'OPP-001', accountName: 'TechVentures', amount: 420000, healthScore: 55, primaryAction: 'call' },
  { id: 'OPP-002', accountName: 'CloudFirst Ent', amount: 650000, healthScore: 52, primaryAction: 'email' },
  { id: 'OPP-003', accountName: 'DataSystems Pro', amount: 890000, healthScore: 45, primaryAction: 'meeting' },
]

export const nextBestActions: NextBestAction[] = [
  {
    actionId: 'ACT-001',
    opportunityId: 'OPP-001',
    accountName: 'TechVentures',
    contactName: 'John Mitchell',
    actionType: 'call',
    actionReason: 'Follow up on pricing proposal',
    priorityRank: 1,
    urgency: 'today',
    dealAmount: 420000,
    dealHealth: 55,
    bestTime: 'Today 2-4pm',
  },
  {
    actionId: 'ACT-002',
    opportunityId: 'OPP-002',
    accountName: 'CloudFirst',
    contactName: 'Lisa Park',
    actionType: 'email',
    actionReason: 'Send competitive battle card',
    priorityRank: 2,
    urgency: 'today',
    dealAmount: 650000,
    dealHealth: 52,
    bestTime: 'Before EOD',
  },
]

// =====================================================
// SALES REP LIST (for selector)
// =====================================================

export const salesReps = [
  { id: 'REP-001', name: 'Sarah Williams', team: 'West', attainment: 0.78 },
  { id: 'REP-002', name: 'Jessica Taylor', team: 'Central', attainment: 1.42 },
  { id: 'REP-003', name: 'David Kim', team: 'East', attainment: 1.08 },
  { id: 'REP-004', name: 'Kevin Brown', team: 'South', attainment: 0.55 },
  { id: 'REP-005', name: 'Emily Rodriguez', team: 'West', attainment: 1.15 },
  { id: 'REP-006', name: 'James Wilson', team: 'West', attainment: 0.78 },
]
