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

// Process bottleneck from analytics
export interface ProcessBottleneck {
  activity: string
  avgDuration: number
  benchmarkDays: number
  delayRatio: number
  bottleneckCount: number
  bottleneckRate: number
  bottleneckRevenue: number
  severity: 'CRITICAL' | 'HIGH' | 'MEDIUM' | 'LOW' | 'ON TARGET'
  daysLostPerDeal: number
  rootCauseHypothesis: string
  recommendedAction: string
  priorityScore: number
}

// Competitive loss summary
export interface CompetitiveLoss {
  competitor: string
  dealsLost: number
  revenueLost: number
  lossReasons: string[]
  affectedSegments: string[]
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
// Pipeline Hygiene Types
// =====================================================

// Individual hygiene alert for a deal
export interface HygieneAlert {
  opportunityId: string
  opportunityName: string
  accountName: string
  ownerId: string
  ownerName: string
  amount: number
  stageName: string
  closeDate: string
  healthScore: number
  hygieneScore: number
  hygieneCategory: 'Clean' | 'Minor Issues' | 'Needs Attention' | 'Critical'
  violationCount: number
  primaryAlert: HygieneAlertType
  alertSeverity: 'CRITICAL' | 'HIGH' | 'MEDIUM' | 'LOW' | 'NONE'
  recommendedAction: string
  // Individual violation flags
  isStaleCloseDate: boolean
  isSingleThreaded: boolean
  isMissingNextSteps: boolean
  isGoneDark: boolean
  isMissingChampion: boolean
  isStalledStage: boolean
  isOverdueCommit: boolean
  isHeavyDiscount: boolean
  // Context
  daysSinceActivity: number
  daysUntilClose: number
  daysInCurrentStage: number
  stakeholderCount: number
  discountPercent: number
}

export type HygieneAlertType =
  | 'GONE_DARK'
  | 'STALE_CLOSE_DATE'
  | 'OVERDUE_COMMIT'
  | 'MISSING_CHAMPION'
  | 'SINGLE_THREADED'
  | 'STALLED_STAGE'
  | 'MISSING_NEXT_STEPS'
  | 'HEAVY_DISCOUNT'
  | 'NONE'

// Pipeline-level hygiene summary
export interface HygieneSummary {
  totalOpportunities: number
  avgHygieneScore: number
  // Violation counts
  staleCloseDateCount: number
  singleThreadedCount: number
  missingNextStepsCount: number
  goneDarkCount: number
  missingChampionCount: number
  stalledStageCount: number
  overdueCommitCount: number
  heavyDiscountCount: number
  // By severity
  criticalCount: number
  highCount: number
  mediumCount: number
  lowCount: number
  totalAlerts: number
  // By hygiene category
  cleanCount: number
  minorIssuesCount: number
  needsAttentionCount: number
  criticalHygieneCount: number
  // Revenue at risk
  criticalRevenueAtRisk: number
  highRevenueAtRisk: number
}

// Hygiene metrics by rep for coaching
export interface HygieneByOwner {
  ownerId: string
  ownerName: string
  totalDeals: number
  avgHygieneScore: number
  totalViolations: number
  criticalAlerts: number
  highAlerts: number
  goneDarkCount: number
  singleThreadedCount: number
  missingChampionCount: number
  missingNextStepsCount: number
  atRiskRevenue: number
  primaryCoachingArea: string
}

// Hygiene trend data point
export interface HygieneTrendPoint {
  snapshotDate: string
  avgHygieneScore: number
  totalAlerts: number
  criticalCount: number
  goneDarkCount: number
  singleThreadedCount: number
  staleCloseCount: number
  revenueAtRisk: number
  scoreChangeWow: number | null
  alertsChangeWow: number | null
  weekNumber: number
  quarter: string
}

// =====================================================
// Leading Indicator / ML Prediction Types
// =====================================================

// Win probability prediction for a deal
export interface DealPrediction {
  opportunityId: string
  opportunityName: string
  accountName: string
  ownerId: string
  ownerName: string
  amount: number
  stageName: string
  closeDate: string
  healthScore: number
  winProbability: number
  winProbabilityTier: 'High' | 'Medium' | 'Low' | 'Very Low'
  slipRiskScore: number
  slipRiskTier: 'High' | 'Medium' | 'Low'
  predictionSummary: string
  recommendedFocus: string
  expectedOutcome: 'WIN' | 'TOSS-UP' | 'UNLIKELY'
  weightedAmount: number
  slipRiskAmount: number
  // Feature breakdown for explainability
  healthFactor: number
  activityRecencyFactor: number
  stakeholderFactor: number
  velocityFactor: number
  predictionConfidence: 'High' | 'Medium' | 'Low'
}

// Aggregate leading indicators summary
export interface LeadingIndicatorsSummary {
  totalOpportunities: number
  totalPipeline: number
  probabilityWeightedPipeline: number
  avgWinProbability: number
  // By probability tier
  highProbCount: number
  highProbAmount: number
  mediumProbCount: number
  mediumProbAmount: number
  lowProbCount: number
  lowProbAmount: number
  // Slip risk
  highSlipRiskCount: number
  highSlipRiskAmount: number
  mediumSlipRiskCount: number
  mediumSlipRiskAmount: number
  totalSlipRiskAmount: number
  avgSlipRisk: number
  // Insights
  avgActivityRecency: number
  avgStakeholderEngagement: number
  avgVelocityScore: number
  aiForecastAdjustment: number
  forecastConfidence: 'High' | 'Medium' | 'Low'
  primaryRiskFactor: string
}

// =====================================================
// Cross-Functional Telemetry Types
// =====================================================

// Funnel handoff metrics by segment/source
export interface FunnelHandoffMetrics {
  leadSource: string
  segment: string
  totalLeads: number
  mqlCount: number
  sqlCount: number
  qualifiedCount: number
  convertedCount: number
  disqualifiedCount: number
  avgLeadScore: number
  avgResponseTimeDays: number
  mqlRate: number
  mqlToSqlRate: number
  sqlToQualifiedRate: number
  qualifiedToConvertedRate: number
  overallConversionRate: number
  disqualificationRate: number
  mqlRateHealth: 'HEALTHY' | 'MONITOR' | 'AT_RISK'
  mqlToSqlHealth: 'HEALTHY' | 'MONITOR' | 'AT_RISK'
  responseTimeHealth: 'HEALTHY' | 'MONITOR' | 'AT_RISK'
}

// Lead response analytics
export interface LeadResponseMetrics {
  leadSource: string
  segment: string
  responseTimeBucket: string
  leadCount: number
  avgResponseHours: number
  avgLeadScore: number
  convertedCount: number
  conversionRate: number
  speedImpact: 'HIGH_POSITIVE' | 'POSITIVE' | 'NEUTRAL' | 'NEGATIVE' | 'HIGH_NEGATIVE' | 'UNKNOWN'
}

// Cross-team activity per opportunity
export interface CrossTeamActivity {
  opportunityId: string
  accountName: string
  stageName: string
  amount: number
  marketingTouches: number
  salesTouches: number
  csTouches: number
  totalTouches: number
  teamsEngaged: number
  multiTeamEngagement: 'FULL' | 'PARTIAL' | 'SINGLE'
  engagementScore: number
  healthScore: number
  isWon: number
}

// Funnel drop-off analysis
export interface FunnelDropAnalysis {
  segment: string
  leadSource: string
  totalLeads: number
  mqlCount: number
  sqlCount: number
  oppCount: number
  qualifiedOppCount: number
  proposalCount: number
  negotiationCount: number
  wonCount: number
  wonAmount: number
  dropLeadToMql: number
  dropMqlToSql: number
  dropSqlToOpp: number
  dropOppToQualified: number
  dropQualifiedToProposal: number
  dropProposalToNegotiation: number
  dropNegotiationToWon: number
  overallConversionRate: number
  largestDropStage: string
}

// Cross-functional telemetry summary
export interface TelemetrySummary {
  // Volume metrics
  totalLeads: number
  totalMqls: number
  totalSqls: number
  totalConverted: number
  // Conversion metrics
  mqlToSqlRate: number
  overallConversionRate: number
  // Response metrics
  avgResponseDays: number
  fastResponsePct: number
  slowResponseCount: number
  // Engagement metrics
  avgEngagementScore: number
  avgTouchesPerDeal: number
  fullEngagementPct: number
  fullEngagementHealthAvg: number
  singleEngagementHealthAvg: number
  healthDelta: number
  // Health indicators
  mqlToSqlHealth: 'HEALTHY' | 'MONITOR' | 'AT_RISK'
  responseHealth: 'HEALTHY' | 'MONITOR' | 'AT_RISK'
  speedToLeadHealth: 'HEALTHY' | 'MONITOR' | 'AT_RISK'
  teamAlignmentHealth: 'HEALTHY' | 'MONITOR' | 'AT_RISK'
  overallHealth: 'HEALTHY' | 'MONITOR' | 'AT_RISK'
  // Insights
  primaryBottleneck: string
  recommendation: string
  snapshotDate: string
}

// =====================================================
// QBR (Quarterly Business Review) Types
// =====================================================

// QBR Executive Summary
export interface QBRExecutiveSummary {
  quarter: string
  // Revenue metrics
  totalClosedWon: number
  totalQuota: number
  teamAttainment: number
  avgRepAttainment: number
  // Team metrics
  totalReps: number
  topPerformers: number
  atRiskReps: number
  // Pipeline metrics
  totalPipeline: number
  avgHealthScore: number
  atRiskPipeline: number
  hygieneScore: number
  // Forecast metrics
  forecastAccuracy: number
  forecastGrade: string
  // Win/loss metrics
  winRate: number
  avgDealSize: number
  avgSalesCycle: number
  topLossReason: string
  // Overall health
  overallHealthScore: number
  healthGrade: string
  // Insights
  insight1: string
  insight2: string
  insight3: string
  // Recommendations
  recommendation1: string
  recommendation2: string
  recommendation3: string
  generatedAt: string
}

// QBR Performance by Rep
export interface QBRRepPerformance {
  ownerId: string
  ownerName: string
  region: string
  segment: string
  closedWon: number
  openPipeline: number
  totalDeals: number
  wonCount: number
  lostCount: number
  avgDealSize: number
  avgSalesCycle: number
  winRate: number
  quotaAmount: number
  quotaAttainment: number
  gapToQuota: number
  performanceTier: 'Top Performer' | 'On Track' | 'Needs Improvement' | 'At Risk'
}

// QBR Win/Loss Analysis
export interface QBRWinLossAnalysis {
  quarter: string
  winsCount: number
  winsRevenue: number
  winsAvgDealSize: number
  winsAvgActivities: number
  winsAvgMeetings: number
  winsAvgCycle: number
  lossesCount: number
  lossesRevenue: number
  lossesAvgDealSize: number
  lossesAvgActivities: number
  lossesAvgMeetings: number
  lossesAvgCycle: number
  winRate: number
  topLossReason1: string
  topLossReason1Pct: number
  topLossReason2: string
  topLossReason2Pct: number
  topLossReason3: string
  topLossReason3Pct: number
  activityDelta: number
  meetingDelta: number
  cycleDelta: number
}

// =====================================================
// Territory & Account Planning Types
// =====================================================

// Account score with propensity
export interface AccountScore {
  accountId: string
  accountName: string
  industry: string
  segment: string
  region: string
  territoryId: string
  territoryName: string
  ownerName: string
  employeeCount: number
  annualRevenue: number
  currentArr: number
  propensityScore: number
  propensityTier: 'High' | 'Medium' | 'Low'
  firmographicScore: number
  behavioralScore: number
  engagementScore: number
  pipelineScore: number
  expansionPotential: number
  topExpansionProduct: string
  lastEngagementDate: string
  openOpportunities: number
  totalPipelineValue: number
  healthScore: number
  recommendedAction: string
}

// Territory balance metrics
export interface TerritoryBalance {
  territoryId: string
  territoryName: string
  region: string
  segment: string
  ownerName: string
  accountCount: number
  highPropensityCount: number
  totalArr: number
  totalPotential: number
  avgPropensityScore: number
  pipelineValue: number
  quotaAmount: number
  capacityScore: number
  balanceStatus: 'Balanced' | 'Underloaded' | 'Overloaded'
  workloadIndex: number
  recommendedAction: string
}

// White space analysis
export interface WhiteSpaceAccount {
  accountId: string
  accountName: string
  industry: string
  segment: string
  region: string
  employeeCount: number
  annualRevenue: number
  currentProducts: string[]
  whitespaceProducts: string[]
  estimatedPotential: number
  propensityScore: number
  propensityTier: 'High' | 'Medium' | 'Low'
  competitorPresent: boolean
  competitorName: string | null
  icpFitScore: number
  recommendedApproach: string
  priorityRank: number
}

// Territory summary
export interface TerritorySummary {
  totalTerritories: number
  totalAccounts: number
  avgAccountsPerTerritory: number
  avgPropensityScore: number
  highPropensityCount: number
  highPropensityRevenuePotential: number
  mediumPropensityCount: number
  lowPropensityCount: number
  balancedTerritories: number
  underloadedTerritories: number
  overloadedTerritories: number
  avgTerritoryBalance: number
  whiteSpaceAccounts: number
  whiteSpaceRevenuePotential: number
  topExpansionSegment: string
  topExpansionIndustry: string
  snapshotDate: string
}

// =====================================================
// Capacity & Hiring Planning Types
// =====================================================

export interface RepCapacity {
  repId: string
  repName: string
  region: string
  segment: string
  hireDate: string
  daysSinceHire: number
  rampStatus: 'Ramping' | 'Developing' | 'Fully Ramped'
  rampFactor: number
  quotaAmount: number
  effectiveQuota: number
  closedWon90d: number
  openPipeline: number
  quotaAttainment: number
  coverageRatio: number
  activeAccounts: number
  capacityUtilization: number
  capacityHeadroom: number
  capacityStatus: 'Over Capacity' | 'At Capacity' | 'Healthy' | 'Under Utilized'
  productivityIndex: number
  avgDealHealth: number
}

export interface TeamCapacitySummary {
  region: string
  segment: string
  totalReps: number
  rampedReps: number
  rampingReps: number
  developingReps: number
  totalQuota: number
  effectiveQuota: number
  totalClosedWon: number
  totalPipeline: number
  teamAttainment: number
  teamCoverageRatio: number
  avgCapacityUtilization: number
  avgProductivity: number
  overCapacityCount: number
  underUtilizedCount: number
  rampCapacityLoss: number
  avgAccountsPerRep: number
  hiringNeedScore: 'Critical' | 'High' | 'Medium' | 'Low'
  recommendedHires: number
  capacityHealth: 'Strong' | 'Healthy' | 'Strained' | 'Under Utilized'
}

export interface RampCohortAnalysis {
  rampCohort: string
  repCount: number
  avgAttainment: number
  avgProductivity: number
  avgCoverage: number
  avgAccounts: number
  avgDaysToFirstClose: number | null
  expectedAttainment: number
  performanceVsExpected: number
  cohortOrder: number
}

export interface HiringImpactModel {
  region: string
  segment: string
  avgRepQuota: number
  currentCapacityGap: number
  quotaPerHireY1: number
  pipelinePerHireY1: number
  hiresToCloseGap: number
  investmentPerHire: number
  totalInvestment: number
  projectedRevenueY1: number
  projectedPipelineY1: number
  roiY1: number
  monthsToBreakeven: number
}

export interface CapacityPlanningSummary {
  totalReps: number
  rampedReps: number
  rampingReps: number
  developingReps: number
  totalQuota: number
  effectiveQuota: number
  totalClosedWon: number
  totalPipeline: number
  avgUtilization: number
  avgProductivity: number
  overCapacityCount: number
  underUtilizedCount: number
  teamAttainment: number
  coverageRatio: number
  rampCapacityLossPct: number
  capacityHealth: 'Strong' | 'Healthy' | 'Strained' | 'Under Utilized'
  hiringUrgency: 'Critical' | 'High' | 'Medium' | 'Low'
  totalRecommendedHires: number
  totalHiringInvestment: number
  totalProjectedRevenue: number
  totalCapacityGap: number
  snapshotDate: string
}

// =====================================================
// Scenario Modeling Types
// =====================================================

export interface ScenarioResult {
  scenarioName: string
  projectedRevenue: number
  revenueDelta: number
  baselineValue: number
  impactPercent: number
}

export interface WinRateScenario {
  scenarioName: string
  winRateDelta: number
  projectedWinRate: number
  projectedRevenue: number
  revenueDelta: number
  revenueDeltaPct: number
}

export interface DealSizeScenario {
  scenarioName: string
  dealSizeDeltaPct: number
  projectedDealSize: number
  projectedPipeline: number
  projectedRevenue: number
  revenueDelta: number
}

export interface CycleTimeScenario {
  scenarioName: string
  cycleDaysDelta: number
  projectedCycleDays: number
  winRateImpact: number
  projectedWinRate: number
  projectedRevenue: number
  revenueDelta: number
  dealsPerQuarterChange: number
}

export interface ScenarioSummary {
  baselineRevenue: number
  baselineWinRate: number
  baselineDealSize: number
  winRateBestScenario: string
  winRateUpside: number
  winRateUpsidePct: number
  dealSizeBestScenario: string
  dealSizeUpside: number
  dealSizeUpsidePct: number
  cycleTimeBestScenario: string
  cycleTimeUpside: number
  totalUpsidePotential: number
  snapshotDate: string
}

// =====================================================
// Customer Health & Expansion Types
// =====================================================

export interface CustomerHealth {
  accountId: string
  accountName: string
  industry: string
  segment: string
  currentArr: number
  healthScore: number
  healthTier: 'Healthy' | 'Monitor' | 'At Risk' | 'Critical'
  engagementScore: number
  relationshipScore: number
  financialScore: number
  supportScore: number
  churnRisk: 'Low' | 'Medium' | 'High' | 'Critical'
  renewalDate: string
  daysToRenewal: number
  daysSinceActivity: number
  contactsEngaged: number
  openPipeline: number
}

export interface ExpansionOpportunity {
  accountId: string
  accountName: string
  currentArr: number
  healthScore: number
  expansionScore: number
  expansionPotential: number
  expansionTier: 'High' | 'Medium' | 'Low' | 'None'
  recommendedPlay: string
  priorityRank: number
}

export interface ChurnRiskAccount {
  accountId: string
  accountName: string
  industry: string
  currentArr: number
  healthScore: number
  churnRisk: 'High' | 'Critical'
  riskFactors: string
  arrAtRisk: number
  saveProbability: number
  daysToRenewal: number
  daysSinceActivity: number
  recommendedAction: string
  urgency: 'Immediate' | 'This Week' | 'This Month'
  priorityRank: number
}

export interface CustomerHealthSummary {
  totalCustomers: number
  totalArr: number
  avgHealthScore: number
  healthyCount: number
  monitorCount: number
  atRiskCount: number
  criticalCount: number
  healthyArr: number
  atRiskArr: number
  renewals90d: number
  renewalArr90d: number
  totalExpansionPotential: number
  highExpansionCount: number
  highExpansionValue: number
  churnRiskCount: number
  totalArrAtRisk: number
  netRetentionForecast: number
  snapshotDate: string
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
