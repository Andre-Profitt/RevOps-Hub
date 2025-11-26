/**
 * Foundry API Integration Layer
 *
 * This module provides functions to connect the React app to Foundry data.
 * In development, it returns mock data. In production, it calls the secure
 * API route which proxies to Foundry (keeping credentials server-side).
 */

// Import mock data for development
import * as mockData from '@/data/mockData'
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
  ProcessBottleneck,
  CompetitiveLoss,
  HygieneAlert,
  HygieneSummary,
  HygieneByOwner,
  HygieneTrendPoint,
  DealPrediction,
  LeadingIndicatorsSummary,
} from '@/types'

// Configuration - only USE_MOCK_DATA is client-safe
const USE_MOCK_DATA = process.env.NEXT_PUBLIC_USE_MOCK_DATA === 'true'

// =====================================================
// FILTER TYPES
// =====================================================

export interface DashboardFilters {
  quarter: string  // e.g., 'Q4 2024'
  team: string     // e.g., 'All Teams', 'West', 'Central', etc.
}

// Helper to build SQL WHERE clause for filters
function buildFilterClause(filters: DashboardFilters, quarterColumn = 'quarter', teamColumn = 'region'): string {
  const clauses: string[] = []

  if (filters.quarter) {
    clauses.push(`${quarterColumn} = '${filters.quarter}'`)
  }

  if (filters.team && filters.team !== 'All Teams') {
    clauses.push(`${teamColumn} = '${filters.team}'`)
  }

  return clauses.length > 0 ? clauses.join(' AND ') : '1=1'
}

// =====================================================
// API CLIENT (calls secure server-side route)
// =====================================================

interface QueryParams {
  query: string
  fallbackBranchIds?: string[]
}

async function executeFoundrySQL<T>(params: QueryParams): Promise<T[]> {
  // Call our secure API route instead of Foundry directly
  const response = await fetch('/api/foundry', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      query: params.query,
      fallbackBranchIds: params.fallbackBranchIds || ['master'],
    }),
  })

  const result = await response.json()

  // Server tells us to use mock data if Foundry isn't configured
  if (result.useMock) {
    throw new Error('Mock mode - Foundry not configured on server')
  }

  if (!response.ok) {
    throw new Error(result.error || `Foundry SQL error: ${response.status}`)
  }

  return result.rows as T[]
}

// =====================================================
// DATA FETCHERS - Pipeline Health Dashboard
// =====================================================

export async function getDashboardKPIs(filters?: DashboardFilters): Promise<DashboardKPIs> {
  if (USE_MOCK_DATA) {
    // Mock data is Q4 2024 only - return as-is for demo
    return mockData.dashboardKPIs
  }

  const quarterFilter = filters?.quarter || 'Q4 2024'
  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `AND region = '${filters.team}'` : ''

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Dashboard/kpis\`
      WHERE quarter = '${quarterFilter}'
      ${teamFilter}
      LIMIT 1
    `,
  })

  if (results.length === 0) {
    throw new Error('No KPI data found')
  }

  // Transform Foundry row to our type
  const row = results[0]
  return {
    quarterTarget: row.quarter_target,
    closedWonAmount: row.closed_won_amount,
    aiForecast: row.ai_forecast,
    aiForecastConfidence: row.ai_forecast_confidence,
    commitAmount: row.commit_amount,
    commitConfidence: row.commit_weighted / row.commit_amount,
    gapToTarget: row.gap_to_target,
    gapPct: row.gap_pct,
    coverageRatio: row.coverage_ratio,
    riskLevel: row.risk_level,
    forecastChange1w: row.forecast_change_1w,
    commitChange1w: row.commit_change_1w,
  }
}

export async function getForecastTrend(filters?: DashboardFilters): Promise<ForecastTrendPoint[]> {
  if (USE_MOCK_DATA) {
    return mockData.forecastTrend
  }

  const quarterFilter = filters?.quarter || 'Q4 2024'

  return executeFoundrySQL<ForecastTrendPoint>({
    query: `
      SELECT
        week,
        week_number as weekNumber,
        cumulative_target as target,
        cumulative_closed as closed,
        cumulative_commit as commit,
        cumulative_forecast as forecast
      FROM \`/RevOps/Analytics/forecast_history\`
      WHERE quarter = '${quarterFilter}'
      ORDER BY week_number
    `,
  })
}

export async function getStuckDeals(filters?: DashboardFilters): Promise<StuckDeal[]> {
  if (USE_MOCK_DATA) {
    // Apply client-side team filter to mock data
    let deals = mockData.stuckDeals
    if (filters?.team && filters.team !== 'All Teams') {
      // For demo, we'll simulate team filtering by owner
      // In real data, deals would have a region field
      deals = deals.filter(d => {
        // Map demo owners to regions for filtering
        const ownerTeams: Record<string, string> = {
          'Sarah Chen': 'West',
          'David Kim': 'East',
          'Emily Rodriguez': 'Central',
          'James Wilson': 'South',
        }
        return ownerTeams[d.ownerName] === filters.team
      })
    }
    return deals
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE region = '${filters.team}'` : ''

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        opportunity_id as id,
        account_name as accountName,
        opportunity_name as opportunityName,
        owner_name as ownerName,
        amount,
        stuck_activity as stageName,
        duration_days as daysInStage,
        health_score as healthScore,
        close_date as closeDate,
        urgency,
        primary_action as primaryAction
      FROM \`/RevOps/Analytics/stuck_deals_view\`
      ${teamFilter}
      ORDER BY amount DESC
      LIMIT 10
    `,
  })

  return results.map((row) => ({
    ...row,
    riskFlags: ['Stuck'], // Would come from a separate field
  }))
}

export async function getHealthDistribution(filters?: DashboardFilters): Promise<HealthDistribution> {
  if (USE_MOCK_DATA) {
    return mockData.healthDistribution
  }

  const quarterFilter = filters?.quarter || 'Q4 2024'
  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `AND region = '${filters.team}'` : ''

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Analytics/health_distribution\`
      WHERE quarter = '${quarterFilter}'
      ${teamFilter}
      LIMIT 1
    `,
  })

  const row = results[0]
  return {
    healthy: { count: row.healthy_count, amount: row.healthy_amount, pct: row.healthy_pct },
    monitor: { count: row.monitor_count, amount: row.monitor_amount, pct: row.monitor_pct },
    atRisk: { count: row.at_risk_count, amount: row.at_risk_amount, pct: row.at_risk_pct },
    critical: { count: row.critical_count, amount: row.critical_amount, pct: row.critical_pct },
    trend: row.at_risk_change_1w,
  }
}

export async function getProcessBottlenecks(filters?: DashboardFilters): Promise<ProcessBottleneck[]> {
  if (USE_MOCK_DATA) {
    return mockData.processBottlenecks
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        activity,
        avg_duration as avgDuration,
        benchmark_days as benchmarkDays,
        delay_ratio as delayRatio,
        bottleneck_count as bottleneckCount,
        bottleneck_rate as bottleneckRate,
        bottleneck_revenue as bottleneckRevenue,
        severity,
        days_lost_per_deal as daysLostPerDeal,
        root_cause_hypothesis as rootCauseHypothesis,
        recommended_action as recommendedAction,
        priority_score as priorityScore
      FROM \`/RevOps/Analytics/process_bottlenecks\`
      WHERE severity IN ('CRITICAL', 'HIGH', 'MEDIUM')
      ORDER BY priority_score DESC
      LIMIT 5
    `,
  })

  return results as ProcessBottleneck[]
}

export async function getCompetitiveLosses(): Promise<CompetitiveLoss[]> {
  if (USE_MOCK_DATA) {
    return mockData.competitiveLosses
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        competitor_mentioned as competitor,
        COUNT(*) as dealsLost,
        SUM(amount) as revenueLost,
        COLLECT_LIST(DISTINCT segment) as segments,
        COLLECT_LIST(DISTINCT loss_reason) as reasons
      FROM \`/RevOps/Enriched/deal_health_scores\`
      WHERE is_closed = true
        AND stage_name = 'Closed Lost'
        AND competitor_mentioned IS NOT NULL
      GROUP BY competitor_mentioned
      ORDER BY revenueLost DESC
    `,
  })

  return results.map((row: any) => ({
    competitor: row.competitor,
    dealsLost: row.dealsLost,
    revenueLost: row.revenueLost,
    lossReasons: row.reasons?.filter((r: string) => r) || ['Price', 'Feature gap'],
    affectedSegments: row.segments?.filter((s: string) => s) || ['Mid-Market'],
  }))
}

export function getStageVelocity(): StageVelocity[] {
  return mockData.stageVelocity
}

export function getPipelineFunnel(): FunnelStage[] {
  return mockData.pipelineFunnel
}

// =====================================================
// DATA FETCHERS - Rep Coaching Dashboard
// =====================================================

export async function getRepPerformance(repId: string): Promise<RepPerformance> {
  if (USE_MOCK_DATA) {
    return mockData.repPerformance
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Analytics/rep_performance_view\`
      WHERE rep_id = '${repId}'
      LIMIT 1
    `,
  })

  if (results.length === 0) {
    throw new Error(`Rep ${repId} not found`)
  }

  const row = results[0]
  return {
    repId: row.rep_id,
    repName: row.rep_name,
    managerName: row.manager_name,
    region: row.region,
    segment: row.segment,
    ytdAttainment: row.ytd_attainment,
    winRate: row.win_rate,
    avgDealSize: row.avg_deal_size,
    avgSalesCycle: row.avg_sales_cycle_days,
    overallRank: row.overall_rank,
    totalReps: row.total_reps,
    rankChange: row.rank_change_qoq,
    teamAvgWinRate: row.team_avg_win_rate,
    teamAvgDealSize: row.team_avg_deal_size,
    teamAvgCycle: row.team_avg_cycle,
    performanceTier: row.performance_tier,
  }
}

export async function getDriverComparisons(repId: string): Promise<DriverComparison[]> {
  if (USE_MOCK_DATA) {
    return mockData.driverComparisons
  }

  return executeFoundrySQL<DriverComparison>({
    query: `
      SELECT *
      FROM \`/RevOps/Analytics/rep_driver_comparison\`
      WHERE rep_id = '${repId}'
    `,
  })
}

export async function getNextBestActions(repId: string): Promise<NextBestAction[]> {
  if (USE_MOCK_DATA) {
    return mockData.nextBestActions
  }

  return executeFoundrySQL<NextBestAction>({
    query: `
      SELECT
        action_id as actionId,
        opportunity_id as opportunityId,
        account_name as accountName,
        contact_name as contactName,
        action_type as actionType,
        action_reason as actionReason,
        priority_rank as priorityRank,
        urgency,
        deal_amount as dealAmount,
        deal_health as dealHealth,
        time_display as bestTime
      FROM \`/RevOps/Dashboard/next_best_actions\`
      WHERE rep_id = '${repId}'
      ORDER BY priority_rank
      LIMIT 5
    `,
  })
}

// =====================================================
// DATA FETCHERS - Pipeline Hygiene
// =====================================================

export async function getHygieneAlerts(filters?: DashboardFilters): Promise<HygieneAlert[]> {
  if (USE_MOCK_DATA) {
    let alerts = mockData.hygieneAlerts
    if (filters?.team && filters.team !== 'All Teams') {
      // Map owners to regions for filtering
      const ownerTeams: Record<string, string> = {
        'Sarah Chen': 'West',
        'David Kim': 'East',
        'Emily Rodriguez': 'Central',
        'James Wilson': 'South',
        'Kevin Brown': 'South',
      }
      alerts = alerts.filter(a => ownerTeams[a.ownerName] === filters.team)
    }
    return alerts
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE region = '${filters.team}'` : ''

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        opportunity_id as opportunityId,
        opportunity_name as opportunityName,
        account_name as accountName,
        owner_id as ownerId,
        owner_name as ownerName,
        amount,
        stage_name as stageName,
        close_date as closeDate,
        health_score as healthScore,
        hygiene_score as hygieneScore,
        hygiene_category as hygieneCategory,
        violation_count as violationCount,
        primary_alert as primaryAlert,
        alert_severity as alertSeverity,
        recommended_action as recommendedAction,
        is_stale_close_date as isStaleCloseDate,
        is_single_threaded as isSingleThreaded,
        is_missing_next_steps as isMissingNextSteps,
        is_gone_dark as isGoneDark,
        is_missing_champion as isMissingChampion,
        is_stalled_stage as isStalledStage,
        is_overdue_commit as isOverdueCommit,
        is_heavy_discount as isHeavyDiscount,
        days_since_activity as daysSinceActivity,
        days_until_close as daysUntilClose,
        days_in_current_stage as daysInCurrentStage,
        stakeholder_count as stakeholderCount,
        discount_percent as discountPercent
      FROM \`/RevOps/Analytics/pipeline_hygiene_alerts\`
      ${teamFilter}
      ORDER BY
        CASE alert_severity
          WHEN 'CRITICAL' THEN 1
          WHEN 'HIGH' THEN 2
          WHEN 'MEDIUM' THEN 3
          WHEN 'LOW' THEN 4
          ELSE 5
        END,
        amount DESC
      LIMIT 20
    `,
  })

  return results as HygieneAlert[]
}

export async function getHygieneSummary(filters?: DashboardFilters): Promise<HygieneSummary> {
  if (USE_MOCK_DATA) {
    return mockData.hygieneSummary
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `AND region = '${filters.team}'` : ''

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Analytics/pipeline_hygiene_summary\`
      WHERE 1=1
      ${teamFilter}
      LIMIT 1
    `,
  })

  if (results.length === 0) {
    throw new Error('No hygiene summary data found')
  }

  const row = results[0]
  return {
    totalOpportunities: row.total_opportunities,
    avgHygieneScore: row.avg_hygiene_score,
    staleCloseDateCount: row.stale_close_date_count,
    singleThreadedCount: row.single_threaded_count,
    missingNextStepsCount: row.missing_next_steps_count,
    goneDarkCount: row.gone_dark_count,
    missingChampionCount: row.missing_champion_count,
    stalledStageCount: row.stalled_stage_count,
    overdueCommitCount: row.overdue_commit_count,
    heavyDiscountCount: row.heavy_discount_count,
    criticalCount: row.critical_count,
    highCount: row.high_count,
    mediumCount: row.medium_count,
    lowCount: row.low_count,
    totalAlerts: row.total_alerts,
    cleanCount: row.clean_count,
    minorIssuesCount: row.minor_issues_count,
    needsAttentionCount: row.needs_attention_count,
    criticalHygieneCount: row.critical_hygiene_count,
    criticalRevenueAtRisk: row.critical_revenue_at_risk,
    highRevenueAtRisk: row.high_revenue_at_risk,
  }
}

export async function getHygieneByOwner(): Promise<HygieneByOwner[]> {
  if (USE_MOCK_DATA) {
    return mockData.hygieneByOwner
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        owner_id as ownerId,
        owner_name as ownerName,
        total_deals as totalDeals,
        avg_hygiene_score as avgHygieneScore,
        total_violations as totalViolations,
        critical_alerts as criticalAlerts,
        high_alerts as highAlerts,
        gone_dark_count as goneDarkCount,
        single_threaded_count as singleThreadedCount,
        missing_champion_count as missingChampionCount,
        missing_next_steps_count as missingNextStepsCount,
        at_risk_revenue as atRiskRevenue,
        primary_coaching_area as primaryCoachingArea
      FROM \`/RevOps/Analytics/pipeline_hygiene_by_owner\`
      ORDER BY critical_alerts DESC, avg_hygiene_score ASC
    `,
  })

  return results as HygieneByOwner[]
}

export async function getHygieneTrends(): Promise<HygieneTrendPoint[]> {
  if (USE_MOCK_DATA) {
    return mockData.hygieneTrends
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        snapshot_date as snapshotDate,
        avg_hygiene_score as avgHygieneScore,
        total_alerts as totalAlerts,
        critical_count as criticalCount,
        gone_dark_count as goneDarkCount,
        single_threaded_count as singleThreadedCount,
        stale_close_count as staleCloseCount,
        revenue_at_risk as revenueAtRisk,
        score_change_wow as scoreChangeWow,
        alerts_change_wow as alertsChangeWow,
        week_number as weekNumber,
        quarter
      FROM \`/RevOps/Analytics/pipeline_hygiene_trends\`
      ORDER BY snapshot_date ASC
    `,
  })

  return results as HygieneTrendPoint[]
}

// =====================================================
// DATA FETCHERS - Leading Indicators / ML Predictions
// =====================================================

export async function getDealPredictions(filters?: DashboardFilters): Promise<DealPrediction[]> {
  if (USE_MOCK_DATA) {
    let predictions = mockData.dealPredictions
    if (filters?.team && filters.team !== 'All Teams') {
      const ownerTeams: Record<string, string> = {
        'Sarah Chen': 'West',
        'Jessica Taylor': 'Central',
        'David Kim': 'East',
        'Emily Rodriguez': 'West',
        'Kevin Brown': 'South',
      }
      predictions = predictions.filter(p => ownerTeams[p.ownerName] === filters.team)
    }
    return predictions
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE region = '${filters.team}'` : ''

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        opportunity_id as opportunityId,
        opportunity_name as opportunityName,
        account_name as accountName,
        owner_id as ownerId,
        owner_name as ownerName,
        amount,
        stage_name as stageName,
        close_date as closeDate,
        health_score as healthScore,
        win_probability as winProbability,
        win_probability_tier as winProbabilityTier,
        slip_risk_score as slipRiskScore,
        slip_risk_tier as slipRiskTier,
        prediction_summary as predictionSummary,
        recommended_focus as recommendedFocus,
        expected_outcome as expectedOutcome,
        weighted_amount as weightedAmount,
        slip_risk_amount as slipRiskAmount,
        health_factor as healthFactor,
        activity_recency_factor as activityRecencyFactor,
        stakeholder_factor as stakeholderFactor,
        velocity_factor as velocityFactor,
        prediction_confidence as predictionConfidence
      FROM \`/RevOps/Analytics/deal_predictions\`
      ${teamFilter}
      ORDER BY
        CASE expected_outcome
          WHEN 'TOSS-UP' THEN 1
          WHEN 'WIN' THEN 2
          ELSE 3
        END,
        amount DESC
      LIMIT 20
    `,
  })

  return results as DealPrediction[]
}

export async function getLeadingIndicatorsSummary(filters?: DashboardFilters): Promise<LeadingIndicatorsSummary> {
  if (USE_MOCK_DATA) {
    return mockData.leadingIndicatorsSummary
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `AND region = '${filters.team}'` : ''

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Analytics/leading_indicators_summary\`
      WHERE 1=1
      ${teamFilter}
      LIMIT 1
    `,
  })

  if (results.length === 0) {
    throw new Error('No leading indicators data found')
  }

  const row = results[0]
  return {
    totalOpportunities: row.total_opportunities,
    totalPipeline: row.total_pipeline,
    probabilityWeightedPipeline: row.probability_weighted_pipeline,
    avgWinProbability: row.avg_win_probability,
    highProbCount: row.high_prob_count,
    highProbAmount: row.high_prob_amount,
    mediumProbCount: row.medium_prob_count,
    mediumProbAmount: row.medium_prob_amount,
    lowProbCount: row.low_prob_count,
    lowProbAmount: row.low_prob_amount,
    highSlipRiskCount: row.high_slip_risk_count,
    highSlipRiskAmount: row.high_slip_risk_amount,
    mediumSlipRiskCount: row.medium_slip_risk_count,
    mediumSlipRiskAmount: row.medium_slip_risk_amount,
    totalSlipRiskAmount: row.total_slip_risk_amount,
    avgSlipRisk: row.avg_slip_risk,
    avgActivityRecency: row.avg_activity_recency,
    avgStakeholderEngagement: row.avg_stakeholder_engagement,
    avgVelocityScore: row.avg_velocity_score,
    aiForecastAdjustment: row.ai_forecast_adjustment,
    forecastConfidence: row.forecast_confidence,
    primaryRiskFactor: row.primary_risk_factor,
  }
}

// =====================================================
// CROSS-FUNCTIONAL TELEMETRY
// =====================================================

import type { TelemetrySummary, FunnelHandoffMetrics, CrossTeamActivity } from '@/types'

export async function getTelemetrySummary(filters?: DashboardFilters): Promise<TelemetrySummary> {
  if (USE_MOCK_DATA) {
    return mockData.telemetrySummary
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE region = '${filters.team}'` : ''

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Analytics/telemetry_summary\`
      ${teamFilter}
      LIMIT 1
    `,
  })

  if (results.length === 0) {
    throw new Error('No telemetry summary data found')
  }

  const row = results[0]
  return {
    totalLeads: row.total_leads,
    totalMqls: row.total_mqls,
    totalSqls: row.total_sqls,
    totalConverted: row.total_converted,
    mqlToSqlRate: row.mql_to_sql_rate,
    overallConversionRate: row.overall_conversion_rate,
    avgResponseDays: row.avg_response_days,
    fastResponsePct: row.fast_response_pct,
    slowResponseCount: row.slow_response_count,
    avgEngagementScore: row.avg_engagement_score,
    avgTouchesPerDeal: row.avg_touches_per_deal,
    fullEngagementPct: row.full_engagement_pct,
    fullEngagementHealthAvg: row.full_engagement_health_avg,
    singleEngagementHealthAvg: row.single_engagement_health_avg,
    healthDelta: row.health_delta,
    mqlToSqlHealth: row.mql_to_sql_health,
    responseHealth: row.response_health,
    speedToLeadHealth: row.speed_to_lead_health,
    teamAlignmentHealth: row.team_alignment_health,
    overallHealth: row.overall_health,
    primaryBottleneck: row.primary_bottleneck,
    recommendation: row.recommendation,
    snapshotDate: row.snapshot_date,
  }
}

export async function getFunnelHandoffs(filters?: DashboardFilters): Promise<FunnelHandoffMetrics[]> {
  if (USE_MOCK_DATA) {
    let handoffs = [...mockData.funnelHandoffs]
    if (filters?.team && filters.team !== 'All Teams') {
      // Filter mock data by segment matching team name
      handoffs = handoffs.filter(h => h.segment === 'Enterprise') // Simplified filter
    }
    return handoffs
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE segment = '${filters.team}'` : ''

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        lead_source as leadSource,
        segment,
        total_leads as totalLeads,
        mql_count as mqlCount,
        sql_count as sqlCount,
        qualified_count as qualifiedCount,
        converted_count as convertedCount,
        disqualified_count as disqualifiedCount,
        avg_lead_score as avgLeadScore,
        avg_response_time_days as avgResponseTimeDays,
        mql_rate as mqlRate,
        mql_to_sql_rate as mqlToSqlRate,
        sql_to_qualified_rate as sqlToQualifiedRate,
        qualified_to_converted_rate as qualifiedToConvertedRate,
        overall_conversion_rate as overallConversionRate,
        disqualification_rate as disqualificationRate,
        mql_rate_health as mqlRateHealth,
        mql_to_sql_health as mqlToSqlHealth,
        response_time_health as responseTimeHealth
      FROM \`/RevOps/Analytics/funnel_handoff_metrics\`
      ${teamFilter}
      ORDER BY total_leads DESC
    `,
  })

  return results as FunnelHandoffMetrics[]
}

export async function getCrossTeamActivity(filters?: DashboardFilters): Promise<CrossTeamActivity[]> {
  if (USE_MOCK_DATA) {
    return mockData.crossTeamActivity
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE region = '${filters.team}'` : ''

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        opportunity_id as opportunityId,
        account_name as accountName,
        stage_name as stageName,
        amount,
        marketing_touches as marketingTouches,
        sales_touches as salesTouches,
        cs_touches as csTouches,
        total_touches as totalTouches,
        teams_engaged as teamsEngaged,
        multi_team_engagement as multiTeamEngagement,
        engagement_score as engagementScore,
        health_score as healthScore,
        is_won as isWon
      FROM \`/RevOps/Analytics/cross_team_activity\`
      ${teamFilter}
      ORDER BY engagement_score DESC
      LIMIT 20
    `,
  })

  return results as CrossTeamActivity[]
}

// =====================================================
// QBR (QUARTERLY BUSINESS REVIEW)
// =====================================================

import type { QBRExecutiveSummary, QBRRepPerformance, QBRWinLossAnalysis } from '@/types'

export async function getQBRExecutiveSummary(quarter?: string): Promise<QBRExecutiveSummary> {
  if (USE_MOCK_DATA) {
    return mockData.qbrExecutiveSummary
  }

  const quarterFilter = quarter ? `WHERE quarter = '${quarter}'` : ''

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Analytics/qbr_executive_summary\`
      ${quarterFilter}
      ORDER BY generated_at DESC
      LIMIT 1
    `,
  })

  if (results.length === 0) {
    throw new Error('No QBR summary data found')
  }

  const row = results[0]
  return {
    quarter: row.quarter,
    totalClosedWon: row.total_closed_won,
    totalQuota: row.total_quota,
    teamAttainment: row.team_attainment,
    avgRepAttainment: row.avg_rep_attainment,
    totalReps: row.total_reps,
    topPerformers: row.top_performers,
    atRiskReps: row.at_risk_reps,
    totalPipeline: row.total_pipeline,
    avgHealthScore: row.avg_health_score,
    atRiskPipeline: row.at_risk_pipeline,
    hygieneScore: row.hygiene_score,
    forecastAccuracy: row.forecast_accuracy,
    forecastGrade: row.forecast_grade,
    winRate: row.win_rate,
    avgDealSize: row.avg_deal_size,
    avgSalesCycle: row.avg_sales_cycle,
    topLossReason: row.top_loss_reason,
    overallHealthScore: row.overall_health_score,
    healthGrade: row.health_grade,
    insight1: row.insight_1,
    insight2: row.insight_2,
    insight3: row.insight_3,
    recommendation1: row.recommendation_1,
    recommendation2: row.recommendation_2,
    recommendation3: row.recommendation_3,
    generatedAt: row.generated_at,
  }
}

export async function getQBRRepPerformance(quarter?: string): Promise<QBRRepPerformance[]> {
  if (USE_MOCK_DATA) {
    return mockData.qbrRepPerformance
  }

  const quarterFilter = quarter ? `WHERE quarter = '${quarter}'` : ''

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        owner_id as ownerId,
        owner_name as ownerName,
        region,
        segment,
        closed_won as closedWon,
        open_pipeline as openPipeline,
        total_deals as totalDeals,
        won_count as wonCount,
        lost_count as lostCount,
        avg_deal_size as avgDealSize,
        avg_sales_cycle as avgSalesCycle,
        win_rate as winRate,
        quota_amount as quotaAmount,
        quota_attainment as quotaAttainment,
        gap_to_quota as gapToQuota,
        performance_tier as performanceTier
      FROM \`/RevOps/Analytics/qbr_performance_summary\`
      ${quarterFilter}
      ORDER BY quota_attainment DESC
    `,
  })

  return results as QBRRepPerformance[]
}

export async function getQBRWinLossAnalysis(quarter?: string): Promise<QBRWinLossAnalysis> {
  if (USE_MOCK_DATA) {
    return mockData.qbrWinLossAnalysis
  }

  const quarterFilter = quarter ? `WHERE quarter = '${quarter}'` : ''

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Analytics/qbr_win_loss_analysis\`
      ${quarterFilter}
      LIMIT 1
    `,
  })

  if (results.length === 0) {
    throw new Error('No win/loss data found')
  }

  const row = results[0]
  return {
    quarter: row.quarter,
    winsCount: row.wins_count,
    winsRevenue: row.wins_revenue,
    winsAvgDealSize: row.wins_avg_deal_size,
    winsAvgActivities: row.wins_avg_activities,
    winsAvgMeetings: row.wins_avg_meetings,
    winsAvgCycle: row.wins_avg_cycle,
    lossesCount: row.losses_count,
    lossesRevenue: row.losses_revenue,
    lossesAvgDealSize: row.losses_avg_deal_size,
    lossesAvgActivities: row.losses_avg_activities,
    lossesAvgMeetings: row.losses_avg_meetings,
    lossesAvgCycle: row.losses_avg_cycle,
    winRate: row.win_rate,
    topLossReason1: row.top_loss_reason_1,
    topLossReason1Pct: row.top_loss_reason_1_pct,
    topLossReason2: row.top_loss_reason_2,
    topLossReason2Pct: row.top_loss_reason_2_pct,
    topLossReason3: row.top_loss_reason_3,
    topLossReason3Pct: row.top_loss_reason_3_pct,
    activityDelta: row.activity_delta,
    meetingDelta: row.meeting_delta,
    cycleDelta: row.cycle_delta,
  }
}

// =====================================================
// TERRITORY & ACCOUNT PLANNING
// =====================================================

import type { AccountScore, TerritoryBalance, WhiteSpaceAccount, TerritorySummary } from '@/types'

export async function getTerritorySummary(filters?: DashboardFilters): Promise<TerritorySummary> {
  if (USE_MOCK_DATA) {
    return mockData.territorySummary
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE region = '${filters.team}'` : ''

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Analytics/territory_summary\`
      ${teamFilter}
      LIMIT 1
    `,
  })

  if (results.length === 0) {
    throw new Error('No territory summary data found')
  }

  const row = results[0]
  return {
    totalTerritories: row.total_territories,
    totalAccounts: row.total_accounts,
    avgAccountsPerTerritory: row.avg_accounts_per_territory,
    avgPropensityScore: row.avg_propensity_score,
    highPropensityCount: row.high_propensity_count,
    highPropensityRevenuePotential: row.high_propensity_revenue_potential,
    mediumPropensityCount: row.medium_propensity_count,
    lowPropensityCount: row.low_propensity_count,
    balancedTerritories: row.balanced_territories,
    underloadedTerritories: row.underloaded_territories,
    overloadedTerritories: row.overloaded_territories,
    avgTerritoryBalance: row.avg_territory_balance,
    whiteSpaceAccounts: row.white_space_accounts,
    whiteSpaceRevenuePotential: row.white_space_revenue_potential,
    topExpansionSegment: row.top_expansion_segment,
    topExpansionIndustry: row.top_expansion_industry,
    snapshotDate: row.snapshot_date,
  }
}

export async function getAccountScores(filters?: DashboardFilters): Promise<AccountScore[]> {
  if (USE_MOCK_DATA) {
    let accounts = [...mockData.accountScores]
    if (filters?.team && filters.team !== 'All Teams') {
      accounts = accounts.filter(a => a.region === filters.team)
    }
    return accounts
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE region = '${filters.team}'` : ''

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        account_id as accountId,
        account_name as accountName,
        industry,
        segment,
        region,
        territory_id as territoryId,
        territory_name as territoryName,
        owner_name as ownerName,
        employee_count as employeeCount,
        annual_revenue as annualRevenue,
        current_arr as currentArr,
        propensity_score as propensityScore,
        propensity_tier as propensityTier,
        firmographic_score as firmographicScore,
        behavioral_score as behavioralScore,
        engagement_score as engagementScore,
        pipeline_score as pipelineScore,
        expansion_potential as expansionPotential,
        top_expansion_product as topExpansionProduct,
        last_engagement_date as lastEngagementDate,
        open_opportunities as openOpportunities,
        total_pipeline_value as totalPipelineValue,
        health_score as healthScore,
        recommended_action as recommendedAction
      FROM \`/RevOps/Analytics/account_scores\`
      ${teamFilter}
      ORDER BY propensity_score DESC
      LIMIT 50
    `,
  })

  return results as AccountScore[]
}

export async function getTerritoryBalance(filters?: DashboardFilters): Promise<TerritoryBalance[]> {
  if (USE_MOCK_DATA) {
    let territories = [...mockData.territoryBalance]
    if (filters?.team && filters.team !== 'All Teams') {
      territories = territories.filter(t => t.region === filters.team)
    }
    return territories
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE region = '${filters.team}'` : ''

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        territory_id as territoryId,
        territory_name as territoryName,
        region,
        segment,
        owner_name as ownerName,
        account_count as accountCount,
        high_propensity_count as highPropensityCount,
        total_arr as totalArr,
        total_potential as totalPotential,
        avg_propensity_score as avgPropensityScore,
        pipeline_value as pipelineValue,
        quota_amount as quotaAmount,
        capacity_score as capacityScore,
        balance_status as balanceStatus,
        workload_index as workloadIndex,
        recommended_action as recommendedAction
      FROM \`/RevOps/Analytics/territory_balance\`
      ${teamFilter}
      ORDER BY workload_index DESC
    `,
  })

  return results as TerritoryBalance[]
}

export async function getWhiteSpaceAccounts(filters?: DashboardFilters): Promise<WhiteSpaceAccount[]> {
  if (USE_MOCK_DATA) {
    let accounts = [...mockData.whiteSpaceAccounts]
    if (filters?.team && filters.team !== 'All Teams') {
      accounts = accounts.filter(a => a.region === filters.team)
    }
    return accounts
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE region = '${filters.team}'` : ''

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        account_id as accountId,
        account_name as accountName,
        industry,
        segment,
        region,
        employee_count as employeeCount,
        annual_revenue as annualRevenue,
        current_products as currentProducts,
        whitespace_products as whitespaceProducts,
        estimated_potential as estimatedPotential,
        propensity_score as propensityScore,
        propensity_tier as propensityTier,
        competitor_present as competitorPresent,
        competitor_name as competitorName,
        icp_fit_score as icpFitScore,
        recommended_approach as recommendedApproach,
        priority_rank as priorityRank
      FROM \`/RevOps/Analytics/white_space_analysis\`
      ${teamFilter}
      ORDER BY priority_rank ASC
      LIMIT 20
    `,
  })

  return results as WhiteSpaceAccount[]
}

// =====================================================
// CAPACITY & HIRING PLANNING
// =====================================================

import type { RepCapacity, TeamCapacitySummary, RampCohortAnalysis, CapacityPlanningSummary } from '@/types'

export async function getCapacityPlanningSummary(): Promise<CapacityPlanningSummary> {
  if (USE_MOCK_DATA) {
    return mockData.capacityPlanningSummary
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Analytics/capacity_planning_summary\`
      LIMIT 1
    `,
  })

  if (results.length === 0) {
    throw new Error('No capacity planning summary data found')
  }

  const row = results[0]
  return {
    totalReps: row.total_reps,
    rampedReps: row.ramped_reps,
    rampingReps: row.ramping_reps,
    developingReps: row.developing_reps,
    totalQuota: row.total_quota,
    effectiveQuota: row.effective_quota,
    totalClosedWon: row.total_closed_won,
    totalPipeline: row.total_pipeline,
    avgUtilization: row.avg_utilization,
    avgProductivity: row.avg_productivity,
    overCapacityCount: row.over_capacity_count,
    underUtilizedCount: row.under_utilized_count,
    teamAttainment: row.team_attainment,
    coverageRatio: row.coverage_ratio,
    rampCapacityLossPct: row.ramp_capacity_loss_pct,
    capacityHealth: row.capacity_health,
    hiringUrgency: row.hiring_urgency,
    totalRecommendedHires: row.total_recommended_hires,
    totalHiringInvestment: row.total_hiring_investment,
    totalProjectedRevenue: row.total_projected_revenue,
    totalCapacityGap: row.total_capacity_gap,
    snapshotDate: row.snapshot_date,
  }
}

export async function getRepCapacity(filters?: DashboardFilters): Promise<RepCapacity[]> {
  if (USE_MOCK_DATA) {
    let reps = [...mockData.repCapacity]
    if (filters?.team && filters.team !== 'All Teams') {
      reps = reps.filter(r => r.region === filters.team)
    }
    return reps
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE region = '${filters.team}'` : ''

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        rep_id as repId,
        rep_name as repName,
        region,
        segment,
        hire_date as hireDate,
        days_since_hire as daysSinceHire,
        ramp_status as rampStatus,
        ramp_factor as rampFactor,
        quota_amount as quotaAmount,
        effective_quota as effectiveQuota,
        closed_won_90d as closedWon90d,
        open_pipeline as openPipeline,
        quota_attainment as quotaAttainment,
        coverage_ratio as coverageRatio,
        active_accounts as activeAccounts,
        capacity_utilization as capacityUtilization,
        capacity_headroom as capacityHeadroom,
        capacity_status as capacityStatus,
        productivity_index as productivityIndex,
        avg_deal_health as avgDealHealth
      FROM \`/RevOps/Analytics/rep_capacity_model\`
      ${teamFilter}
      ORDER BY capacity_utilization DESC
    `,
  })

  return results as RepCapacity[]
}

export async function getTeamCapacitySummary(filters?: DashboardFilters): Promise<TeamCapacitySummary[]> {
  if (USE_MOCK_DATA) {
    let teams = [...mockData.teamCapacitySummary]
    if (filters?.team && filters.team !== 'All Teams') {
      teams = teams.filter(t => t.region === filters.team)
    }
    return teams
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE region = '${filters.team}'` : ''

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        region,
        segment,
        total_reps as totalReps,
        ramped_reps as rampedReps,
        ramping_reps as rampingReps,
        developing_reps as developingReps,
        total_quota as totalQuota,
        effective_quota as effectiveQuota,
        total_closed_won as totalClosedWon,
        total_pipeline as totalPipeline,
        team_attainment as teamAttainment,
        team_coverage_ratio as teamCoverageRatio,
        avg_capacity_utilization as avgCapacityUtilization,
        avg_productivity as avgProductivity,
        over_capacity_count as overCapacityCount,
        under_utilized_count as underUtilizedCount,
        ramp_capacity_loss as rampCapacityLoss,
        avg_accounts_per_rep as avgAccountsPerRep,
        hiring_need_score as hiringNeedScore,
        recommended_hires as recommendedHires,
        capacity_health as capacityHealth
      FROM \`/RevOps/Analytics/team_capacity_summary\`
      ${teamFilter}
      ORDER BY avg_capacity_utilization DESC
    `,
  })

  return results as TeamCapacitySummary[]
}

export async function getRampCohortAnalysis(): Promise<RampCohortAnalysis[]> {
  if (USE_MOCK_DATA) {
    return mockData.rampCohortAnalysis
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        ramp_cohort as rampCohort,
        rep_count as repCount,
        avg_attainment as avgAttainment,
        avg_productivity as avgProductivity,
        avg_coverage as avgCoverage,
        avg_accounts as avgAccounts,
        avg_days_to_first_close as avgDaysToFirstClose,
        expected_attainment as expectedAttainment,
        performance_vs_expected as performanceVsExpected,
        cohort_order as cohortOrder
      FROM \`/RevOps/Analytics/ramp_time_analysis\`
      ORDER BY cohort_order ASC
    `,
  })

  return results as RampCohortAnalysis[]
}

// =====================================================
// SCENARIO MODELING
// =====================================================

import type { WinRateScenario, DealSizeScenario, CycleTimeScenario, ScenarioSummary } from '@/types'

export async function getScenarioSummary(): Promise<ScenarioSummary> {
  if (USE_MOCK_DATA) {
    return mockData.scenarioSummary
  }

  const results = await executeFoundrySQL<any>({
    query: `SELECT * FROM \`/RevOps/Analytics/scenario_summary\` LIMIT 1`,
  })

  if (results.length === 0) throw new Error('No scenario summary data found')
  const row = results[0]
  return {
    baselineRevenue: row.baseline_revenue,
    baselineWinRate: row.baseline_win_rate,
    baselineDealSize: row.baseline_deal_size,
    winRateBestScenario: row.win_rate_best_scenario,
    winRateUpside: row.win_rate_upside,
    winRateUpsidePct: row.win_rate_upside_pct,
    dealSizeBestScenario: row.deal_size_best_scenario,
    dealSizeUpside: row.deal_size_upside,
    dealSizeUpsidePct: row.deal_size_upside_pct,
    cycleTimeBestScenario: row.cycle_time_best_scenario,
    cycleTimeUpside: row.cycle_time_upside,
    totalUpsidePotential: row.total_upside_potential,
    snapshotDate: row.snapshot_date,
  }
}

export async function getWinRateScenarios(): Promise<WinRateScenario[]> {
  if (USE_MOCK_DATA) return mockData.winRateScenarios
  const results = await executeFoundrySQL<any>({
    query: `SELECT * FROM \`/RevOps/Analytics/scenario_win_rate_impact\` ORDER BY projected_revenue`,
  })
  return results.map((r: any) => ({
    scenarioName: r.scenario_name,
    winRateDelta: r.win_rate_delta,
    projectedWinRate: r.projected_win_rate,
    projectedRevenue: r.projected_revenue,
    revenueDelta: r.revenue_delta,
    revenueDeltaPct: r.revenue_delta_pct,
  }))
}

export async function getDealSizeScenarios(): Promise<DealSizeScenario[]> {
  if (USE_MOCK_DATA) return mockData.dealSizeScenarios
  const results = await executeFoundrySQL<any>({
    query: `SELECT * FROM \`/RevOps/Analytics/scenario_deal_size_impact\` ORDER BY projected_revenue`,
  })
  return results.map((r: any) => ({
    scenarioName: r.scenario_name,
    dealSizeDeltaPct: r.deal_size_delta_pct,
    projectedDealSize: r.projected_deal_size,
    projectedPipeline: r.projected_pipeline,
    projectedRevenue: r.projected_revenue,
    revenueDelta: r.revenue_delta,
  }))
}

export async function getCycleTimeScenarios(): Promise<CycleTimeScenario[]> {
  if (USE_MOCK_DATA) return mockData.cycleTimeScenarios
  const results = await executeFoundrySQL<any>({
    query: `SELECT * FROM \`/RevOps/Analytics/scenario_cycle_time_impact\` ORDER BY projected_revenue`,
  })
  return results.map((r: any) => ({
    scenarioName: r.scenario_name,
    cycleDaysDelta: r.cycle_days_delta,
    projectedCycleDays: r.projected_cycle_days,
    winRateImpact: r.win_rate_impact,
    projectedWinRate: r.projected_win_rate,
    projectedRevenue: r.projected_revenue,
    revenueDelta: r.revenue_delta,
    dealsPerQuarterChange: r.deals_per_quarter_change,
  }))
}

// =====================================================
// CUSTOMER HEALTH & EXPANSION
// =====================================================

import type { CustomerHealth, ExpansionOpportunity, ChurnRiskAccount, CustomerHealthSummary } from '@/types'

export async function getCustomerHealthSummary(): Promise<CustomerHealthSummary> {
  if (USE_MOCK_DATA) return mockData.customerHealthSummary
  const results = await executeFoundrySQL<any>({
    query: `SELECT * FROM \`/RevOps/Analytics/customer_health_summary\` LIMIT 1`,
  })
  if (results.length === 0) throw new Error('No customer health summary data found')
  const row = results[0]
  return {
    totalCustomers: row.total_customers,
    totalArr: row.total_arr,
    avgHealthScore: row.avg_health_score,
    healthyCount: row.healthy_count,
    monitorCount: row.monitor_count,
    atRiskCount: row.at_risk_count,
    criticalCount: row.critical_count,
    healthyArr: row.healthy_arr,
    atRiskArr: row.at_risk_arr,
    renewals90d: row.renewals_90d,
    renewalArr90d: row.renewal_arr_90d,
    totalExpansionPotential: row.total_expansion_potential,
    highExpansionCount: row.high_expansion_count,
    highExpansionValue: row.high_expansion_value,
    churnRiskCount: row.churn_risk_count,
    totalArrAtRisk: row.total_arr_at_risk,
    netRetentionForecast: row.net_retention_forecast,
    snapshotDate: row.snapshot_date,
  }
}

export async function getCustomerHealth(): Promise<CustomerHealth[]> {
  if (USE_MOCK_DATA) return mockData.customerHealth
  const results = await executeFoundrySQL<any>({
    query: `SELECT * FROM \`/RevOps/Analytics/customer_health_scores\` ORDER BY health_score DESC LIMIT 50`,
  })
  return results.map((r: any) => ({
    accountId: r.account_id,
    accountName: r.account_name,
    industry: r.industry,
    segment: r.segment,
    currentArr: r.current_arr,
    healthScore: r.health_score,
    healthTier: r.health_tier,
    engagementScore: r.engagement_score,
    relationshipScore: r.relationship_score,
    financialScore: r.financial_score,
    supportScore: r.support_score,
    churnRisk: r.churn_risk,
    renewalDate: r.renewal_date,
    daysToRenewal: r.days_to_renewal,
    daysSinceActivity: r.days_since_activity,
    contactsEngaged: r.contacts_engaged,
    openPipeline: r.open_pipeline,
  }))
}

export async function getExpansionOpportunities(): Promise<ExpansionOpportunity[]> {
  if (USE_MOCK_DATA) return mockData.expansionOpportunities
  const results = await executeFoundrySQL<any>({
    query: `SELECT * FROM \`/RevOps/Analytics/expansion_opportunities\` ORDER BY priority_rank LIMIT 20`,
  })
  return results.map((r: any) => ({
    accountId: r.account_id,
    accountName: r.account_name,
    currentArr: r.current_arr,
    healthScore: r.health_score,
    expansionScore: r.expansion_score,
    expansionPotential: r.expansion_potential,
    expansionTier: r.expansion_tier,
    recommendedPlay: r.recommended_play,
    priorityRank: r.priority_rank,
  }))
}

export async function getChurnRiskAccounts(): Promise<ChurnRiskAccount[]> {
  if (USE_MOCK_DATA) return mockData.churnRiskAccounts
  const results = await executeFoundrySQL<any>({
    query: `SELECT * FROM \`/RevOps/Analytics/churn_risk_analysis\` ORDER BY priority_rank LIMIT 20`,
  })
  return results.map((r: any) => ({
    accountId: r.account_id,
    accountName: r.account_name,
    industry: r.industry,
    currentArr: r.current_arr,
    healthScore: r.health_score,
    churnRisk: r.churn_risk,
    riskFactors: r.risk_factors,
    arrAtRisk: r.arr_at_risk,
    saveProbability: r.save_probability,
    daysToRenewal: r.days_to_renewal,
    daysSinceActivity: r.days_since_activity,
    recommendedAction: r.recommended_action,
    urgency: r.urgency,
    priorityRank: r.priority_rank,
  }))
}

// =====================================================
// HOOKS FOR REACT COMPONENTS
// =====================================================

import { useState, useEffect } from 'react'

export function useDashboardKPIs() {
  const [data, setData] = useState<DashboardKPIs | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<Error | null>(null)

  useEffect(() => {
    getDashboardKPIs()
      .then(setData)
      .catch(setError)
      .finally(() => setLoading(false))
  }, [])

  return { data, loading, error }
}

export function useRepPerformance(repId: string) {
  const [data, setData] = useState<RepPerformance | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<Error | null>(null)

  useEffect(() => {
    getRepPerformance(repId)
      .then(setData)
      .catch(setError)
      .finally(() => setLoading(false))
  }, [repId])

  return { data, loading, error }
}

// =====================================================
// UTILITY FUNCTIONS
// =====================================================

export async function checkFoundryConnection(): Promise<boolean> {
  if (USE_MOCK_DATA) return false
  try {
    const response = await fetch('/api/foundry')
    const data = await response.json()
    return data.configured === true
  } catch {
    return false
  }
}

export function getDataSource(): 'foundry' | 'mock' {
  return USE_MOCK_DATA ? 'mock' : 'foundry'
}
