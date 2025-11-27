/**
 * Pipeline Health Dashboard data fetchers
 */

import { USE_MOCK_DATA, mock, executeFoundrySQL, type DashboardFilters } from './core'
import type {
  DashboardKPIs,
  ForecastTrendPoint,
  StuckDeal,
  HealthDistribution,
  ProcessBottleneck,
  CompetitiveLoss,
  StageVelocity,
  FunnelStage,
} from '@/types'

export async function getDashboardKPIs(filters?: DashboardFilters): Promise<DashboardKPIs> {
  if (USE_MOCK_DATA) {
    return mock('dashboardKPIs')
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
    return mock('forecastTrend')
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
    const mockStuckDeals = await mock('stuckDeals')
    let deals = mockStuckDeals
    if (filters?.team && filters.team !== 'All Teams') {
      const ownerTeams: Record<string, string> = {
        'Sarah Chen': 'West',
        'David Kim': 'East',
        'Emily Rodriguez': 'Central',
        'James Wilson': 'South',
      }
      deals = deals.filter(d => ownerTeams[d.ownerName] === filters.team)
    }
    return deals
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE region = '${filters.team}'` : ''

  return executeFoundrySQL<StuckDeal>({
    query: `
      SELECT
        opportunity_id as id,
        account_name as accountName,
        opportunity_name as opportunityName,
        owner_name as ownerName,
        amount,
        stage_name as stageName,
        days_in_stage as daysInStage,
        health_score as healthScore,
        days_since_activity as daysSinceActivity,
        risk_flags as riskFlags,
        urgency,
        primary_action as primaryAction
      FROM \`/RevOps/Analytics/deals_stuck_in_process\`
      ${teamFilter}
      ORDER BY urgency DESC, amount DESC
    `,
  })
}

export async function getHealthDistribution(filters?: DashboardFilters): Promise<HealthDistribution> {
  if (USE_MOCK_DATA) {
    // Mock data doesn't have quarter/team breakdown - returns demo aggregate
    return mock('healthDistribution')
  }

  const quarterFilter = filters?.quarter || 'Q4 2024'
  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `AND region = '${filters.team}'` : ''

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Analytics/pipeline_health_summary\`
      WHERE quarter = '${quarterFilter}'
      ${teamFilter}
      LIMIT 1
    `,
  })

  if (results.length === 0) {
    throw new Error('No health distribution data found')
  }

  const row = results[0]
  return {
    healthy: { count: row.healthy_count, amount: row.healthy_amount, pct: row.healthy_pct },
    monitor: { count: row.monitor_count, amount: row.monitor_amount, pct: row.monitor_pct },
    atRisk: { count: row.at_risk_count, amount: row.at_risk_amount, pct: row.at_risk_pct },
    critical: { count: row.critical_count, amount: row.critical_amount, pct: row.critical_pct },
    trend: row.trend,
  }
}

export async function getProcessBottlenecks(filters?: DashboardFilters): Promise<ProcessBottleneck[]> {
  if (USE_MOCK_DATA) {
    // Mock data doesn't have quarter/team breakdown - returns demo aggregate
    return mock('processBottlenecks')
  }

  const quarterFilter = filters?.quarter || 'Q4 2024'
  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `AND region = '${filters.team}'` : ''

  return executeFoundrySQL<ProcessBottleneck>({
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
      WHERE quarter = '${quarterFilter}'
      ${teamFilter}
      ORDER BY severity DESC, delay_ratio DESC
    `,
  })
}

export async function getCompetitiveLosses(filters?: DashboardFilters): Promise<CompetitiveLoss[]> {
  if (USE_MOCK_DATA) {
    // Mock data doesn't have quarter/team breakdown - returns demo aggregate
    return mock('competitiveLosses')
  }

  const quarterFilter = filters?.quarter || 'Q4 2024'
  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `AND region = '${filters.team}'` : ''

  return executeFoundrySQL<CompetitiveLoss>({
    query: `
      SELECT
        competitor,
        deals_lost as dealsLost,
        revenue_lost as revenueLost,
        loss_reasons as lossReasons,
        affected_segments as affectedSegments
      FROM \`/RevOps/Analytics/competitive_battles\`
      WHERE quarter = '${quarterFilter}'
      ${teamFilter}
      ORDER BY revenue_lost DESC
    `,
  })
}

export async function getStageVelocity(filters?: DashboardFilters): Promise<StageVelocity[]> {
  if (USE_MOCK_DATA) {
    // Mock data doesn't have quarter/team breakdown - returns demo aggregate
    return mock('stageVelocity')
  }

  const quarterFilter = filters?.quarter || 'Q4 2024'
  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `AND region = '${filters.team}'` : ''

  return executeFoundrySQL<StageVelocity>({
    query: `
      SELECT
        stage_name as stageName,
        stage_order as stageOrder,
        avg_duration as avgDuration,
        benchmark,
        deviation,
        trend_direction as trendDirection,
        conversion_rate as conversionRate,
        deals_in_stage as dealsInStage,
        amount_in_stage as amountInStage
      FROM \`/RevOps/Analytics/stage_velocity\`
      WHERE quarter = '${quarterFilter}'
      ${teamFilter}
      ORDER BY stage_order
    `,
  })
}

export async function getPipelineFunnel(filters?: DashboardFilters): Promise<FunnelStage[]> {
  if (USE_MOCK_DATA) {
    // Mock data doesn't have quarter/team breakdown - returns demo aggregate
    return mock('pipelineFunnel')
  }

  const quarterFilter = filters?.quarter || 'Q4 2024'
  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `AND region = '${filters.team}'` : ''

  return executeFoundrySQL<FunnelStage>({
    query: `
      SELECT
        name,
        amount,
        count,
        conversion_rate as conversionRate
      FROM \`/RevOps/Analytics/pipeline_funnel\`
      WHERE quarter = '${quarterFilter}'
      ${teamFilter}
      ORDER BY stage_order
    `,
  })
}

// ============================================================================
// PIPELINE SUMMARY & ANALYTICS
// ============================================================================

export async function getPipelineSummary(filters?: DashboardFilters): Promise<any> {
  if (USE_MOCK_DATA) {
    return null
  }

  const quarterFilter = filters?.quarter || 'Q4 2024'
  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `AND region = '${filters.team}'` : ''

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        total_pipeline as totalPipeline,
        total_opportunities as totalOpportunities,
        avg_deal_size as avgDealSize,
        avg_sales_cycle as avgSalesCycle,
        pipeline_by_stage as pipelineByStage,
        pipeline_by_segment as pipelineBySegment,
        created_this_quarter as createdThisQuarter,
        closed_won_this_quarter as closedWonThisQuarter,
        closed_lost_this_quarter as closedLostThisQuarter,
        win_rate as winRate,
        velocity_score as velocityScore,
        health_score as healthScore,
        snapshot_date as snapshotDate
      FROM \`/RevOps/Analytics/pipeline_summary\`
      WHERE quarter = '${quarterFilter}'
      ${teamFilter}
      ORDER BY snapshot_date DESC
      LIMIT 1
    `,
  })

  return results[0] || null
}

export async function getReworkAnalysis(filters?: DashboardFilters): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  const quarterFilter = filters?.quarter || 'Q4 2024'
  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `AND region = '${filters.team}'` : ''

  return executeFoundrySQL<any>({
    query: `
      SELECT
        opportunity_id as opportunityId,
        account_name as accountName,
        opportunity_name as opportunityName,
        owner_name as ownerName,
        amount,
        rework_type as reworkType,
        rework_count as reworkCount,
        stage_reversals as stageReversals,
        close_date_changes as closeDateChanges,
        amount_changes as amountChanges,
        days_in_rework as daysInRework,
        cost_estimate as costEstimate,
        root_cause as rootCause,
        recommended_action as recommendedAction
      FROM \`/RevOps/Analytics/rework_analysis\`
      WHERE quarter = '${quarterFilter}'
      ${teamFilter}
      ORDER BY rework_count DESC, amount DESC
    `,
  })
}
