/**
 * Scenario Modeling data fetchers
 */

import { USE_MOCK_DATA, mock, executeFoundrySQL } from './core'
import type {
  ScenarioSummary,
  WinRateScenario,
  DealSizeScenario,
  CycleTimeScenario,
} from '@/types'

export async function getScenarioSummary(): Promise<ScenarioSummary> {
  if (USE_MOCK_DATA) {
    return mock('scenarioSummary')
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Analytics/scenario_summary\`
      ORDER BY snapshot_date DESC
      LIMIT 1
    `,
  })

  if (results.length === 0) {
    throw new Error('No scenario summary found')
  }

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
  if (USE_MOCK_DATA) {
    return mock('winRateScenarios')
  }

  return executeFoundrySQL<WinRateScenario>({
    query: `
      SELECT
        scenario_name as scenarioName,
        description,
        current_win_rate as currentWinRate,
        projected_win_rate as projectedWinRate,
        improvement_pct as improvementPct,
        revenue_impact as revenueImpact,
        implementation_effort as implementationEffort,
        time_to_impact as timeToImpact,
        confidence,
        key_actions as keyActions
      FROM \`/RevOps/Analytics/scenario_win_rate_impact\`
      ORDER BY revenue_impact DESC
    `,
  })
}

export async function getDealSizeScenarios(): Promise<DealSizeScenario[]> {
  if (USE_MOCK_DATA) {
    return mock('dealSizeScenarios')
  }

  return executeFoundrySQL<DealSizeScenario>({
    query: `
      SELECT
        scenario_name as scenarioName,
        description,
        current_avg_deal as currentAvgDeal,
        projected_avg_deal as projectedAvgDeal,
        improvement_pct as improvementPct,
        revenue_impact as revenueImpact,
        implementation_effort as implementationEffort,
        time_to_impact as timeToImpact,
        confidence,
        key_actions as keyActions
      FROM \`/RevOps/Analytics/scenario_deal_size_impact\`
      ORDER BY revenue_impact DESC
    `,
  })
}

export async function getCycleTimeScenarios(): Promise<CycleTimeScenario[]> {
  if (USE_MOCK_DATA) {
    return mock('cycleTimeScenarios')
  }

  return executeFoundrySQL<CycleTimeScenario>({
    query: `
      SELECT
        scenario_name as scenarioName,
        description,
        current_cycle as currentCycle,
        projected_cycle as projectedCycle,
        reduction_days as reductionDays,
        throughput_increase as throughputIncrease,
        revenue_impact as revenueImpact,
        implementation_effort as implementationEffort,
        time_to_impact as timeToImpact,
        confidence,
        key_actions as keyActions
      FROM \`/RevOps/Analytics/scenario_cycle_time_impact\`
      ORDER BY revenue_impact DESC
    `,
  })
}

export async function getScenarioBaseMetrics(): Promise<any> {
  if (USE_MOCK_DATA) {
    return null
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        baseline_win_rate as baselineWinRate,
        baseline_deal_size as baselineDealSize,
        baseline_cycle_days as baselineCycleDays,
        baseline_pipeline as baselinePipeline,
        baseline_conversion_rate as baselineConversionRate,
        historical_win_rate_trend as historicalWinRateTrend,
        historical_deal_size_trend as historicalDealSizeTrend,
        historical_cycle_trend as historicalCycleTrend,
        segment_breakdown as segmentBreakdown,
        quarter,
        snapshot_date as snapshotDate
      FROM \`/RevOps/Analytics/scenario_base_metrics\`
      ORDER BY snapshot_date DESC
      LIMIT 1
    `,
  })

  return results[0] || null
}
