/**
 * Forecasting Hub data fetchers
 */

import { USE_MOCK_DATA, mock, executeFoundrySQL } from './core'
import type {
  ForecastSummary,
  ForecastBySegment,
  ForecastHistoryPoint,
  ForecastAccuracy,
} from '@/types'

export async function getForecastSummary(): Promise<ForecastSummary> {
  if (USE_MOCK_DATA) {
    return mock('forecastSummary')
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Analytics/forecast_summary\`
      ORDER BY snapshot_date DESC
      LIMIT 1
    `,
  })

  if (results.length === 0) {
    throw new Error('No forecast summary found')
  }

  const row = results[0]
  return {
    quota: row.quota,
    totalPipeline: row.total_pipeline,
    pipelineCoverage: row.pipeline_coverage,
    bottomUpForecast: row.bottom_up_forecast,
    aiForecast: row.ai_forecast,
    bestCaseForecast: row.best_case_forecast,
    highConfidenceAmount: row.high_confidence_amount,
    methodologyVariancePct: row.methodology_variance_pct,
    recommendedMethod: row.recommended_method,
    methodAccuracy: row.method_accuracy,
    gapToTarget: row.gap_to_target,
    closedWonYtd: row.closed_won_ytd,
    topSegment: row.top_segment,
    topSegmentForecast: row.top_segment_forecast,
    forecastConfidence: row.forecast_confidence,
    weeksRemaining: row.weeks_remaining,
    requiredWeeklyClose: row.required_weekly_close,
    snapshotDate: row.snapshot_date,
  }
}

export async function getForecastBySegment(): Promise<ForecastBySegment[]> {
  if (USE_MOCK_DATA) {
    return mock('forecastBySegment')
  }

  return executeFoundrySQL<ForecastBySegment>({
    query: `
      SELECT
        segment,
        quota,
        closed_won as closedWon,
        pipeline,
        commit,
        best_case as bestCase,
        ai_forecast as aiForecast,
        attainment,
        gap_to_target as gapToTarget,
        coverage,
        avg_deal_size as avgDealSize,
        avg_cycle as avgCycle,
        win_rate as winRate
      FROM \`/RevOps/Analytics/forecast_by_segment\`
      ORDER BY ai_forecast DESC
    `,
  })
}

export async function getForecastHistory(): Promise<ForecastHistoryPoint[]> {
  if (USE_MOCK_DATA) {
    return mock('forecastHistory')
  }

  return executeFoundrySQL<ForecastHistoryPoint>({
    query: `
      SELECT
        snapshot_date as snapshotDate,
        week_number as weekNumber,
        bottom_up as bottomUp,
        ai_forecast as aiForecast,
        best_case as bestCase,
        actual_closed as actualClosed,
        quota
      FROM \`/RevOps/Analytics/forecast_history\`
      ORDER BY snapshot_date
    `,
  })
}

export async function getForecastAccuracy(): Promise<ForecastAccuracy[]> {
  if (USE_MOCK_DATA) {
    return mock('forecastAccuracy')
  }

  return executeFoundrySQL<ForecastAccuracy>({
    query: `
      SELECT
        methodology,
        quarter,
        forecast_amount as forecastAmount,
        actual_amount as actualAmount,
        variance,
        variance_pct as variancePct,
        accuracy_score as accuracyScore,
        accuracy_grade as accuracyGrade,
        historical_accuracy as historicalAccuracy
      FROM \`/RevOps/Analytics/forecast_accuracy\`
      ORDER BY accuracy_score DESC
    `,
  })
}

export async function getCompanyForecast(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Analytics/company_forecast\`
      ORDER BY snapshot_date DESC
    `,
  })
}

export async function getManagerForecast(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        manager_id as managerId,
        manager_name as managerName,
        team_quota as teamQuota,
        team_pipeline as teamPipeline,
        team_commit as teamCommit,
        team_best_case as teamBestCase,
        team_closed as teamClosed,
        coverage,
        attainment,
        rep_count as repCount
      FROM \`/RevOps/Analytics/manager_forecast\`
      ORDER BY team_pipeline DESC
    `,
  })
}

export async function getRegionForecast(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        region,
        quota,
        pipeline,
        commit,
        best_case as bestCase,
        closed,
        coverage,
        attainment,
        rep_count as repCount,
        avg_deal_size as avgDealSize
      FROM \`/RevOps/Analytics/region_forecast\`
      ORDER BY pipeline DESC
    `,
  })
}

export async function getSwingDeals(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        opportunity_id as opportunityId,
        opportunity_name as opportunityName,
        account_name as accountName,
        owner_name as ownerName,
        amount,
        stage_name as stageName,
        close_date as closeDate,
        win_probability as winProbability,
        swing_impact as swingImpact,
        risk_factors as riskFactors,
        recommended_action as recommendedAction
      FROM \`/RevOps/Analytics/swing_deals\`
      ORDER BY swing_impact DESC
      LIMIT 20
    `,
  })
}
