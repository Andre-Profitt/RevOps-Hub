/**
 * QBR (Quarterly Business Review) data fetchers
 */

import { USE_MOCK_DATA, mock, executeFoundrySQL } from './core'
import type {
  QBRExecutiveSummary,
  QBRRepPerformance,
  QBRWinLossAnalysis,
} from '@/types'

export async function getQBRExecutiveSummary(quarter?: string): Promise<QBRExecutiveSummary> {
  if (USE_MOCK_DATA) {
    return mock('qbrExecutiveSummary')
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
    throw new Error('No QBR executive summary found')
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
    return mock('qbrRepPerformance')
  }

  const quarterFilter = quarter ? `WHERE quarter = '${quarter}'` : ''

  return executeFoundrySQL<QBRRepPerformance>({
    query: `
      SELECT
        rep_id as repId,
        rep_name as repName,
        region,
        segment,
        quota,
        closed_won as closedWon,
        attainment,
        win_rate as winRate,
        avg_deal_size as avgDealSize,
        avg_sales_cycle as avgSalesCycle,
        pipeline,
        pipeline_coverage as pipelineCoverage,
        health_score as healthScore,
        performance_tier as performanceTier,
        trend,
        top_deal as topDeal,
        top_deal_amount as topDealAmount
      FROM \`/RevOps/Analytics/qbr_performance_summary\`
      ${quarterFilter}
      ORDER BY attainment DESC
    `,
  })
}

export async function getQBRWinLossAnalysis(quarter?: string): Promise<QBRWinLossAnalysis> {
  if (USE_MOCK_DATA) {
    return mock('qbrWinLossAnalysis')
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
    throw new Error('No QBR win/loss analysis found')
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
