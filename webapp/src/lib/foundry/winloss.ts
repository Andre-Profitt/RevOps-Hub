/**
 * Win/Loss Analysis data fetchers
 */

import { USE_MOCK_DATA, mock, executeFoundrySQL } from './core'
import type {
  WinLossSummary,
  LossReason,
  WinFactor,
  CompetitiveBattle,
  WinLossBySegment,
} from '@/types'

export async function getWinLossSummary(): Promise<WinLossSummary> {
  if (USE_MOCK_DATA) {
    return mock('winLossSummary')
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Analytics/win_loss_summary\`
      ORDER BY snapshot_date DESC
      LIMIT 1
    `,
  })

  if (results.length === 0) {
    throw new Error('No win/loss summary found')
  }

  const row = results[0]
  return {
    totalClosed: row.total_closed,
    totalWon: row.total_won,
    totalLost: row.total_lost,
    wonRevenue: row.won_revenue,
    lostRevenue: row.lost_revenue,
    avgWonDealSize: row.avg_won_deal_size,
    avgLostDealSize: row.avg_lost_deal_size,
    avgWonCycle: row.avg_won_cycle,
    avgLostCycle: row.avg_lost_cycle,
    winRate: row.win_rate,
    lossRate: row.loss_rate,
    winRateByValue: row.win_rate_by_value,
    dealSizeGap: row.deal_size_gap,
    cycleGap: row.cycle_gap,
  }
}

export async function getLossReasons(): Promise<LossReason[]> {
  if (USE_MOCK_DATA) {
    return mock('lossReasons')
  }

  return executeFoundrySQL<LossReason>({
    query: `
      SELECT
        reason,
        category,
        deal_count as dealCount,
        total_value as totalValue,
        pct_of_losses as pctOfLosses,
        avg_deal_size as avgDealSize,
        avg_stage_lost as avgStageLost,
        trend,
        mitigation_actions as mitigationActions
      FROM \`/RevOps/Analytics/loss_reasons\`
      ORDER BY total_value DESC
    `,
  })
}

export async function getWinFactors(): Promise<WinFactor[]> {
  if (USE_MOCK_DATA) {
    return mock('winFactors')
  }

  return executeFoundrySQL<WinFactor>({
    query: `
      SELECT
        factor,
        category,
        correlation_score as correlationScore,
        win_rate_with as winRateWith,
        win_rate_without as winRateWithout,
        impact_score as impactScore,
        deal_count as dealCount,
        recommended_actions as recommendedActions
      FROM \`/RevOps/Analytics/win_factors\`
      ORDER BY correlation_score DESC
    `,
  })
}

export async function getCompetitiveBattles(): Promise<CompetitiveBattle[]> {
  if (USE_MOCK_DATA) {
    return mock('competitiveBattles')
  }

  return executeFoundrySQL<CompetitiveBattle>({
    query: `
      SELECT
        competitor,
        total_battles as totalBattles,
        wins,
        losses,
        win_rate as winRate,
        won_revenue as wonRevenue,
        lost_revenue as lostRevenue,
        avg_won_deal as avgWonDeal,
        avg_lost_deal as avgLostDeal,
        primary_loss_reasons as primaryLossReasons,
        primary_win_factors as primaryWinFactors,
        battlecard_link as battlecardLink,
        trend
      FROM \`/RevOps/Analytics/competitive_battles\`
      ORDER BY total_battles DESC
    `,
  })
}

export async function getWinLossBySegment(): Promise<WinLossBySegment[]> {
  if (USE_MOCK_DATA) {
    return mock('winLossBySegment')
  }

  return executeFoundrySQL<WinLossBySegment>({
    query: `
      SELECT
        segment,
        total_closed as totalClosed,
        wins,
        losses,
        win_rate as winRate,
        won_revenue as wonRevenue,
        lost_revenue as lostRevenue,
        avg_won_deal as avgWonDeal,
        avg_lost_deal as avgLostDeal,
        avg_won_cycle as avgWonCycle,
        avg_lost_cycle as avgLostCycle,
        top_loss_reason as topLossReason,
        top_win_factor as topWinFactor
      FROM \`/RevOps/Analytics/win_loss_by_segment\`
      ORDER BY won_revenue DESC
    `,
  })
}
