/**
 * Compensation & Attainment data fetchers
 */

import { USE_MOCK_DATA, mock, executeFoundrySQL } from './core'
import type {
  CompPlanSummary,
  RepAttainment,
  AttainmentTrend,
} from '@/types'

export async function getCompPlanSummary(): Promise<CompPlanSummary> {
  if (USE_MOCK_DATA) {
    return mock('compPlanSummary')
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Compensation/summary\`
      ORDER BY snapshot_date DESC
      LIMIT 1
    `,
  })

  if (results.length === 0) {
    throw new Error('No compensation summary found')
  }

  const row = results[0]
  return {
    totalQuota: row.total_quota,
    totalClosed: row.total_closed,
    teamAttainment: row.team_attainment,
    avgRepAttainment: row.avg_rep_attainment,
    onTrackCount: row.on_track_count,
    atRiskCount: row.at_risk_count,
    overachieversCount: row.overachievers_count,
    totalCommissionPaid: row.total_commission_paid,
    projectedCommission: row.projected_commission,
    acceleratorEligible: row.accelerator_eligible,
  }
}

export async function getRepAttainment(): Promise<RepAttainment[]> {
  if (USE_MOCK_DATA) {
    return mock('repAttainment')
  }

  return executeFoundrySQL<RepAttainment>({
    query: `
      SELECT
        rep_id as repId,
        rep_name as repName,
        region,
        segment,
        quota,
        closed_won as closedWon,
        attainment,
        commission_earned as commissionEarned,
        projected_commission as projectedCommission,
        accelerator_tier as acceleratorTier,
        accelerator_multiplier as acceleratorMultiplier,
        gap_to_accelerator as gapToAccelerator,
        status,
        pipeline,
        commit,
        projected_attainment as projectedAttainment
      FROM \`/RevOps/Compensation/rep_attainment\`
      ORDER BY attainment DESC
    `,
  })
}

export async function getAttainmentTrend(): Promise<AttainmentTrend[]> {
  if (USE_MOCK_DATA) {
    return mock('attainmentTrend')
  }

  return executeFoundrySQL<AttainmentTrend>({
    query: `
      SELECT
        snapshot_date as snapshotDate,
        week_number as weekNumber,
        team_attainment as teamAttainment,
        avg_rep_attainment as avgRepAttainment,
        on_track_pct as onTrackPct,
        at_risk_pct as atRiskPct,
        overachiever_pct as overachieverPct,
        quota_pace as quotaPace
      FROM \`/RevOps/Compensation/attainment_trend\`
      ORDER BY snapshot_date
    `,
  })
}
