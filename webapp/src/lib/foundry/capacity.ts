/**
 * Capacity Planning data fetchers
 */

import { USE_MOCK_DATA, mock, executeFoundrySQL, type DashboardFilters } from './core'
import type {
  CapacityPlanningSummary,
  RepCapacity,
  TeamCapacitySummary,
  RampCohortAnalysis,
} from '@/types'

export async function getCapacityPlanningSummary(): Promise<CapacityPlanningSummary> {
  if (USE_MOCK_DATA) {
    return mock('capacityPlanningSummary')
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Analytics/capacity_planning_summary\`
      ORDER BY snapshot_date DESC
      LIMIT 1
    `,
  })

  if (results.length === 0) {
    throw new Error('No capacity planning summary found')
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
    const capacity = await mock('repCapacity')
    let filtered = [...capacity]
    if (filters?.team && filters.team !== 'All Teams') {
      filtered = filtered.filter(r => r.region === filters.team)
    }
    return filtered
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE region = '${filters.team}'` : ''

  return executeFoundrySQL<RepCapacity>({
    query: `
      SELECT
        rep_id as repId,
        rep_name as repName,
        region,
        segment,
        tenure_months as tenureMonths,
        ramp_status as rampStatus,
        ramp_pct as rampPct,
        quota,
        effective_quota as effectiveQuota,
        closed_won as closedWon,
        pipeline,
        attainment,
        utilization,
        productivity_score as productivityScore,
        capacity_status as capacityStatus,
        deals_in_progress as dealsInProgress,
        avg_deal_size as avgDealSize,
        recommended_action as recommendedAction
      FROM \`/RevOps/Analytics/rep_capacity_model\`
      ${teamFilter}
      ORDER BY utilization DESC
    `,
  })
}

export async function getTeamCapacitySummary(filters?: DashboardFilters): Promise<TeamCapacitySummary[]> {
  if (USE_MOCK_DATA) {
    const summary = await mock('teamCapacitySummary')
    let filtered = [...summary]
    if (filters?.team && filters.team !== 'All Teams') {
      filtered = filtered.filter(t => t.region === filters.team)
    }
    return filtered
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE region = '${filters.team}'` : ''

  return executeFoundrySQL<TeamCapacitySummary>({
    query: `
      SELECT
        team_id as teamId,
        team_name as teamName,
        region,
        manager_name as managerName,
        total_reps as totalReps,
        ramped_reps as rampedReps,
        total_quota as totalQuota,
        effective_quota as effectiveQuota,
        total_closed as totalClosed,
        total_pipeline as totalPipeline,
        avg_attainment as avgAttainment,
        avg_utilization as avgUtilization,
        capacity_status as capacityStatus,
        headcount_gap as headcountGap,
        recommended_hires as recommendedHires
      FROM \`/RevOps/Analytics/team_capacity_summary\`
      ${teamFilter}
      ORDER BY avg_attainment DESC
    `,
  })
}

export async function getRampCohortAnalysis(): Promise<RampCohortAnalysis[]> {
  if (USE_MOCK_DATA) {
    return mock('rampCohortAnalysis')
  }

  return executeFoundrySQL<RampCohortAnalysis>({
    query: `
      SELECT
        cohort_name as cohortName,
        start_date as startDate,
        total_reps as totalReps,
        avg_months_to_ramp as avgMonthsToRamp,
        on_track_count as onTrackCount,
        behind_count as behindCount,
        avg_quota_pct as avgQuotaPct,
        avg_pipeline_coverage as avgPipelineCoverage,
        projected_ramp_date as projectedRampDate,
        cohort_health as cohortHealth
      FROM \`/RevOps/Analytics/ramp_time_analysis\`
      ORDER BY start_date DESC
    `,
  })
}
