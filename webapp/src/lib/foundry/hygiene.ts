/**
 * Pipeline Hygiene data fetchers
 */

import { USE_MOCK_DATA, mock, executeFoundrySQL, type DashboardFilters } from './core'
import type {
  HygieneAlert,
  HygieneSummary,
  HygieneByOwner,
  HygieneTrendPoint,
} from '@/types'

export async function getHygieneAlerts(filters?: DashboardFilters): Promise<HygieneAlert[]> {
  if (USE_MOCK_DATA) {
    const alerts = await mock('hygieneAlerts')
    let filtered = [...alerts]
    if (filters?.team && filters.team !== 'All Teams') {
      const ownerTeams: Record<string, string> = {
        'Sarah Chen': 'West',
        'David Kim': 'East',
        'Emily Rodriguez': 'Central',
        'James Wilson': 'South',
      }
      filtered = filtered.filter(a => ownerTeams[a.ownerName] === filters.team)
    }
    return filtered
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE region = '${filters.team}'` : ''

  return executeFoundrySQL<HygieneAlert>({
    query: `
      SELECT
        opportunity_id as opportunityId,
        account_name as accountName,
        opportunity_name as opportunityName,
        owner_name as ownerName,
        amount,
        stage_name as stageName,
        days_in_stage as daysInStage,
        hygiene_score as hygieneScore,
        primary_alert as primaryAlert,
        alert_severity as alertSeverity,
        stale_close_date as staleCloseDate,
        single_threaded as singleThreaded,
        missing_next_steps as missingNextSteps,
        gone_dark as goneDark,
        missing_champion as missingChampion,
        stalled_stage as stalledStage,
        recommended_action as recommendedAction
      FROM \`/RevOps/Analytics/pipeline_hygiene_alerts\`
      ${teamFilter}
      ORDER BY alert_severity DESC, amount DESC
    `,
  })
}

export async function getHygieneSummary(filters?: DashboardFilters): Promise<HygieneSummary> {
  if (USE_MOCK_DATA) {
    return mock('hygieneSummary')
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE region = '${filters.team}'` : ''

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Analytics/pipeline_hygiene_summary\`
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
    return mock('hygieneByOwner')
  }

  return executeFoundrySQL<HygieneByOwner>({
    query: `
      SELECT
        owner_id as ownerId,
        owner_name as ownerName,
        opportunity_count as opportunityCount,
        avg_hygiene_score as avgHygieneScore,
        clean_count as cleanCount,
        issues_count as issuesCount,
        critical_count as criticalCount,
        total_pipeline as totalPipeline,
        at_risk_pipeline as atRiskPipeline
      FROM \`/RevOps/Analytics/pipeline_hygiene_by_owner\`
      ORDER BY avg_hygiene_score ASC
    `,
  })
}

export async function getHygieneTrends(): Promise<HygieneTrendPoint[]> {
  if (USE_MOCK_DATA) {
    return mock('hygieneTrends')
  }

  return executeFoundrySQL<HygieneTrendPoint>({
    query: `
      SELECT
        snapshot_date as snapshotDate,
        week_number as weekNumber,
        avg_hygiene_score as avgHygieneScore,
        total_opportunities as totalOpportunities,
        clean_count as cleanCount,
        issues_count as issuesCount,
        critical_count as criticalCount,
        total_alerts as totalAlerts
      FROM \`/RevOps/Analytics/pipeline_hygiene_trends\`
      ORDER BY snapshot_date
    `,
  })
}

export async function getHygieneAlertFeed(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        alert_id as alertId,
        opportunity_id as opportunityId,
        opportunity_name as opportunityName,
        account_name as accountName,
        owner_name as ownerName,
        alert_type as alertType,
        alert_severity as alertSeverity,
        alert_message as alertMessage,
        amount,
        created_at as createdAt,
        acknowledged,
        acknowledged_by as acknowledgedBy
      FROM \`/RevOps/Analytics/hygiene_alert_feed\`
      WHERE acknowledged = false
      ORDER BY alert_severity DESC, created_at DESC
      LIMIT 50
    `,
  })
}
