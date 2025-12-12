/**
 * ACTION INBOX DATA FETCHING
 * ==========================
 *
 * Fetches next-best-actions and impact tracker data from Foundry.
 *
 * Datasets:
 * - /RevOps/Dashboard/next_best_actions → DashboardActionItem[]
 * - /RevOps/Inbox/impact_tracker → InboxImpactMetrics[]
 */

import type { DashboardActionItem, InboxImpactMetrics } from '@/types'
import { USE_MOCK_DATA, mock, executeFoundrySQL, DashboardFilters, buildFilterClause } from './core'

// =====================================================
// DASHBOARD ACTION ITEMS
// =====================================================

export async function fetchDashboardActions(
  filters?: DashboardFilters
): Promise<DashboardActionItem[]> {
  if (USE_MOCK_DATA) {
    const data = await mock('dashboardActions')
    // Apply client-side filtering if filters provided
    if (filters && filters.team && filters.team !== 'All Teams') {
      return data.filter((action) => action.region === filters.team)
    }
    return data
  }

  const whereClause = filters ? buildFilterClause(filters, 'quarter', 'region') : '1=1'

  const query = `
    SELECT
      action_id AS "actionId",
      opportunity_id AS "opportunityId",
      account_id AS "accountId",
      account_name AS "accountName",
      opportunity_name AS "opportunityName",
      rep_id AS "repId",
      owner_name AS "ownerName",
      region,
      segment,
      action_type AS "actionType",
      action_title AS "actionTitle",
      action_description AS "actionDescription",
      priority,
      expected_impact AS "expectedImpact",
      expected_impact_amount AS "expectedImpactAmount",
      confidence,
      reasoning,
      due_date AS "dueDate",
      created_at AS "createdAt",
      status,
      source,
      playbook,
      amount,
      health_score AS "healthScore",
      health_category AS "healthCategory",
      risk_level AS "riskLevel",
      needs_attention AS "needsAttention"
    FROM \`/RevOps/Dashboard/next_best_actions\`
    WHERE ${whereClause}
      AND status = 'Open'
    ORDER BY priority DESC, created_at DESC
  `

  return executeFoundrySQL<DashboardActionItem>({ query })
}

// =====================================================
// ACTION INBOX IMPACT METRICS
// =====================================================

export async function fetchImpactTracker(
  filters?: DashboardFilters
): Promise<InboxImpactMetrics[]> {
  if (USE_MOCK_DATA) {
    const data = await mock('impactTracker')
    // Apply client-side filtering if filters provided
    if (filters) {
      return data.filter((metric) => {
        const teamMatch =
          !filters.team || filters.team === 'All Teams' || metric.region === filters.team
        const quarterMatch = !filters.quarter || metric.quarter === filters.quarter
        return teamMatch && quarterMatch
      })
    }
    return data
  }

  const whereClause = filters ? buildFilterClause(filters, 'quarter', 'region') : '1=1'

  const query = `
    SELECT
      quarter,
      region,
      segment,
      actions_created AS "actionsCreated",
      actions_with_decision AS "actionsWithDecision",
      actions_without_decision AS "actionsWithoutDecision",
      avg_time_to_decision_days AS "avgTimeToDecisionDays",
      total_expected_impact_amount AS "totalExpectedImpactAmount",
      total_realized_impact_amount AS "totalRealizedImpactAmount",
      deals_won AS "dealsWon",
      deals_lost AS "dealsLost",
      deals_slipped AS "dealsSlipped",
      decision_rate AS "decisionRate",
      realized_vs_expected_ratio AS "realizedVsExpectedRatio",
      last_updated_ts AS "lastUpdatedTs"
    FROM \`/RevOps/Inbox/impact_tracker\`
    WHERE ${whereClause}
    ORDER BY quarter DESC, region, segment
  `

  return executeFoundrySQL<InboxImpactMetrics>({ query })
}

// =====================================================
// AGGREGATED IMPACT SUMMARY
// =====================================================

export interface ImpactSummary {
  totalActionsCreated: number
  totalActionsActioned: number
  decisionRate: number
  avgTimeToDecisionDays: number
  totalExpectedImpact: number
  totalRealizedImpact: number
  realizedVsExpectedRatio: number
  dealsWon: number
  dealsLost: number
  dealsSlipped: number
}

export async function fetchImpactSummary(
  filters?: DashboardFilters
): Promise<ImpactSummary> {
  const metrics = await fetchImpactTracker(filters)

  if (metrics.length === 0) {
    return {
      totalActionsCreated: 0,
      totalActionsActioned: 0,
      decisionRate: 0,
      avgTimeToDecisionDays: 0,
      totalExpectedImpact: 0,
      totalRealizedImpact: 0,
      realizedVsExpectedRatio: 0,
      dealsWon: 0,
      dealsLost: 0,
      dealsSlipped: 0,
    }
  }

  const totalActionsCreated = metrics.reduce((sum, m) => sum + m.actionsCreated, 0)
  const totalActionsActioned = metrics.reduce((sum, m) => sum + m.actionsWithDecision, 0)
  const totalExpectedImpact = metrics.reduce((sum, m) => sum + m.totalExpectedImpactAmount, 0)
  const totalRealizedImpact = metrics.reduce((sum, m) => sum + m.totalRealizedImpactAmount, 0)
  const dealsWon = metrics.reduce((sum, m) => sum + m.dealsWon, 0)
  const dealsLost = metrics.reduce((sum, m) => sum + m.dealsLost, 0)
  const dealsSlipped = metrics.reduce((sum, m) => sum + m.dealsSlipped, 0)

  // Weighted average for time to decision
  const weightedTimeSum = metrics.reduce(
    (sum, m) => sum + m.avgTimeToDecisionDays * m.actionsWithDecision,
    0
  )
  const avgTimeToDecisionDays =
    totalActionsActioned > 0 ? weightedTimeSum / totalActionsActioned : 0

  return {
    totalActionsCreated,
    totalActionsActioned,
    decisionRate: totalActionsCreated > 0 ? totalActionsActioned / totalActionsCreated : 0,
    avgTimeToDecisionDays,
    totalExpectedImpact,
    totalRealizedImpact,
    realizedVsExpectedRatio: totalExpectedImpact > 0 ? totalRealizedImpact / totalExpectedImpact : 0,
    dealsWon,
    dealsLost,
    dealsSlipped,
  }
}
