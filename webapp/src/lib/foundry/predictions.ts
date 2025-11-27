/**
 * AI Predictions and Leading Indicators data fetchers
 */

import { USE_MOCK_DATA, mock, executeFoundrySQL, type DashboardFilters } from './core'
import type {
  DealPrediction,
  LeadingIndicatorsSummary,
} from '@/types'

export async function getDealPredictions(filters?: DashboardFilters): Promise<DealPrediction[]> {
  if (USE_MOCK_DATA) {
    const predictions = await mock('dealPredictions')
    let filtered = [...predictions]
    if (filters?.team && filters.team !== 'All Teams') {
      const ownerTeams: Record<string, string> = {
        'Sarah Chen': 'West',
        'David Kim': 'East',
        'Emily Rodriguez': 'Central',
        'James Wilson': 'South',
      }
      filtered = filtered.filter(p => ownerTeams[p.ownerName] === filters.team)
    }
    return filtered
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE region = '${filters.team}'` : ''

  return executeFoundrySQL<DealPrediction>({
    query: `
      SELECT
        opportunity_id as opportunityId,
        account_name as accountName,
        opportunity_name as opportunityName,
        owner_name as ownerName,
        amount,
        stage_name as stageName,
        close_date as closeDate,
        win_probability as winProbability,
        win_probability_tier as winProbabilityTier,
        slip_risk_score as slipRiskScore,
        slip_risk_tier as slipRiskTier,
        slip_risk_amount as slipRiskAmount,
        weighted_amount as weightedAmount,
        activity_score as activityScore,
        engagement_score as engagementScore,
        velocity_score as velocityScore,
        prediction_confidence as predictionConfidence,
        prediction_summary as predictionSummary,
        key_risk_factors as keyRiskFactors
      FROM \`/RevOps/Analytics/deal_predictions\`
      ${teamFilter}
      ORDER BY win_probability DESC
    `,
  })
}

export async function getLeadingIndicatorsSummary(filters?: DashboardFilters): Promise<LeadingIndicatorsSummary> {
  if (USE_MOCK_DATA) {
    return mock('leadingIndicatorsSummary')
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE region = '${filters.team}'` : ''

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Analytics/leading_indicators_summary\`
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

// ============================================================================
// NEXT BEST ACTIONS & ML RECOMMENDATIONS
// ============================================================================

export async function getNextBestActionsDashboard(filters?: DashboardFilters): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE region = '${filters.team}'` : ''

  return executeFoundrySQL<any>({
    query: `
      SELECT
        action_id as actionId,
        opportunity_id as opportunityId,
        account_id as accountId,
        account_name as accountName,
        opportunity_name as opportunityName,
        owner_name as ownerName,
        action_type as actionType,
        action_title as actionTitle,
        action_description as actionDescription,
        priority,
        expected_impact as expectedImpact,
        expected_impact_amount as expectedImpactAmount,
        confidence,
        reasoning,
        due_date as dueDate,
        created_at as createdAt
      FROM \`/RevOps/Dashboard/next_best_actions\`
      ${teamFilter}
      ORDER BY priority DESC, expected_impact_amount DESC
      LIMIT 50
    `,
  })
}

export async function getAccountRecommendations(accountId?: string): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  const accountFilter = accountId ? `WHERE account_id = '${accountId}'` : ''

  return executeFoundrySQL<any>({
    query: `
      SELECT
        recommendation_id as recommendationId,
        account_id as accountId,
        account_name as accountName,
        recommendation_type as recommendationType,
        title,
        description,
        expected_arr_impact as expectedArrImpact,
        probability,
        priority,
        confidence,
        based_on as basedOn,
        action_steps as actionSteps,
        assigned_to as assignedTo,
        due_date as dueDate,
        created_at as createdAt
      FROM \`/RevOps/ML/account_recommendations\`
      ${accountFilter}
      ORDER BY priority DESC, expected_arr_impact DESC
    `,
  })
}
