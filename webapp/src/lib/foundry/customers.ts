/**
 * Customer Health data fetchers
 */

import { USE_MOCK_DATA, mock, executeFoundrySQL } from './core'
import type {
  CustomerHealthSummary,
  CustomerHealth,
  ExpansionOpportunity,
  ChurnRiskAccount,
} from '@/types'

export async function getCustomerHealthSummary(): Promise<CustomerHealthSummary> {
  if (USE_MOCK_DATA) {
    return mock('customerHealthSummary')
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Analytics/customer_health_summary\`
      ORDER BY snapshot_date DESC
      LIMIT 1
    `,
  })

  if (results.length === 0) {
    throw new Error('No customer health summary found')
  }

  const row = results[0]
  return {
    totalCustomers: row.total_customers,
    totalArr: row.total_arr,
    avgHealthScore: row.avg_health_score,
    healthyCount: row.healthy_count,
    monitorCount: row.monitor_count,
    atRiskCount: row.at_risk_count,
    criticalCount: row.critical_count,
    healthyArr: row.healthy_arr,
    atRiskArr: row.at_risk_arr,
    renewals90d: row.renewals_90d,
    renewalArr90d: row.renewal_arr_90d,
    totalExpansionPotential: row.total_expansion_potential,
    highExpansionCount: row.high_expansion_count,
    highExpansionValue: row.high_expansion_value,
    churnRiskCount: row.churn_risk_count,
    totalArrAtRisk: row.total_arr_at_risk,
    netRetentionForecast: row.net_retention_forecast,
    snapshotDate: row.snapshot_date,
  }
}

export async function getCustomerHealth(): Promise<CustomerHealth[]> {
  if (USE_MOCK_DATA) {
    return mock('customerHealth')
  }

  return executeFoundrySQL<CustomerHealth>({
    query: `
      SELECT
        customer_id as customerId,
        customer_name as customerName,
        industry,
        segment,
        arr,
        health_score as healthScore,
        health_status as healthStatus,
        nps_score as npsScore,
        usage_score as usageScore,
        engagement_score as engagementScore,
        support_score as supportScore,
        renewal_date as renewalDate,
        days_to_renewal as daysToRenewal,
        csm_name as csmName,
        last_qbr_date as lastQbrDate,
        expansion_potential as expansionPotential,
        churn_risk as churnRisk,
        key_risks as keyRisks,
        recommended_actions as recommendedActions
      FROM \`/RevOps/Analytics/customer_health_scores\`
      ORDER BY health_score ASC
    `,
  })
}

export async function getExpansionOpportunities(): Promise<ExpansionOpportunity[]> {
  if (USE_MOCK_DATA) {
    return mock('expansionOpportunities')
  }

  return executeFoundrySQL<ExpansionOpportunity>({
    query: `
      SELECT
        customer_id as customerId,
        customer_name as customerName,
        current_arr as currentArr,
        expansion_potential as expansionPotential,
        expansion_type as expansionType,
        probability,
        target_products as targetProducts,
        health_score as healthScore,
        usage_trend as usageTrend,
        recommended_approach as recommendedApproach,
        csm_name as csmName,
        ae_name as aeName,
        next_step as nextStep,
        target_close_date as targetCloseDate
      FROM \`/RevOps/Analytics/expansion_opportunities\`
      ORDER BY expansion_potential DESC
    `,
  })
}

export async function getChurnRiskAccounts(): Promise<ChurnRiskAccount[]> {
  if (USE_MOCK_DATA) {
    return mock('churnRiskAccounts')
  }

  return executeFoundrySQL<ChurnRiskAccount>({
    query: `
      SELECT
        customer_id as customerId,
        customer_name as customerName,
        arr,
        health_score as healthScore,
        churn_probability as churnProbability,
        churn_risk_tier as churnRiskTier,
        renewal_date as renewalDate,
        days_to_renewal as daysToRenewal,
        primary_risk_factors as primaryRiskFactors,
        usage_trend as usageTrend,
        engagement_trend as engagementTrend,
        support_tickets_30d as supportTickets30d,
        nps_score as npsScore,
        csm_name as csmName,
        save_actions as saveActions,
        executive_sponsor as executiveSponsor,
        last_exec_meeting as lastExecMeeting
      FROM \`/RevOps/Analytics/churn_risk_analysis\`
      ORDER BY churn_probability DESC
    `,
  })
}

// ============================================================================
// ML CHURN PREDICTIONS
// ============================================================================

export async function getChurnCohorts(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        cohort_id as cohortId,
        cohort_name as cohortName,
        description,
        customer_count as customerCount,
        total_arr as totalArr,
        avg_churn_probability as avgChurnProbability,
        churn_risk_tier as churnRiskTier,
        common_risk_factors as commonRiskFactors,
        avg_health_score as avgHealthScore,
        avg_usage_trend as avgUsageTrend,
        avg_engagement_score as avgEngagementScore,
        recommended_intervention as recommendedIntervention,
        intervention_priority as interventionPriority,
        expected_save_rate as expectedSaveRate,
        snapshot_date as snapshotDate
      FROM \`/RevOps/ML/churn_cohorts\`
      ORDER BY total_arr DESC
    `,
  })
}
