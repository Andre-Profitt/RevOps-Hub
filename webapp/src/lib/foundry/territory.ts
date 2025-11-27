/**
 * Territory Planning data fetchers
 */

import { USE_MOCK_DATA, mock, executeFoundrySQL, type DashboardFilters } from './core'
import type {
  TerritorySummary,
  AccountScore,
  TerritoryBalance,
  WhiteSpaceAccount,
} from '@/types'

export async function getTerritorySummary(filters?: DashboardFilters): Promise<TerritorySummary> {
  if (USE_MOCK_DATA) {
    return mock('territorySummary')
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE region = '${filters.team}'` : ''

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Analytics/territory_summary\`
      ${teamFilter}
      LIMIT 1
    `,
  })

  if (results.length === 0) {
    throw new Error('No territory summary found')
  }

  const row = results[0]
  return {
    totalTerritories: row.total_territories,
    totalAccounts: row.total_accounts,
    avgAccountsPerTerritory: row.avg_accounts_per_territory,
    avgPropensityScore: row.avg_propensity_score,
    highPropensityCount: row.high_propensity_count,
    highPropensityRevenuePotential: row.high_propensity_revenue_potential,
    mediumPropensityCount: row.medium_propensity_count,
    lowPropensityCount: row.low_propensity_count,
    balancedTerritories: row.balanced_territories,
    underloadedTerritories: row.underloaded_territories,
    overloadedTerritories: row.overloaded_territories,
    avgTerritoryBalance: row.avg_territory_balance,
    whiteSpaceAccounts: row.white_space_accounts,
    whiteSpaceRevenuePotential: row.white_space_revenue_potential,
    topExpansionSegment: row.top_expansion_segment,
    topExpansionIndustry: row.top_expansion_industry,
    snapshotDate: row.snapshot_date,
  }
}

export async function getAccountScores(filters?: DashboardFilters): Promise<AccountScore[]> {
  if (USE_MOCK_DATA) {
    const scores = await mock('accountScores')
    let filtered = [...scores]
    if (filters?.team && filters.team !== 'All Teams') {
      filtered = filtered.filter(a => a.region === filters.team)
    }
    return filtered
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE region = '${filters.team}'` : ''

  return executeFoundrySQL<AccountScore>({
    query: `
      SELECT
        account_id as accountId,
        account_name as accountName,
        industry,
        segment,
        territory,
        owner_name as ownerName,
        propensity_score as propensityScore,
        propensity_tier as propensityTier,
        revenue_potential as revenuePotential,
        fit_score as fitScore,
        engagement_score as engagementScore,
        intent_score as intentScore,
        last_activity_date as lastActivityDate,
        current_arr as currentArr,
        expansion_potential as expansionPotential,
        key_signals as keySignals
      FROM \`/RevOps/Analytics/account_scores\`
      ${teamFilter}
      ORDER BY propensity_score DESC
    `,
  })
}

export async function getTerritoryBalance(filters?: DashboardFilters): Promise<TerritoryBalance[]> {
  if (USE_MOCK_DATA) {
    const balance = await mock('territoryBalance')
    let filtered = [...balance]
    if (filters?.team && filters.team !== 'All Teams') {
      filtered = filtered.filter(t => t.region === filters.team)
    }
    return filtered
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE region = '${filters.team}'` : ''

  return executeFoundrySQL<TerritoryBalance>({
    query: `
      SELECT
        territory_id as territoryId,
        territory_name as territoryName,
        region,
        owner_name as ownerName,
        account_count as accountCount,
        total_potential as totalPotential,
        high_propensity_count as highPropensityCount,
        quota,
        coverage_ratio as coverageRatio,
        balance_score as balanceScore,
        balance_status as balanceStatus,
        recommended_action as recommendedAction
      FROM \`/RevOps/Analytics/territory_balance\`
      ${teamFilter}
      ORDER BY balance_score ASC
    `,
  })
}

export async function getWhiteSpaceAccounts(filters?: DashboardFilters): Promise<WhiteSpaceAccount[]> {
  if (USE_MOCK_DATA) {
    const accounts = await mock('whiteSpaceAccounts')
    let filtered = [...accounts]
    if (filters?.team && filters.team !== 'All Teams') {
      filtered = filtered.filter(a => a.region === filters.team)
    }
    return filtered
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE region = '${filters.team}'` : ''

  return executeFoundrySQL<WhiteSpaceAccount>({
    query: `
      SELECT
        account_id as accountId,
        account_name as accountName,
        industry,
        segment,
        revenue_potential as revenuePotential,
        propensity_score as propensityScore,
        fit_score as fitScore,
        intent_signals as intentSignals,
        recommended_territory as recommendedTerritory,
        recommended_owner as recommendedOwner,
        priority_rank as priorityRank,
        key_expansion_products as keyExpansionProducts
      FROM \`/RevOps/Analytics/white_space_analysis\`
      ${teamFilter}
      ORDER BY priority_rank
    `,
  })
}
