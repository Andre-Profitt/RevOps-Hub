/**
 * Foundry API Integration Layer
 *
 * This module provides functions to connect the React app to Foundry data.
 * In development, it returns mock data. In production, it calls the secure
 * API route which proxies to Foundry (keeping credentials server-side).
 */

// Import mock data for development
import * as mockData from '@/data/mockData'
import type {
  DashboardKPIs,
  ForecastTrendPoint,
  StageVelocity,
  FunnelStage,
  StuckDeal,
  HealthDistribution,
  RepPerformance,
  DriverComparison,
  ActivityMetrics,
  NextBestAction,
  CoachingInsight,
} from '@/types'

// Configuration - only USE_MOCK_DATA is client-safe
const USE_MOCK_DATA = process.env.NEXT_PUBLIC_USE_MOCK_DATA === 'true'

// =====================================================
// API CLIENT (calls secure server-side route)
// =====================================================

interface QueryParams {
  query: string
  fallbackBranchIds?: string[]
}

async function executeFoundrySQL<T>(params: QueryParams): Promise<T[]> {
  // Call our secure API route instead of Foundry directly
  const response = await fetch('/api/foundry', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      query: params.query,
      fallbackBranchIds: params.fallbackBranchIds || ['master'],
    }),
  })

  const result = await response.json()

  // Server tells us to use mock data if Foundry isn't configured
  if (result.useMock) {
    throw new Error('Mock mode - Foundry not configured on server')
  }

  if (!response.ok) {
    throw new Error(result.error || `Foundry SQL error: ${response.status}`)
  }

  return result.rows as T[]
}

// =====================================================
// DATA FETCHERS - Pipeline Health Dashboard
// =====================================================

export async function getDashboardKPIs(): Promise<DashboardKPIs> {
  if (USE_MOCK_DATA) {
    return mockData.dashboardKPIs
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Dashboard/kpis\`
      WHERE quarter = 'Q4 2024'
      LIMIT 1
    `,
  })

  if (results.length === 0) {
    throw new Error('No KPI data found')
  }

  // Transform Foundry row to our type
  const row = results[0]
  return {
    quarterTarget: row.quarter_target,
    closedWonAmount: row.closed_won_amount,
    aiForecast: row.ai_forecast,
    aiForecastConfidence: row.ai_forecast_confidence,
    commitAmount: row.commit_amount,
    commitConfidence: row.commit_weighted / row.commit_amount,
    gapToTarget: row.gap_to_target,
    gapPct: row.gap_pct,
    coverageRatio: row.coverage_ratio,
    riskLevel: row.risk_level,
    forecastChange1w: row.forecast_change_1w,
    commitChange1w: row.commit_change_1w,
  }
}

export async function getForecastTrend(): Promise<ForecastTrendPoint[]> {
  if (USE_MOCK_DATA) {
    return mockData.forecastTrend
  }

  return executeFoundrySQL<ForecastTrendPoint>({
    query: `
      SELECT
        week,
        week_number as weekNumber,
        cumulative_target as target,
        cumulative_closed as closed,
        cumulative_commit as commit,
        cumulative_forecast as forecast
      FROM \`/RevOps/Analytics/forecast_history\`
      WHERE quarter = 'Q4 2024'
      ORDER BY week_number
    `,
  })
}

export async function getStuckDeals(): Promise<StuckDeal[]> {
  if (USE_MOCK_DATA) {
    return mockData.stuckDeals
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        opportunity_id as id,
        account_name as accountName,
        opportunity_name as opportunityName,
        owner_name as ownerName,
        amount,
        stuck_activity as stageName,
        duration_days as daysInStage,
        health_score as healthScore,
        close_date as closeDate,
        urgency,
        primary_action as primaryAction
      FROM \`/RevOps/Analytics/stuck_deals_view\`
      ORDER BY amount DESC
      LIMIT 10
    `,
  })

  return results.map((row) => ({
    ...row,
    riskFlags: ['Stuck'], // Would come from a separate field
  }))
}

export async function getHealthDistribution(): Promise<HealthDistribution> {
  if (USE_MOCK_DATA) {
    return mockData.healthDistribution
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Analytics/health_distribution\`
      WHERE quarter = 'Q4 2024'
      LIMIT 1
    `,
  })

  const row = results[0]
  return {
    healthy: { count: row.healthy_count, amount: row.healthy_amount, pct: row.healthy_pct },
    monitor: { count: row.monitor_count, amount: row.monitor_amount, pct: row.monitor_pct },
    atRisk: { count: row.at_risk_count, amount: row.at_risk_amount, pct: row.at_risk_pct },
    critical: { count: row.critical_count, amount: row.critical_amount, pct: row.critical_pct },
    trend: row.at_risk_change_1w,
  }
}

// =====================================================
// DATA FETCHERS - Rep Coaching Dashboard
// =====================================================

export async function getRepPerformance(repId: string): Promise<RepPerformance> {
  if (USE_MOCK_DATA) {
    return mockData.repPerformance
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Analytics/rep_performance_view\`
      WHERE rep_id = '${repId}'
      LIMIT 1
    `,
  })

  if (results.length === 0) {
    throw new Error(`Rep ${repId} not found`)
  }

  const row = results[0]
  return {
    repId: row.rep_id,
    repName: row.rep_name,
    managerName: row.manager_name,
    region: row.region,
    segment: row.segment,
    ytdAttainment: row.ytd_attainment,
    winRate: row.win_rate,
    avgDealSize: row.avg_deal_size,
    avgSalesCycle: row.avg_sales_cycle_days,
    overallRank: row.overall_rank,
    totalReps: row.total_reps,
    rankChange: row.rank_change_qoq,
    teamAvgWinRate: row.team_avg_win_rate,
    teamAvgDealSize: row.team_avg_deal_size,
    teamAvgCycle: row.team_avg_cycle,
    performanceTier: row.performance_tier,
  }
}

export async function getDriverComparisons(repId: string): Promise<DriverComparison[]> {
  if (USE_MOCK_DATA) {
    return mockData.driverComparisons
  }

  return executeFoundrySQL<DriverComparison>({
    query: `
      SELECT *
      FROM \`/RevOps/Analytics/rep_driver_comparison\`
      WHERE rep_id = '${repId}'
    `,
  })
}

export async function getNextBestActions(repId: string): Promise<NextBestAction[]> {
  if (USE_MOCK_DATA) {
    return mockData.nextBestActions
  }

  return executeFoundrySQL<NextBestAction>({
    query: `
      SELECT
        action_id as actionId,
        opportunity_id as opportunityId,
        account_name as accountName,
        contact_name as contactName,
        action_type as actionType,
        action_reason as actionReason,
        priority_rank as priorityRank,
        urgency,
        deal_amount as dealAmount,
        deal_health as dealHealth,
        time_display as bestTime
      FROM \`/RevOps/Dashboard/next_best_actions\`
      WHERE rep_id = '${repId}'
      ORDER BY priority_rank
      LIMIT 5
    `,
  })
}

// =====================================================
// HOOKS FOR REACT COMPONENTS
// =====================================================

import { useState, useEffect } from 'react'

export function useDashboardKPIs() {
  const [data, setData] = useState<DashboardKPIs | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<Error | null>(null)

  useEffect(() => {
    getDashboardKPIs()
      .then(setData)
      .catch(setError)
      .finally(() => setLoading(false))
  }, [])

  return { data, loading, error }
}

export function useRepPerformance(repId: string) {
  const [data, setData] = useState<RepPerformance | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<Error | null>(null)

  useEffect(() => {
    getRepPerformance(repId)
      .then(setData)
      .catch(setError)
      .finally(() => setLoading(false))
  }, [repId])

  return { data, loading, error }
}

// =====================================================
// UTILITY FUNCTIONS
// =====================================================

export async function checkFoundryConnection(): Promise<boolean> {
  if (USE_MOCK_DATA) return false
  try {
    const response = await fetch('/api/foundry')
    const data = await response.json()
    return data.configured === true
  } catch {
    return false
  }
}

export function getDataSource(): 'foundry' | 'mock' {
  return USE_MOCK_DATA ? 'mock' : 'foundry'
}
