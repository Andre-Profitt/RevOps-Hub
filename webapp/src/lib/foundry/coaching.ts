/**
 * Rep Coaching data fetchers
 */

import { USE_MOCK_DATA, mock, executeFoundrySQL } from './core'
import type {
  RepPerformance,
  DriverComparison,
  ActivityMetrics,
  NextBestAction,
  CoachingInsight,
  SalesRep,
} from '@/types'

export async function getRepPerformance(repId: string): Promise<RepPerformance> {
  if (USE_MOCK_DATA) {
    // Mock data has a single demo rep - ignore repId for demo
    return mock('repPerformance')
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Coaching/rep_performance\`
      WHERE rep_id = '${repId}'
      LIMIT 1
    `,
  })

  if (results.length === 0) {
    throw new Error(`Rep not found: ${repId}`)
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
    avgSalesCycle: row.avg_sales_cycle,
    overallRank: row.overall_rank,
    totalReps: row.total_reps,
    rankChange: row.rank_change,
    teamAvgWinRate: row.team_avg_win_rate,
    teamAvgDealSize: row.team_avg_deal_size,
    teamAvgCycle: row.team_avg_cycle,
    performanceTier: row.performance_tier,
  }
}

export async function getDriverComparisons(repId: string): Promise<DriverComparison[]> {
  if (USE_MOCK_DATA) {
    return mock('driverComparisons')
  }

  return executeFoundrySQL<DriverComparison>({
    query: `
      SELECT
        rep_id as repId,
        metric_name as metricName,
        rep_value as repValue,
        team_avg as teamAvg,
        top_performer as topPerformer,
        percentile,
        trend_direction as trendDirection,
        impact_on_quota as impactOnQuota
      FROM \`/RevOps/Coaching/driver_comparisons\`
      WHERE rep_id = '${repId}'
      ORDER BY impact_on_quota DESC
    `,
  })
}

export async function getActivityMetrics(repId: string): Promise<ActivityMetrics> {
  if (USE_MOCK_DATA) {
    // Mock data has single activity metrics object - ignore repId for demo
    return mock('activityMetrics')
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Coaching/activity_metrics\`
      WHERE rep_id = '${repId}'
      LIMIT 1
    `,
  })

  if (results.length === 0) {
    return {
      totalActivities: 0,
      calls: 0,
      emails: 0,
      meetings: 0,
      demos: 0,
      avgDaily: 0,
      teamAvgDaily: 0,
      topPerformerDaily: 0,
      dailyTrend: [],
    }
  }

  const row = results[0]
  return {
    totalActivities: row.total_activities,
    calls: row.calls,
    emails: row.emails,
    meetings: row.meetings,
    demos: row.demos,
    avgDaily: row.avg_daily,
    teamAvgDaily: row.team_avg_daily,
    topPerformerDaily: row.top_performer_daily,
    dailyTrend: row.daily_trend || [],
  }
}

export async function getNextBestActions(repId: string): Promise<NextBestAction[]> {
  if (USE_MOCK_DATA) {
    return mock('nextBestActions')
  }

  return executeFoundrySQL<NextBestAction>({
    query: `
      SELECT
        rep_id as repId,
        opportunity_id as opportunityId,
        account_name as accountName,
        action_type as actionType,
        action_description as actionDescription,
        priority,
        expected_impact as expectedImpact,
        due_date as dueDate
      FROM \`/RevOps/Coaching/next_best_actions\`
      WHERE rep_id = '${repId}'
      ORDER BY priority DESC
    `,
  })
}

export async function getCoachingInsights(repId: string): Promise<CoachingInsight[]> {
  if (USE_MOCK_DATA) {
    // Mock data has separate arrays - combine them for coaching insights
    const strengths = await mock('coachingStrengths')
    const improvements = await mock('coachingImprovements')
    const actions = await mock('coachingActions')
    return [...strengths, ...improvements, ...actions]
  }

  return executeFoundrySQL<CoachingInsight>({
    query: `
      SELECT
        rep_id as repId,
        insight_type as insightType,
        title,
        description,
        metric_impact as metricImpact,
        recommended_action as recommendedAction,
        priority,
        category
      FROM \`/RevOps/Coaching/coaching_insights\`
      WHERE rep_id = '${repId}'
      ORDER BY priority DESC
    `,
  })
}

/**
 * Get all sales reps for the rep selector dropdown
 */
export async function getSalesReps(): Promise<SalesRep[]> {
  if (USE_MOCK_DATA) {
    return mock('salesReps')
  }

  return executeFoundrySQL<SalesRep>({
    query: `
      SELECT
        rep_id as id,
        rep_name as name,
        region as team,
        region,
        segment
      FROM \`/RevOps/Coaching/rep_roster\`
      ORDER BY rep_name
    `,
  })
}

// ============================================================================
// TOP PERFORMER & ML RECOMMENDATIONS
// ============================================================================

export async function getTopPerformerPatterns(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        pattern_id as patternId,
        pattern_name as patternName,
        description,
        top_performer_pct as topPerformerPct,
        avg_performer_pct as avgPerformerPct,
        impact_on_win_rate as impactOnWinRate,
        impact_on_deal_size as impactOnDealSize,
        implementation_difficulty as implementationDifficulty,
        category,
        example_behaviors as exampleBehaviors,
        recommended_for as recommendedFor
      FROM \`/RevOps/Analytics/top_performer_patterns\`
      ORDER BY impact_on_win_rate DESC
    `,
  })
}

export async function getRepRecommendations(repId?: string): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  const repFilter = repId ? `WHERE rep_id = '${repId}'` : ''

  return executeFoundrySQL<any>({
    query: `
      SELECT
        recommendation_id as recommendationId,
        rep_id as repId,
        rep_name as repName,
        recommendation_type as recommendationType,
        title,
        description,
        expected_impact as expectedImpact,
        priority,
        confidence,
        based_on as basedOn,
        action_steps as actionSteps,
        due_date as dueDate,
        created_at as createdAt
      FROM \`/RevOps/ML/rep_recommendations\`
      ${repFilter}
      ORDER BY priority DESC, expected_impact DESC
    `,
  })
}
