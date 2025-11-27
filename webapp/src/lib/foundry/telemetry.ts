/**
 * Cross-Functional Telemetry data fetchers
 */

import { USE_MOCK_DATA, mock, executeFoundrySQL, type DashboardFilters } from './core'
import type {
  TelemetrySummary,
  FunnelHandoffMetrics,
  CrossTeamActivity,
} from '@/types'

export async function getTelemetrySummary(filters?: DashboardFilters): Promise<TelemetrySummary> {
  if (USE_MOCK_DATA) {
    return mock('telemetrySummary')
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE region = '${filters.team}'` : ''

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Analytics/telemetry_summary\`
      ${teamFilter}
      LIMIT 1
    `,
  })

  if (results.length === 0) {
    throw new Error('No telemetry summary data found')
  }

  const row = results[0]
  return {
    totalLeads: row.total_leads,
    totalMqls: row.total_mqls,
    totalSqls: row.total_sqls,
    totalConverted: row.total_converted,
    mqlToSqlRate: row.mql_to_sql_rate,
    overallConversionRate: row.overall_conversion_rate,
    avgResponseDays: row.avg_response_days,
    fastResponsePct: row.fast_response_pct,
    slowResponseCount: row.slow_response_count,
    avgEngagementScore: row.avg_engagement_score,
    avgTouchesPerDeal: row.avg_touches_per_deal,
    fullEngagementPct: row.full_engagement_pct,
    fullEngagementHealthAvg: row.full_engagement_health_avg,
    singleEngagementHealthAvg: row.single_engagement_health_avg,
    healthDelta: row.health_delta,
    mqlToSqlHealth: row.mql_to_sql_health,
    responseHealth: row.response_health,
    speedToLeadHealth: row.speed_to_lead_health,
    teamAlignmentHealth: row.team_alignment_health,
    overallHealth: row.overall_health,
    primaryBottleneck: row.primary_bottleneck,
    recommendation: row.recommendation,
    snapshotDate: row.snapshot_date,
  }
}

export async function getFunnelHandoffs(filters?: DashboardFilters): Promise<FunnelHandoffMetrics[]> {
  if (USE_MOCK_DATA) {
    const handoffs = await mock('funnelHandoffs')
    let filtered = [...handoffs]
    if (filters?.team && filters.team !== 'All Teams') {
      // Filter mock data by segment matching team name (simplified)
      filtered = filtered.filter(h => h.segment === 'Enterprise')
    }
    return filtered
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE segment = '${filters.team}'` : ''

  return executeFoundrySQL<FunnelHandoffMetrics>({
    query: `
      SELECT
        lead_source as leadSource,
        segment,
        region,
        total_leads as totalLeads,
        mqls,
        sqls,
        converted,
        mql_to_sql_rate as mqlToSqlRate,
        overall_conversion_rate as overallConversionRate,
        avg_response_time_days as avgResponseTimeDays,
        mql_to_sql_health as mqlToSqlHealth,
        response_time_health as responseTimeHealth
      FROM \`/RevOps/Analytics/funnel_handoff_metrics\`
      ${teamFilter}
      ORDER BY total_leads DESC
    `,
  })
}

export async function getCrossTeamActivity(filters?: DashboardFilters): Promise<CrossTeamActivity[]> {
  if (USE_MOCK_DATA) {
    // Mock data doesn't support filtering - return all
    return mock('crossTeamActivity')
  }

  const teamFilter = filters?.team && filters.team !== 'All Teams' ? `WHERE region = '${filters.team}'` : ''

  return executeFoundrySQL<CrossTeamActivity>({
    query: `
      SELECT
        opportunity_id as opportunityId,
        account_name as accountName,
        owner_name as ownerName,
        amount,
        stage_name as stageName,
        marketing_touches as marketingTouches,
        sales_touches as salesTouches,
        cs_touches as csTouches,
        total_touches as totalTouches,
        multi_team_engagement as multiTeamEngagement,
        health_score as healthScore
      FROM \`/RevOps/Analytics/cross_team_activity\`
      ${teamFilter}
      ORDER BY amount DESC
    `,
  })
}

// ============================================================================
// TELEMETRY DATA SOURCES
// ============================================================================

export async function getSupportHealthByAccount(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        account_id as accountId,
        account_name as accountName,
        open_tickets as openTickets,
        resolved_30d as resolved30d,
        avg_resolution_hours as avgResolutionHours,
        p1_tickets_30d as p1Tickets30d,
        csat_score as csatScore,
        support_health_score as supportHealthScore,
        support_trend as supportTrend,
        escalation_rate as escalationRate,
        top_issue_categories as topIssueCategories,
        assigned_support_rep as assignedSupportRep,
        last_ticket_date as lastTicketDate
      FROM \`/RevOps/Telemetry/support_health_by_account\`
      ORDER BY support_health_score ASC
    `,
  })
}

export async function getUnifiedTimeline(accountId?: string, opportunityId?: string): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  let whereClause = ''
  if (accountId) {
    whereClause = `WHERE account_id = '${accountId}'`
  } else if (opportunityId) {
    whereClause = `WHERE opportunity_id = '${opportunityId}'`
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        event_id as eventId,
        account_id as accountId,
        opportunity_id as opportunityId,
        account_name as accountName,
        event_type as eventType,
        event_source as eventSource,
        event_timestamp as eventTimestamp,
        title,
        description,
        actor_name as actorName,
        actor_role as actorRole,
        sentiment,
        impact_score as impactScore,
        related_object_type as relatedObjectType,
        related_object_id as relatedObjectId,
        metadata
      FROM \`/RevOps/Telemetry/unified_timeline\`
      ${whereClause}
      ORDER BY event_timestamp DESC
      LIMIT 100
    `,
  })
}

export async function getMarketingActivities(accountId?: string): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  const accountFilter = accountId ? `WHERE account_id = '${accountId}'` : ''

  return executeFoundrySQL<any>({
    query: `
      SELECT
        activity_id as activityId,
        account_id as accountId,
        contact_id as contactId,
        account_name as accountName,
        contact_name as contactName,
        activity_type as activityType,
        campaign_name as campaignName,
        campaign_id as campaignId,
        activity_date as activityDate,
        channel,
        engagement_score as engagementScore,
        response_type as responseType,
        utm_source as utmSource,
        utm_medium as utmMedium,
        utm_campaign as utmCampaign
      FROM \`/RevOps/Staging/marketing_activities\`
      ${accountFilter}
      ORDER BY activity_date DESC
    `,
  })
}
