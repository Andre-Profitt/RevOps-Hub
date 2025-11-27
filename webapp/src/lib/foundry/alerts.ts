/**
 * Alerts & Notifications data fetchers
 */

import { USE_MOCK_DATA, mock, executeFoundrySQL } from './core'
import type {
  AlertSummary,
  Alert,
  AlertRule,
} from '@/types'

export async function getAlertSummary(): Promise<AlertSummary> {
  if (USE_MOCK_DATA) {
    return mock('alertSummary')
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/Alerts/summary\`
      ORDER BY snapshot_date DESC
      LIMIT 1
    `,
  })

  if (results.length === 0) {
    throw new Error('No alert summary found')
  }

  const row = results[0]
  return {
    totalActive: row.total_active,
    criticalCount: row.critical_count,
    highCount: row.high_count,
    mediumCount: row.medium_count,
    lowCount: row.low_count,
    acknowledgedCount: row.acknowledged_count,
    resolvedToday: row.resolved_today,
    avgResolutionTime: row.avg_resolution_time,
  }
}

export async function getAlerts(): Promise<Alert[]> {
  if (USE_MOCK_DATA) {
    return mock('alerts')
  }

  return executeFoundrySQL<Alert>({
    query: `
      SELECT
        alert_id as alertId,
        title,
        description,
        severity,
        category,
        source,
        created_at as createdAt,
        status,
        acknowledged_by as acknowledgedBy,
        acknowledged_at as acknowledgedAt,
        resolved_at as resolvedAt,
        related_object_type as relatedObjectType,
        related_object_id as relatedObjectId,
        related_object_name as relatedObjectName,
        action_url as actionUrl,
        assignee
      FROM \`/RevOps/Alerts/active\`
      ORDER BY severity DESC, created_at DESC
    `,
  })
}

export async function getAlertRules(): Promise<AlertRule[]> {
  if (USE_MOCK_DATA) {
    return mock('alertRules')
  }

  return executeFoundrySQL<AlertRule>({
    query: `
      SELECT
        rule_id as ruleId,
        name,
        description,
        category,
        severity,
        condition,
        threshold,
        enabled,
        notification_channels as notificationChannels,
        recipients,
        created_by as createdBy,
        created_at as createdAt,
        last_triggered as lastTriggered,
        trigger_count as triggerCount
      FROM \`/RevOps/Alerts/rules\`
      ORDER BY severity DESC, name
    `,
  })
}

// ============================================================================
// MONITORING ALERTS
// ============================================================================

export async function getMonitoringAlertSummary(): Promise<any> {
  if (USE_MOCK_DATA) {
    return null
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        total_alerts as totalAlerts,
        critical_count as criticalCount,
        high_count as highCount,
        medium_count as mediumCount,
        low_count as lowCount,
        acknowledged_count as acknowledgedCount,
        unacknowledged_count as unacknowledgedCount,
        resolved_24h as resolved24h,
        new_24h as new24h,
        avg_resolution_minutes as avgResolutionMinutes,
        oldest_unresolved_hours as oldestUnresolvedHours,
        by_category as byCategory,
        by_source as bySource,
        trend_7d as trend7d,
        snapshot_time as snapshotTime
      FROM \`/RevOps/Monitoring/alert_summary\`
      ORDER BY snapshot_time DESC
      LIMIT 1
    `,
  })

  return results[0] || null
}

export async function getAlertHistory(hours: number = 24): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        alert_id as alertId,
        title,
        description,
        severity,
        category,
        source,
        status,
        created_at as createdAt,
        acknowledged_at as acknowledgedAt,
        resolved_at as resolvedAt,
        resolution_minutes as resolutionMinutes,
        acknowledged_by as acknowledgedBy,
        resolved_by as resolvedBy
      FROM \`/RevOps/Monitoring/alert_history\`
      WHERE created_at >= current_timestamp() - INTERVAL ${hours} HOURS
      ORDER BY created_at DESC
    `,
  })
}
