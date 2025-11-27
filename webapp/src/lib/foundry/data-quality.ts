/**
 * Data Quality Admin data fetchers
 */

import { USE_MOCK_DATA, mock, executeFoundrySQL } from './core'
import type {
  DataQualitySummary,
  DataQualityMetric,
  SyncStatus,
} from '@/types'

export async function getDataQualitySummary(): Promise<DataQualitySummary> {
  if (USE_MOCK_DATA) {
    return mock('dataQualitySummary')
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/DataQuality/summary\`
      ORDER BY last_sync_time DESC
      LIMIT 1
    `,
  })

  if (results.length === 0) {
    throw new Error('No data quality summary found')
  }

  const row = results[0]
  return {
    overallScore: row.overall_score,
    completenessScore: row.completeness_score,
    validityScore: row.validity_score,
    freshnessScore: row.freshness_score,
    totalRecords: row.total_records,
    issueCount: row.issue_count,
    criticalIssues: row.critical_issues,
    lastSyncTime: row.last_sync_time,
    syncStatus: row.sync_status,
    trend: row.trend,
  }
}

export async function getDataQualityMetrics(): Promise<DataQualityMetric[]> {
  if (USE_MOCK_DATA) {
    return mock('dataQualityMetrics')
  }

  return executeFoundrySQL<DataQualityMetric>({
    query: `
      SELECT
        metric_name as metricName,
        category,
        object_type as objectType,
        current_score as currentScore,
        target_score as targetScore,
        trend,
        issue_count as issueCount,
        sample_issues as sampleIssues,
        remediation_action as remediationAction,
        owner
      FROM \`/RevOps/DataQuality/metrics\`
      ORDER BY current_score ASC
    `,
  })
}

export async function getSyncStatus(): Promise<SyncStatus[]> {
  if (USE_MOCK_DATA) {
    return mock('syncStatus')
  }

  return executeFoundrySQL<SyncStatus>({
    query: `
      SELECT
        source_system as sourceSystem,
        object_type as objectType,
        last_sync as lastSync,
        status,
        records_synced as recordsSynced,
        records_failed as recordsFailed,
        error_message as errorMessage,
        next_sync as nextSync,
        sync_frequency as syncFrequency
      FROM \`/RevOps/DataQuality/sync_status\`
      ORDER BY last_sync DESC
    `,
  })
}
