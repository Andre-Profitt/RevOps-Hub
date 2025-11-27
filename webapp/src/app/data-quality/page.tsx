'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import {
  getDataQualitySummary,
  getDataQualityMetrics,
  getSyncStatus,
} from '@/lib/foundry'
import type {
  DataQualitySummary,
  DataQualityMetric,
  SyncStatus,
} from '@/types'

function formatNumber(value: number): string {
  if (value >= 1000000) return `${(value / 1000000).toFixed(1)}M`
  if (value >= 1000) return `${(value / 1000).toFixed(1)}K`
  return value.toFixed(0)
}

function formatPercent(value: number): string {
  return `${value.toFixed(1)}%`
}

function formatTime(dateStr: string): string {
  return new Date(dateStr).toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

function getScoreColor(score: number): string {
  if (score >= 90) return 'text-green-600'
  if (score >= 75) return 'text-yellow-600'
  return 'text-red-600'
}

function getScoreBg(score: number): string {
  if (score >= 90) return 'bg-green-500'
  if (score >= 75) return 'bg-yellow-500'
  return 'bg-red-500'
}

export default function DataQualityPage() {
  const [summary, setSummary] = useState<DataQualitySummary | null>(null)
  const [metrics, setMetrics] = useState<DataQualityMetric[]>([])
  const [syncStatus, setSyncStatus] = useState<SyncStatus[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    Promise.all([
      getDataQualitySummary(),
      getDataQualityMetrics(),
      getSyncStatus(),
    ]).then(([summaryData, metricsData, syncData]) => {
      setSummary(summaryData)
      setMetrics(metricsData)
      setSyncStatus(syncData)
      setLoading(false)
    })
  }, [])

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 p-8">
        <div className="animate-pulse">Loading data quality metrics...</div>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 px-6 py-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Link href="/" className="text-gray-500 hover:text-gray-700">
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" />
              </svg>
            </Link>
            <div>
              <h1 className="text-2xl font-bold text-gray-900">Data Quality Admin</h1>
              <p className="text-sm text-gray-500">Monitor data health and sync status</p>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <span className={`px-3 py-1 rounded-full text-sm font-medium ${
              summary?.syncStatus === 'healthy' ? 'bg-green-100 text-green-800' :
              summary?.syncStatus === 'warning' ? 'bg-yellow-100 text-yellow-800' :
              'bg-red-100 text-red-800'
            }`}>
              Sync: {summary?.syncStatus}
            </span>
            <span className="text-sm text-gray-500">
              Last sync: {summary?.lastSyncTime ? formatTime(summary.lastSyncTime) : 'N/A'}
            </span>
          </div>
        </div>
      </header>

      <main className="p-6 space-y-6">
        {/* Score Cards */}
        <div className="grid grid-cols-4 gap-4">
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Overall Score</p>
            <div className="flex items-end gap-2">
              <p className={`text-3xl font-bold ${getScoreColor(summary?.overallScore || 0)}`}>
                {summary?.overallScore}
              </p>
              <p className="text-sm text-gray-400 mb-1">/ 100</p>
            </div>
            <div className="mt-2 bg-gray-100 rounded-full h-2">
              <div
                className={`h-2 rounded-full ${getScoreBg(summary?.overallScore || 0)}`}
                style={{ width: `${summary?.overallScore || 0}%` }}
              />
            </div>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Completeness</p>
            <p className={`text-3xl font-bold ${getScoreColor(summary?.completenessScore || 0)}`}>
              {summary?.completenessScore}%
            </p>
            <div className="mt-2 bg-gray-100 rounded-full h-2">
              <div
                className={`h-2 rounded-full ${getScoreBg(summary?.completenessScore || 0)}`}
                style={{ width: `${summary?.completenessScore || 0}%` }}
              />
            </div>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Validity</p>
            <p className={`text-3xl font-bold ${getScoreColor(summary?.validityScore || 0)}`}>
              {summary?.validityScore}%
            </p>
            <div className="mt-2 bg-gray-100 rounded-full h-2">
              <div
                className={`h-2 rounded-full ${getScoreBg(summary?.validityScore || 0)}`}
                style={{ width: `${summary?.validityScore || 0}%` }}
              />
            </div>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Freshness</p>
            <p className={`text-3xl font-bold ${getScoreColor(summary?.freshnessScore || 0)}`}>
              {summary?.freshnessScore}%
            </p>
            <div className="mt-2 bg-gray-100 rounded-full h-2">
              <div
                className={`h-2 rounded-full ${getScoreBg(summary?.freshnessScore || 0)}`}
                style={{ width: `${summary?.freshnessScore || 0}%` }}
              />
            </div>
          </div>
        </div>

        {/* Stats Row */}
        <div className="grid grid-cols-4 gap-4">
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Total Records</p>
            <p className="text-2xl font-bold text-gray-900">{formatNumber(summary?.totalRecords || 0)}</p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Total Issues</p>
            <p className="text-2xl font-bold text-yellow-600">{summary?.issueCount}</p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Critical Issues</p>
            <p className="text-2xl font-bold text-red-600">{summary?.criticalIssues}</p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Trend</p>
            <p className={`text-2xl font-bold ${
              summary?.trend === 'improving' ? 'text-green-600' :
              summary?.trend === 'declining' ? 'text-red-600' :
              'text-gray-600'
            }`}>
              {summary?.trend === 'improving' ? '↑ Improving' :
               summary?.trend === 'declining' ? '↓ Declining' :
               '→ Stable'}
            </p>
          </div>
        </div>

        <div className="grid grid-cols-3 gap-6">
          {/* Field Quality Metrics */}
          <div className="col-span-2 bg-white rounded-lg border border-gray-200">
            <div className="p-4 border-b border-gray-200">
              <h2 className="font-semibold text-gray-900">Field Quality Metrics</h2>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead className="bg-gray-50">
                  <tr className="text-left text-gray-500">
                    <th className="px-4 py-3">Field</th>
                    <th className="px-4 py-3">Object</th>
                    <th className="px-4 py-3 text-right">Completeness</th>
                    <th className="px-4 py-3 text-right">Validity</th>
                    <th className="px-4 py-3 text-right">Issues</th>
                    <th className="px-4 py-3">Severity</th>
                    <th className="px-4 py-3">Recommendation</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {metrics.map((metric, idx) => (
                    <tr key={idx} className="hover:bg-gray-50">
                      <td className="px-4 py-3 font-medium text-gray-900">{metric.field}</td>
                      <td className="px-4 py-3 text-gray-600">{metric.object}</td>
                      <td className="px-4 py-3 text-right">
                        <span className={getScoreColor(metric.completeness)}>
                          {formatPercent(metric.completeness)}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-right">
                        <span className={getScoreColor(metric.validity)}>
                          {formatPercent(metric.validity)}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-right">{metric.issueCount}</td>
                      <td className="px-4 py-3">
                        <span className={`px-2 py-0.5 rounded text-xs ${
                          metric.severity === 'critical' ? 'bg-red-100 text-red-700' :
                          metric.severity === 'high' ? 'bg-orange-100 text-orange-700' :
                          metric.severity === 'medium' ? 'bg-yellow-100 text-yellow-700' :
                          'bg-gray-100 text-gray-700'
                        }`}>
                          {metric.severity}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-xs text-gray-500 max-w-xs truncate">
                        {metric.recommendation}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Sync Status */}
          <div className="bg-white rounded-lg border border-gray-200">
            <div className="p-4 border-b border-gray-200">
              <h2 className="font-semibold text-gray-900">Sync Status</h2>
            </div>
            <div className="p-4 space-y-4">
              {syncStatus.map((sync, idx) => (
                <div key={idx} className="p-3 bg-gray-50 rounded-lg">
                  <div className="flex items-center justify-between mb-2">
                    <span className="font-medium text-gray-900">{sync.source}</span>
                    <span className={`px-2 py-0.5 rounded text-xs ${
                      sync.status === 'healthy' ? 'bg-green-100 text-green-700' :
                      sync.status === 'warning' ? 'bg-yellow-100 text-yellow-700' :
                      'bg-red-100 text-red-700'
                    }`}>
                      {sync.status}
                    </span>
                  </div>
                  <div className="grid grid-cols-2 gap-2 text-sm">
                    <div>
                      <span className="text-gray-500">Last sync:</span>
                      <div className="text-gray-900">{formatTime(sync.lastSync)}</div>
                    </div>
                    <div>
                      <span className="text-gray-500">Records:</span>
                      <div className="text-gray-900">{formatNumber(sync.recordsProcessed)}</div>
                    </div>
                    <div>
                      <span className="text-gray-500">Errors:</span>
                      <div className={sync.errorCount > 0 ? 'text-red-600 font-medium' : 'text-gray-900'}>
                        {sync.errorCount}
                      </div>
                    </div>
                    <div>
                      <span className="text-gray-500">Avg time:</span>
                      <div className="text-gray-900">{sync.avgSyncTime}s</div>
                    </div>
                  </div>
                  <div className="mt-2 text-xs text-gray-500">
                    Next: {formatTime(sync.nextScheduledSync)}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </main>
    </div>
  )
}
