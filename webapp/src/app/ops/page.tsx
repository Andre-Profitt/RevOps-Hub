'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import {
  getPipelineEvents,
  getOpsAgentQueue,
  getOpsAgentQueueSummary,
  getBuildSummaryByDomain,
  getDatasetHealthOverview,
} from '@/lib/foundry'

interface PipelineEvent {
  eventId: string
  eventTs: string
  runId: string
  datasetPath: string
  domain: string
  eventType: string
  severity: string
  message: string
  details: string
  requiresAction: boolean
  suggestedAction: string
  priorityScore: number
  actionStatus: string
}

interface AgentQueueItem {
  eventId: string
  eventTs: string
  datasetPath: string
  domain: string
  eventType: string
  severity: string
  message: string
  details: string
  suggestedAction: string
  priorityScore: number
  actionStatus: string
  queuePosition: number
  estimatedResolutionMin: number
}

interface QueueSummary {
  totalItems: number
  criticalCount: number
  warningCount: number
  totalEstimatedMinutes: number
  maxPriority: number
}

interface DomainSummary {
  domain: string
  totalDatasets: number
  healthyDatasets: number
  emptyDatasets: number
  errorDatasets: number
  totalRecords: number
  latestBuild: string
  avgLatencyMs: number
  maxLatencyMs: number
  slowDatasets: number
  avgNullPct: number
  qualityIssueDatasets: number
  totalColumns: number
  healthPct: number
  performanceScore: number
  qualityScore: number
  latestRunId: string
}

interface HealthOverview {
  totalDatasets: number
  healthyDatasets: number
  errorDatasets: number
  slowDatasets: number
  qualityIssues: number
  avgLatencyMs: number
  avgNullPct: number
  totalRecords: number
}

function formatTime(dateStr: string): string {
  return new Date(dateStr).toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

function getTimeAgo(dateStr: string): string {
  const diff = Date.now() - new Date(dateStr).getTime()
  const minutes = Math.floor(diff / 60000)
  const hours = Math.floor(diff / 3600000)
  const days = Math.floor(diff / 86400000)

  if (days > 0) return `${days}d ago`
  if (hours > 0) return `${hours}h ago`
  return `${minutes}m ago`
}

function getEventTypeColor(eventType: string): string {
  switch (eventType) {
    case 'BUILD_COMPLETE': return 'bg-green-100 text-green-700'
    case 'BUILD_EMPTY': return 'bg-yellow-100 text-yellow-700'
    case 'BUILD_ERROR': return 'bg-red-100 text-red-700'
    case 'DATASET_STALE': return 'bg-orange-100 text-orange-700'
    case 'PERFORMANCE_DEGRADED': return 'bg-purple-100 text-purple-700'
    case 'QUALITY_ALERT': return 'bg-blue-100 text-blue-700'
    default: return 'bg-gray-100 text-gray-700'
  }
}

function getSeverityColor(severity: string): string {
  switch (severity) {
    case 'critical': return 'bg-red-500'
    case 'warning': return 'bg-orange-500'
    case 'info': return 'bg-blue-500'
    default: return 'bg-gray-500'
  }
}

function getActionIcon(action: string): string {
  switch (action) {
    case 'TRIGGER_REBUILD': return 'M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15'
    case 'CHECK_UPSTREAM_DATA': return 'M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2'
    case 'OPTIMIZE_TRANSFORM': return 'M13 10V3L4 14h7v7l9-11h-7z'
    case 'REVIEW_DATA_QUALITY': return 'M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z'
    default: return 'M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z'
  }
}

export default function OpsPage() {
  const [events, setEvents] = useState<PipelineEvent[]>([])
  const [queue, setQueue] = useState<AgentQueueItem[]>([])
  const [queueSummary, setQueueSummary] = useState<QueueSummary | null>(null)
  const [domainSummaries, setDomainSummaries] = useState<DomainSummary[]>([])
  const [healthOverview, setHealthOverview] = useState<HealthOverview | null>(null)
  const [loading, setLoading] = useState(true)
  const [activeTab, setActiveTab] = useState<'events' | 'queue' | 'domains'>('events')
  const [eventFilter, setEventFilter] = useState<string>('all')

  useEffect(() => {
    Promise.all([
      getPipelineEvents(100),
      getOpsAgentQueue(),
      getOpsAgentQueueSummary(),
      getBuildSummaryByDomain(),
      getDatasetHealthOverview(),
    ]).then(([eventsData, queueData, queueSummaryData, domainsData, healthData]) => {
      setEvents(eventsData || [])
      setQueue(queueData || [])
      setQueueSummary(queueSummaryData)
      setDomainSummaries(domainsData || [])
      setHealthOverview(healthData)
      setLoading(false)
    })
  }, [])

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 p-8">
        <div className="animate-pulse">Loading AIOps Dashboard...</div>
      </div>
    )
  }

  const filteredEvents = eventFilter === 'all'
    ? events
    : events.filter(e => e.eventType === eventFilter)

  const criticalEvents = events.filter(e => e.severity === 'critical').length
  const warningEvents = events.filter(e => e.severity === 'warning').length
  const actionableEvents = events.filter(e => e.requiresAction).length

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
              <h1 className="text-2xl font-bold text-gray-900">AIOps Dashboard</h1>
              <p className="text-sm text-gray-500">Pipeline health, events, and autonomous operations</p>
            </div>
          </div>
          <div className="flex items-center gap-4">
            {criticalEvents > 0 && (
              <div className="flex items-center gap-2 bg-red-50 px-3 py-1.5 rounded-lg animate-pulse">
                <svg className="w-5 h-5 text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                </svg>
                <span className="text-sm font-medium text-red-700">{criticalEvents} Critical</span>
              </div>
            )}
            <div className="flex items-center gap-2 bg-blue-50 px-3 py-1.5 rounded-lg">
              <svg className="w-5 h-5 text-blue-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
              </svg>
              <span className="text-sm font-medium text-blue-700">Ops Agent: Active</span>
            </div>
          </div>
        </div>
      </header>

      <main className="p-6 space-y-6">
        {/* Health Overview Cards */}
        <div className="grid grid-cols-6 gap-4">
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Total Datasets</p>
            <p className="text-2xl font-bold text-gray-900">{healthOverview?.totalDatasets || 0}</p>
          </div>
          <div className={`rounded-lg p-4 border ${
            (healthOverview?.healthyDatasets || 0) / (healthOverview?.totalDatasets || 1) > 0.9
              ? 'bg-green-50 border-green-200'
              : 'bg-yellow-50 border-yellow-200'
          }`}>
            <p className="text-sm text-green-700">Healthy</p>
            <p className="text-2xl font-bold text-green-600">{healthOverview?.healthyDatasets || 0}</p>
          </div>
          <div className={`rounded-lg p-4 border ${
            (healthOverview?.errorDatasets || 0) > 0
              ? 'bg-red-50 border-red-200'
              : 'bg-gray-50 border-gray-200'
          }`}>
            <p className="text-sm text-red-700">Errors</p>
            <p className="text-2xl font-bold text-red-600">{healthOverview?.errorDatasets || 0}</p>
          </div>
          <div className="bg-purple-50 rounded-lg p-4 border border-purple-200">
            <p className="text-sm text-purple-700">Slow</p>
            <p className="text-2xl font-bold text-purple-600">{healthOverview?.slowDatasets || 0}</p>
          </div>
          <div className="bg-blue-50 rounded-lg p-4 border border-blue-200">
            <p className="text-sm text-blue-700">Quality Issues</p>
            <p className="text-2xl font-bold text-blue-600">{healthOverview?.qualityIssues || 0}</p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Avg Latency</p>
            <p className="text-2xl font-bold text-gray-900">{(healthOverview?.avgLatencyMs || 0).toFixed(0)}ms</p>
          </div>
        </div>

        {/* Agent Queue Summary */}
        {queueSummary && queueSummary.totalItems > 0 && (
          <div className="bg-gradient-to-r from-indigo-500 to-purple-600 rounded-lg p-6 text-white">
            <div className="flex items-center justify-between">
              <div>
                <h2 className="text-lg font-semibold">Ops Agent Queue</h2>
                <p className="text-sm opacity-80">Automated remediation in progress</p>
              </div>
              <div className="flex items-center gap-6">
                <div className="text-center">
                  <p className="text-3xl font-bold">{queueSummary.totalItems}</p>
                  <p className="text-sm opacity-80">Pending Actions</p>
                </div>
                {queueSummary.criticalCount > 0 && (
                  <div className="text-center bg-red-500/20 rounded-lg px-4 py-2">
                    <p className="text-2xl font-bold">{queueSummary.criticalCount}</p>
                    <p className="text-sm opacity-80">Critical</p>
                  </div>
                )}
                <div className="text-center">
                  <p className="text-2xl font-bold">{queueSummary.totalEstimatedMinutes}</p>
                  <p className="text-sm opacity-80">Est. Minutes</p>
                </div>
                <button className="px-4 py-2 bg-white/20 hover:bg-white/30 rounded-lg text-sm font-medium transition">
                  Run All Actions
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Tab Navigation */}
        <div className="flex gap-1 bg-gray-100 rounded-lg p-1 w-fit">
          <button
            onClick={() => setActiveTab('events')}
            className={`px-4 py-2 rounded-lg text-sm font-medium ${
              activeTab === 'events' ? 'bg-white shadow text-gray-900' : 'text-gray-600'
            }`}
          >
            Pipeline Events ({events.length})
          </button>
          <button
            onClick={() => setActiveTab('queue')}
            className={`px-4 py-2 rounded-lg text-sm font-medium ${
              activeTab === 'queue' ? 'bg-white shadow text-gray-900' : 'text-gray-600'
            }`}
          >
            Agent Queue ({queue.length})
          </button>
          <button
            onClick={() => setActiveTab('domains')}
            className={`px-4 py-2 rounded-lg text-sm font-medium ${
              activeTab === 'domains' ? 'bg-white shadow text-gray-900' : 'text-gray-600'
            }`}
          >
            Domain Health ({domainSummaries.length})
          </button>
        </div>

        {/* Events Tab */}
        {activeTab === 'events' && (
          <div className="bg-white rounded-lg border border-gray-200">
            <div className="p-4 border-b border-gray-200 flex items-center justify-between">
              <h2 className="font-semibold text-gray-900">Pipeline Events</h2>
              <div className="flex gap-2">
                <select
                  value={eventFilter}
                  onChange={(e) => setEventFilter(e.target.value)}
                  className="text-sm border border-gray-200 rounded-lg px-3 py-1.5"
                >
                  <option value="all">All Events</option>
                  <option value="BUILD_ERROR">Errors</option>
                  <option value="BUILD_EMPTY">Empty</option>
                  <option value="DATASET_STALE">Stale</option>
                  <option value="PERFORMANCE_DEGRADED">Slow</option>
                  <option value="QUALITY_ALERT">Quality</option>
                  <option value="BUILD_COMPLETE">Complete</option>
                </select>
              </div>
            </div>
            <div className="divide-y divide-gray-100 max-h-[600px] overflow-y-auto">
              {filteredEvents.map((event, idx) => (
                <div key={idx} className={`p-4 hover:bg-gray-50 ${
                  event.severity === 'critical' ? 'bg-red-50 border-l-4 border-red-500' :
                  event.severity === 'warning' ? 'border-l-4 border-orange-500' : ''
                }`}>
                  <div className="flex items-start justify-between">
                    <div className="flex-1">
                      <div className="flex items-center gap-2">
                        <span className={`w-2 h-2 rounded-full ${getSeverityColor(event.severity)}`} />
                        <span className={`px-2 py-0.5 rounded text-xs ${getEventTypeColor(event.eventType)}`}>
                          {event.eventType.replace(/_/g, ' ')}
                        </span>
                        <span className="text-sm text-gray-500">{event.domain}</span>
                      </div>
                      <p className="text-sm text-gray-900 mt-1 font-medium">{event.message}</p>
                      <p className="text-xs text-gray-500 mt-1 font-mono">{event.datasetPath}</p>
                      <div className="flex items-center gap-4 mt-2 text-xs text-gray-500">
                        <span>{getTimeAgo(event.eventTs)}</span>
                        <span>Run: {event.runId}</span>
                        {event.suggestedAction && (
                          <span className="text-indigo-600">Suggested: {event.suggestedAction.replace(/_/g, ' ')}</span>
                        )}
                      </div>
                    </div>
                    {event.requiresAction && (
                      <button className="px-3 py-1.5 text-xs bg-indigo-100 text-indigo-700 rounded-lg hover:bg-indigo-200 flex items-center gap-1">
                        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d={getActionIcon(event.suggestedAction)} />
                        </svg>
                        Execute
                      </button>
                    )}
                  </div>
                </div>
              ))}
              {filteredEvents.length === 0 && (
                <div className="p-8 text-center text-gray-500">
                  No events found
                </div>
              )}
            </div>
          </div>
        )}

        {/* Queue Tab */}
        {activeTab === 'queue' && (
          <div className="bg-white rounded-lg border border-gray-200">
            <div className="p-4 border-b border-gray-200">
              <h2 className="font-semibold text-gray-900">Agent Action Queue</h2>
              <p className="text-sm text-gray-500">Prioritized actions for automated remediation</p>
            </div>
            <div className="divide-y divide-gray-100 max-h-[600px] overflow-y-auto">
              {queue.map((item, idx) => (
                <div key={idx} className={`p-4 hover:bg-gray-50 ${
                  item.severity === 'critical' ? 'bg-red-50' : ''
                }`}>
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-4">
                      <div className="w-8 h-8 rounded-full bg-indigo-100 flex items-center justify-center text-indigo-700 font-bold text-sm">
                        {item.queuePosition}
                      </div>
                      <div>
                        <div className="flex items-center gap-2">
                          <span className={`px-2 py-0.5 rounded text-xs ${getEventTypeColor(item.eventType)}`}>
                            {item.eventType.replace(/_/g, ' ')}
                          </span>
                          <span className="text-sm text-gray-500">{item.domain}</span>
                        </div>
                        <p className="text-sm text-gray-900 mt-1">{item.message}</p>
                        <p className="text-xs text-gray-500 font-mono">{item.datasetPath}</p>
                      </div>
                    </div>
                    <div className="flex items-center gap-4">
                      <div className="text-right">
                        <p className="text-sm font-medium text-gray-900">{item.suggestedAction?.replace(/_/g, ' ')}</p>
                        <p className="text-xs text-gray-500">~{item.estimatedResolutionMin} min</p>
                      </div>
                      <div className="flex gap-2">
                        <button className="px-3 py-1.5 text-xs bg-green-100 text-green-700 rounded-lg hover:bg-green-200">
                          Execute
                        </button>
                        <button className="px-3 py-1.5 text-xs bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200">
                          Skip
                        </button>
                      </div>
                    </div>
                  </div>
                </div>
              ))}
              {queue.length === 0 && (
                <div className="p-8 text-center text-gray-500">
                  <svg className="w-12 h-12 mx-auto text-gray-300 mb-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                  <p>All clear! No pending actions in queue.</p>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Domains Tab */}
        {activeTab === 'domains' && (
          <div className="grid grid-cols-2 gap-4">
            {domainSummaries.map((domain, idx) => (
              <div key={idx} className="bg-white rounded-lg border border-gray-200 p-4">
                <div className="flex items-center justify-between mb-4">
                  <h3 className="font-semibold text-gray-900">{domain.domain}</h3>
                  <div className="flex items-center gap-2">
                    <span className={`px-2 py-1 rounded text-xs font-medium ${
                      domain.healthPct >= 90 ? 'bg-green-100 text-green-700' :
                      domain.healthPct >= 70 ? 'bg-yellow-100 text-yellow-700' :
                      'bg-red-100 text-red-700'
                    }`}>
                      {domain.healthPct}% Healthy
                    </span>
                  </div>
                </div>

                <div className="grid grid-cols-4 gap-3 mb-4">
                  <div className="text-center">
                    <p className="text-2xl font-bold text-gray-900">{domain.totalDatasets}</p>
                    <p className="text-xs text-gray-500">Datasets</p>
                  </div>
                  <div className="text-center">
                    <p className="text-2xl font-bold text-green-600">{domain.healthyDatasets}</p>
                    <p className="text-xs text-gray-500">Healthy</p>
                  </div>
                  <div className="text-center">
                    <p className="text-2xl font-bold text-red-600">{domain.errorDatasets}</p>
                    <p className="text-xs text-gray-500">Errors</p>
                  </div>
                  <div className="text-center">
                    <p className="text-2xl font-bold text-purple-600">{domain.slowDatasets}</p>
                    <p className="text-xs text-gray-500">Slow</p>
                  </div>
                </div>

                {/* Score Bars */}
                <div className="space-y-2">
                  <div>
                    <div className="flex justify-between text-xs mb-1">
                      <span className="text-gray-500">Performance</span>
                      <span className="font-medium">{domain.performanceScore?.toFixed(0) || 0}</span>
                    </div>
                    <div className="h-2 bg-gray-100 rounded-full overflow-hidden">
                      <div
                        className="h-full bg-indigo-500 rounded-full"
                        style={{ width: `${Math.max(0, Math.min(100, domain.performanceScore || 0))}%` }}
                      />
                    </div>
                  </div>
                  <div>
                    <div className="flex justify-between text-xs mb-1">
                      <span className="text-gray-500">Quality</span>
                      <span className="font-medium">{domain.qualityScore?.toFixed(0) || 0}</span>
                    </div>
                    <div className="h-2 bg-gray-100 rounded-full overflow-hidden">
                      <div
                        className="h-full bg-green-500 rounded-full"
                        style={{ width: `${Math.max(0, Math.min(100, domain.qualityScore || 0))}%` }}
                      />
                    </div>
                  </div>
                </div>

                <div className="mt-4 pt-4 border-t border-gray-100 flex justify-between text-xs text-gray-500">
                  <span>{domain.totalRecords?.toLocaleString()} records</span>
                  <span>Avg: {domain.avgLatencyMs?.toFixed(0)}ms</span>
                  <span>Run: {domain.latestRunId}</span>
                </div>
              </div>
            ))}
            {domainSummaries.length === 0 && (
              <div className="col-span-2 bg-white rounded-lg border border-gray-200 p-8 text-center text-gray-500">
                No domain data available
              </div>
            )}
          </div>
        )}
      </main>
    </div>
  )
}
