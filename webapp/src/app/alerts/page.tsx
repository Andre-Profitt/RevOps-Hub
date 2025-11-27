'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import {
  getAlertSummary,
  getAlerts,
  getAlertRules,
} from '@/lib/foundry'
import type {
  AlertSummary,
  Alert,
  AlertRule,
} from '@/types'

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

export default function AlertsPage() {
  const [summary, setSummary] = useState<AlertSummary | null>(null)
  const [alerts, setAlerts] = useState<Alert[]>([])
  const [rules, setRules] = useState<AlertRule[]>([])
  const [loading, setLoading] = useState(true)
  const [activeTab, setActiveTab] = useState<'active' | 'acknowledged' | 'resolved'>('active')

  useEffect(() => {
    Promise.all([
      getAlertSummary(),
      getAlerts(),
      getAlertRules(),
    ]).then(([summaryData, alertsData, rulesData]) => {
      setSummary(summaryData)
      setAlerts(alertsData)
      setRules(rulesData)
      setLoading(false)
    })
  }, [])

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 p-8">
        <div className="animate-pulse">Loading alerts...</div>
      </div>
    )
  }

  const filteredAlerts = alerts.filter(a => {
    if (activeTab === 'active') return a.status === 'active'
    if (activeTab === 'acknowledged') return a.status === 'acknowledged'
    return a.status === 'resolved'
  })

  const activeCount = alerts.filter(a => a.status === 'active').length
  const acknowledgedCount = alerts.filter(a => a.status === 'acknowledged').length
  const resolvedCount = alerts.filter(a => a.status === 'resolved').length

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
              <h1 className="text-2xl font-bold text-gray-900">Alerts & Notifications</h1>
              <p className="text-sm text-gray-500">Monitor critical events and configure rules</p>
            </div>
          </div>
          {(summary?.criticalCount ?? 0) > 0 && (
            <div className="flex items-center gap-2 bg-red-50 px-3 py-1.5 rounded-lg animate-pulse">
              <svg className="w-5 h-5 text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
              </svg>
              <span className="text-sm font-medium text-red-700">{summary?.criticalCount} Critical Alerts</span>
            </div>
          )}
        </div>
      </header>

      <main className="p-6 space-y-6">
        {/* Summary Cards */}
        <div className="grid grid-cols-5 gap-4">
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Total Active</p>
            <p className="text-2xl font-bold text-gray-900">{summary?.totalActive}</p>
          </div>
          <div className="bg-red-50 rounded-lg p-4 border border-red-200">
            <p className="text-sm text-red-700">Critical</p>
            <p className="text-2xl font-bold text-red-600">{summary?.criticalCount}</p>
          </div>
          <div className="bg-orange-50 rounded-lg p-4 border border-orange-200">
            <p className="text-sm text-orange-700">High</p>
            <p className="text-2xl font-bold text-orange-600">{summary?.highCount}</p>
          </div>
          <div className="bg-yellow-50 rounded-lg p-4 border border-yellow-200">
            <p className="text-sm text-yellow-700">Medium</p>
            <p className="text-2xl font-bold text-yellow-600">{summary?.mediumCount}</p>
          </div>
          <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Low</p>
            <p className="text-2xl font-bold text-gray-600">{summary?.lowCount}</p>
          </div>
        </div>

        {/* Stats */}
        <div className="grid grid-cols-3 gap-4">
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Acknowledged</p>
            <p className="text-2xl font-bold text-blue-600">{summary?.acknowledgedCount}</p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Resolved Today</p>
            <p className="text-2xl font-bold text-green-600">{summary?.resolvedToday}</p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Avg Resolution Time</p>
            <p className="text-2xl font-bold text-gray-900">{summary?.avgResolutionTime?.toFixed(1)}h</p>
          </div>
        </div>

        <div className="grid grid-cols-3 gap-6">
          {/* Alerts List */}
          <div className="col-span-2 bg-white rounded-lg border border-gray-200">
            <div className="p-4 border-b border-gray-200">
              <div className="flex items-center justify-between">
                <h2 className="font-semibold text-gray-900">Alerts</h2>
                <div className="flex gap-1 bg-gray-100 rounded-lg p-1">
                  <button
                    onClick={() => setActiveTab('active')}
                    className={`px-3 py-1 rounded text-sm ${
                      activeTab === 'active' ? 'bg-white shadow text-gray-900' : 'text-gray-600'
                    }`}
                  >
                    Active ({activeCount})
                  </button>
                  <button
                    onClick={() => setActiveTab('acknowledged')}
                    className={`px-3 py-1 rounded text-sm ${
                      activeTab === 'acknowledged' ? 'bg-white shadow text-gray-900' : 'text-gray-600'
                    }`}
                  >
                    Acknowledged ({acknowledgedCount})
                  </button>
                  <button
                    onClick={() => setActiveTab('resolved')}
                    className={`px-3 py-1 rounded text-sm ${
                      activeTab === 'resolved' ? 'bg-white shadow text-gray-900' : 'text-gray-600'
                    }`}
                  >
                    Resolved ({resolvedCount})
                  </button>
                </div>
              </div>
            </div>
            <div className="divide-y divide-gray-100 max-h-[600px] overflow-y-auto">
              {filteredAlerts.map((alert, idx) => (
                <div key={idx} className={`p-4 hover:bg-gray-50 ${
                  alert.severity === 'critical' ? 'bg-red-50 border-l-4 border-red-500' :
                  alert.severity === 'high' ? 'border-l-4 border-orange-500' :
                  ''
                }`}>
                  <div className="flex items-start justify-between">
                    <div className="flex-1">
                      <div className="flex items-center gap-2">
                        <span className={`px-2 py-0.5 rounded text-xs ${
                          alert.severity === 'critical' ? 'bg-red-100 text-red-700' :
                          alert.severity === 'high' ? 'bg-orange-100 text-orange-700' :
                          alert.severity === 'medium' ? 'bg-yellow-100 text-yellow-700' :
                          'bg-gray-100 text-gray-700'
                        }`}>
                          {alert.severity}
                        </span>
                        <span className="font-medium text-gray-900">{alert.ruleName}</span>
                      </div>
                      <p className="text-sm text-gray-600 mt-1">{alert.message}</p>
                      <div className="flex items-center gap-4 mt-2 text-xs text-gray-500">
                        <span>{getTimeAgo(alert.triggeredAt)}</span>
                        {alert.relatedEntity && (
                          <span>{alert.relatedEntity}: {alert.relatedEntityId}</span>
                        )}
                        {alert.assignedTo && (
                          <span>Assigned: {alert.assignedTo}</span>
                        )}
                      </div>
                    </div>
                    <div className="flex gap-2">
                      {alert.status === 'active' && (
                        <>
                          <button className="px-3 py-1 text-xs bg-blue-100 text-blue-700 rounded hover:bg-blue-200">
                            Acknowledge
                          </button>
                          <button className="px-3 py-1 text-xs bg-green-100 text-green-700 rounded hover:bg-green-200">
                            Resolve
                          </button>
                        </>
                      )}
                      {alert.status === 'acknowledged' && (
                        <button className="px-3 py-1 text-xs bg-green-100 text-green-700 rounded hover:bg-green-200">
                          Resolve
                        </button>
                      )}
                    </div>
                  </div>
                </div>
              ))}
              {filteredAlerts.length === 0 && (
                <div className="p-8 text-center text-gray-500">
                  No {activeTab} alerts
                </div>
              )}
            </div>
          </div>

          {/* Alert Rules */}
          <div className="bg-white rounded-lg border border-gray-200">
            <div className="p-4 border-b border-gray-200">
              <h2 className="font-semibold text-gray-900">Alert Rules</h2>
            </div>
            <div className="divide-y divide-gray-100 max-h-[600px] overflow-y-auto">
              {rules.map((rule, idx) => (
                <div key={idx} className="p-4">
                  <div className="flex items-center justify-between mb-1">
                    <span className="font-medium text-gray-900">{rule.name}</span>
                    <span className={`w-2 h-2 rounded-full ${rule.enabled ? 'bg-green-500' : 'bg-gray-300'}`} />
                  </div>
                  <p className="text-xs text-gray-500 mb-2">{rule.description}</p>
                  <div className="flex items-center justify-between text-xs">
                    <div className="flex items-center gap-2">
                      <span className={`px-2 py-0.5 rounded ${
                        rule.severity === 'critical' ? 'bg-red-100 text-red-700' :
                        rule.severity === 'high' ? 'bg-orange-100 text-orange-700' :
                        rule.severity === 'medium' ? 'bg-yellow-100 text-yellow-700' :
                        'bg-gray-100 text-gray-700'
                      }`}>
                        {rule.severity}
                      </span>
                      <span className="text-gray-500">{rule.category}</span>
                    </div>
                    <span className="text-gray-500">{rule.triggeredCount30d} in 30d</span>
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
