'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import {
  fetchDashboardActions,
  fetchImpactSummary,
  type ImpactSummary,
} from '@/lib/foundry'
import type { DashboardActionItem } from '@/types'

function formatCurrency(value: number): string {
  if (value >= 1000000) return `$${(value / 1000000).toFixed(1)}M`
  if (value >= 1000) return `$${(value / 1000).toFixed(0)}K`
  return `$${value.toFixed(0)}`
}

function formatDate(dateStr: string): string {
  return new Date(dateStr).toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
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

function getPriorityColor(priority: number): string {
  if (priority >= 90) return 'bg-red-100 text-red-700 border-red-200'
  if (priority >= 70) return 'bg-orange-100 text-orange-700 border-orange-200'
  if (priority >= 50) return 'bg-yellow-100 text-yellow-700 border-yellow-200'
  return 'bg-gray-100 text-gray-700 border-gray-200'
}

function getHealthColor(category: string): string {
  switch (category) {
    case 'Healthy':
      return 'bg-green-100 text-green-700'
    case 'Monitor':
      return 'bg-yellow-100 text-yellow-700'
    case 'At Risk':
      return 'bg-orange-100 text-orange-700'
    case 'Critical':
      return 'bg-red-100 text-red-700'
    default:
      return 'bg-gray-100 text-gray-700'
  }
}

function getActionTypeIcon(actionType: string): string {
  switch (actionType) {
    case 'EXEC_SPONSOR_OUTREACH':
      return 'M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z'
    case 'MULTI_THREAD_DEAL':
      return 'M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z'
    case 'NEGOTIATE_TERMS':
      return 'M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z'
    case 'REENGAGE_CONTACTS':
      return 'M3 8l7.89 5.26a2 2 0 002.22 0L21 8M5 19h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z'
    default:
      return 'M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z'
  }
}

export default function ActionInboxPage() {
  const [actions, setActions] = useState<DashboardActionItem[]>([])
  const [impact, setImpact] = useState<ImpactSummary | null>(null)
  const [loading, setLoading] = useState(true)
  const [selectedAction, setSelectedAction] = useState<DashboardActionItem | null>(null)
  const [filterPriority, setFilterPriority] = useState<'all' | 'high' | 'medium' | 'low'>('all')

  useEffect(() => {
    Promise.all([fetchDashboardActions(), fetchImpactSummary()]).then(
      ([actionsData, impactData]) => {
        setActions(actionsData)
        setImpact(impactData)
        setLoading(false)
        if (actionsData.length > 0) {
          setSelectedAction(actionsData[0])
        }
      }
    )
  }, [])

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 p-8">
        <div className="animate-pulse">Loading action inbox...</div>
      </div>
    )
  }

  const filteredActions = actions.filter((action) => {
    if (filterPriority === 'all') return true
    if (filterPriority === 'high') return action.priority >= 80
    if (filterPriority === 'medium') return action.priority >= 50 && action.priority < 80
    return action.priority < 50
  })

  const highPriorityCount = actions.filter((a) => a.priority >= 80).length
  const totalExpectedImpact = actions.reduce((sum, a) => sum + a.expectedImpactAmount, 0)

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 px-6 py-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Link href="/" className="text-gray-500 hover:text-gray-700">
              <svg
                className="w-5 h-5"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M10 19l-7-7m0 0l7-7m-7 7h18"
                />
              </svg>
            </Link>
            <div>
              <h1 className="text-2xl font-bold text-gray-900">Action Inbox</h1>
              <p className="text-sm text-gray-500">
                AI-recommended next-best-actions for your pipeline
              </p>
            </div>
          </div>
          {highPriorityCount > 0 && (
            <div className="flex items-center gap-2 bg-red-50 px-3 py-1.5 rounded-lg">
              <svg
                className="w-5 h-5 text-red-500"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
                />
              </svg>
              <span className="text-sm font-medium text-red-700">
                {highPriorityCount} High Priority
              </span>
            </div>
          )}
        </div>
      </header>

      <main className="p-6 space-y-6">
        {/* Impact Summary Cards */}
        <div className="grid grid-cols-6 gap-4">
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Open Actions</p>
            <p className="text-2xl font-bold text-gray-900">{actions.length}</p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Expected Impact</p>
            <p className="text-2xl font-bold text-green-600">
              {formatCurrency(totalExpectedImpact)}
            </p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Decision Rate</p>
            <p className="text-2xl font-bold text-blue-600">
              {((impact?.decisionRate ?? 0) * 100).toFixed(0)}%
            </p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Avg Time to Decision</p>
            <p className="text-2xl font-bold text-gray-900">
              {(impact?.avgTimeToDecisionDays ?? 0).toFixed(1)}d
            </p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Realized Impact</p>
            <p className="text-2xl font-bold text-green-600">
              {formatCurrency(impact?.totalRealizedImpact ?? 0)}
            </p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">ROI vs Expected</p>
            <p className="text-2xl font-bold text-purple-600">
              {((impact?.realizedVsExpectedRatio ?? 0) * 100).toFixed(0)}%
            </p>
          </div>
        </div>

        {/* Main Content */}
        <div className="grid grid-cols-3 gap-6">
          {/* Actions List */}
          <div className="col-span-1 bg-white rounded-lg border border-gray-200">
            <div className="p-4 border-b border-gray-200">
              <div className="flex items-center justify-between">
                <h2 className="font-semibold text-gray-900">Actions</h2>
                <select
                  value={filterPriority}
                  onChange={(e) =>
                    setFilterPriority(e.target.value as typeof filterPriority)
                  }
                  className="text-sm border border-gray-200 rounded px-2 py-1"
                >
                  <option value="all">All Priorities</option>
                  <option value="high">High (80+)</option>
                  <option value="medium">Medium (50-79)</option>
                  <option value="low">Low (&lt;50)</option>
                </select>
              </div>
            </div>
            <div className="divide-y divide-gray-100 max-h-[600px] overflow-y-auto">
              {filteredActions.map((action) => (
                <button
                  key={action.actionId}
                  onClick={() => setSelectedAction(action)}
                  className={`w-full p-4 text-left hover:bg-gray-50 transition-colors ${
                    selectedAction?.actionId === action.actionId
                      ? 'bg-blue-50 border-l-4 border-blue-500'
                      : ''
                  }`}
                >
                  <div className="flex items-start justify-between">
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 mb-1">
                        <span
                          className={`px-2 py-0.5 rounded text-xs font-medium ${getPriorityColor(
                            action.priority
                          )}`}
                        >
                          P{action.priority}
                        </span>
                        <span
                          className={`px-2 py-0.5 rounded text-xs ${getHealthColor(
                            action.healthCategory
                          )}`}
                        >
                          {action.healthCategory}
                        </span>
                      </div>
                      <p className="font-medium text-gray-900 truncate">
                        {action.actionTitle}
                      </p>
                      <p className="text-sm text-gray-600 truncate">
                        {action.accountName}
                      </p>
                      <div className="flex items-center gap-3 mt-1 text-xs text-gray-500">
                        <span>{formatCurrency(action.amount)}</span>
                        <span>Due {formatDate(action.dueDate)}</span>
                      </div>
                    </div>
                  </div>
                </button>
              ))}
              {filteredActions.length === 0 && (
                <div className="p-8 text-center text-gray-500">
                  No actions match filters
                </div>
              )}
            </div>
          </div>

          {/* Action Detail */}
          <div className="col-span-2 bg-white rounded-lg border border-gray-200">
            {selectedAction ? (
              <div className="h-full flex flex-col">
                {/* Detail Header */}
                <div className="p-6 border-b border-gray-200">
                  <div className="flex items-start justify-between">
                    <div>
                      <div className="flex items-center gap-2 mb-2">
                        <span
                          className={`px-2 py-0.5 rounded text-xs font-medium ${getPriorityColor(
                            selectedAction.priority
                          )}`}
                        >
                          Priority {selectedAction.priority}
                        </span>
                        <span className="px-2 py-0.5 rounded text-xs bg-blue-100 text-blue-700">
                          {selectedAction.playbook}
                        </span>
                        <span
                          className={`px-2 py-0.5 rounded text-xs ${getHealthColor(
                            selectedAction.healthCategory
                          )}`}
                        >
                          Health: {selectedAction.healthScore}
                        </span>
                      </div>
                      <h2 className="text-xl font-bold text-gray-900">
                        {selectedAction.actionTitle}
                      </h2>
                      <p className="text-gray-600 mt-1">
                        {selectedAction.opportunityName}
                      </p>
                    </div>
                    <div className="text-right">
                      <p className="text-2xl font-bold text-gray-900">
                        {formatCurrency(selectedAction.amount)}
                      </p>
                      <p className="text-sm text-gray-500">Deal Value</p>
                    </div>
                  </div>
                </div>

                {/* Detail Content */}
                <div className="flex-1 p-6 space-y-6">
                  {/* Action Description */}
                  <div>
                    <h3 className="text-sm font-medium text-gray-500 mb-2">
                      Recommended Action
                    </h3>
                    <div className="flex items-start gap-3">
                      <div className="p-2 bg-blue-100 rounded-lg">
                        <svg
                          className="w-5 h-5 text-blue-600"
                          fill="none"
                          stroke="currentColor"
                          viewBox="0 0 24 24"
                        >
                          <path
                            strokeLinecap="round"
                            strokeLinejoin="round"
                            strokeWidth={2}
                            d={getActionTypeIcon(selectedAction.actionType)}
                          />
                        </svg>
                      </div>
                      <div>
                        <p className="text-gray-900">
                          {selectedAction.actionDescription}
                        </p>
                        {selectedAction.reasoning && (
                          <p className="text-sm text-gray-500 mt-2 italic">
                            {selectedAction.reasoning}
                          </p>
                        )}
                      </div>
                    </div>
                  </div>

                  {/* Deal Context */}
                  <div className="grid grid-cols-2 gap-4">
                    <div className="bg-gray-50 rounded-lg p-4">
                      <h3 className="text-sm font-medium text-gray-500 mb-3">
                        Deal Context
                      </h3>
                      <div className="space-y-2 text-sm">
                        <div className="flex justify-between">
                          <span className="text-gray-500">Account</span>
                          <span className="font-medium text-gray-900">
                            {selectedAction.accountName}
                          </span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-gray-500">Owner</span>
                          <span className="font-medium text-gray-900">
                            {selectedAction.ownerName}
                          </span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-gray-500">Segment</span>
                          <span className="font-medium text-gray-900">
                            {selectedAction.segment}
                          </span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-gray-500">Region</span>
                          <span className="font-medium text-gray-900">
                            {selectedAction.region}
                          </span>
                        </div>
                      </div>
                    </div>

                    <div className="bg-gray-50 rounded-lg p-4">
                      <h3 className="text-sm font-medium text-gray-500 mb-3">
                        Expected Impact
                      </h3>
                      <div className="space-y-2 text-sm">
                        <div className="flex justify-between">
                          <span className="text-gray-500">Impact Level</span>
                          <span className="font-medium text-gray-900">
                            {selectedAction.expectedImpact}
                          </span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-gray-500">Est. Value</span>
                          <span className="font-medium text-green-600">
                            {formatCurrency(selectedAction.expectedImpactAmount)}
                          </span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-gray-500">Confidence</span>
                          <span className="font-medium text-gray-900">
                            {(selectedAction.confidence * 100).toFixed(0)}%
                          </span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-gray-500">Due Date</span>
                          <span className="font-medium text-gray-900">
                            {formatDate(selectedAction.dueDate)}
                          </span>
                        </div>
                      </div>
                    </div>
                  </div>

                  {/* Metadata */}
                  <div className="flex items-center gap-4 text-xs text-gray-500">
                    <span>Source: {selectedAction.source}</span>
                    <span>Created {getTimeAgo(selectedAction.createdAt)}</span>
                    <span>Action ID: {selectedAction.actionId}</span>
                  </div>
                </div>

                {/* Action Buttons */}
                <div className="p-6 border-t border-gray-200 bg-gray-50">
                  <div className="flex items-center justify-between">
                    <div className="flex gap-2">
                      <button className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 font-medium">
                        Take Action
                      </button>
                      <button className="px-4 py-2 bg-white border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 font-medium">
                        Snooze
                      </button>
                      <button className="px-4 py-2 bg-white border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 font-medium">
                        Dismiss
                      </button>
                    </div>
                    <button className="px-4 py-2 text-blue-600 hover:text-blue-700 font-medium">
                      View Deal Details
                    </button>
                  </div>
                </div>
              </div>
            ) : (
              <div className="h-full flex items-center justify-center text-gray-500">
                Select an action to view details
              </div>
            )}
          </div>
        </div>

        {/* Impact History Summary */}
        {impact && (
          <div className="bg-white rounded-lg border border-gray-200 p-6">
            <h2 className="font-semibold text-gray-900 mb-4">
              Closed-Loop Impact Summary
            </h2>
            <div className="grid grid-cols-4 gap-6">
              <div>
                <p className="text-sm text-gray-500">Actions Created</p>
                <p className="text-2xl font-bold text-gray-900">
                  {impact.totalActionsCreated}
                </p>
              </div>
              <div>
                <p className="text-sm text-gray-500">Actions Actioned</p>
                <p className="text-2xl font-bold text-blue-600">
                  {impact.totalActionsActioned}
                </p>
              </div>
              <div>
                <p className="text-sm text-gray-500">Deals Won</p>
                <p className="text-2xl font-bold text-green-600">
                  {impact.dealsWon}
                </p>
              </div>
              <div>
                <p className="text-sm text-gray-500">Deals Lost</p>
                <p className="text-2xl font-bold text-red-600">
                  {impact.dealsLost}
                </p>
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  )
}
