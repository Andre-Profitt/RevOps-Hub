'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import {
  getCustomerImplementationStatus,
  getImplementationSummary,
  getTenantHealthScores,
  type CustomerImplementation,
  type TenantHealth,
} from '@/lib/foundry'

const STAGE_CONFIG: Record<string, { label: string; color: string; bgColor: string }> = {
  onboarding: { label: 'Onboarding', color: 'text-gray-700', bgColor: 'bg-gray-100' },
  connector_setup: { label: 'Connector Setup', color: 'text-blue-700', bgColor: 'bg-blue-100' },
  dashboard_config: { label: 'Dashboard Config', color: 'text-purple-700', bgColor: 'bg-purple-100' },
  agent_training: { label: 'Agent Training', color: 'text-orange-700', bgColor: 'bg-orange-100' },
  live: { label: 'Live', color: 'text-green-700', bgColor: 'bg-green-100' },
}

const TIER_CONFIG: Record<string, { label: string; color: string; bgColor: string }> = {
  starter: { label: 'Starter', color: 'text-gray-600', bgColor: 'bg-gray-50' },
  growth: { label: 'Growth', color: 'text-blue-600', bgColor: 'bg-blue-50' },
  enterprise: { label: 'Enterprise', color: 'text-purple-600', bgColor: 'bg-purple-50' },
}

const HEALTH_STATUS_CONFIG: Record<string, { label: string; color: string; bgColor: string }> = {
  healthy: { label: 'Healthy', color: 'text-green-700', bgColor: 'bg-green-100' },
  monitor: { label: 'Monitor', color: 'text-yellow-700', bgColor: 'bg-yellow-100' },
  at_risk: { label: 'At Risk', color: 'text-orange-700', bgColor: 'bg-orange-100' },
  critical: { label: 'Critical', color: 'text-red-700', bgColor: 'bg-red-100' },
}

function formatDate(dateStr: string | null): string {
  if (!dateStr) return '-'
  return new Date(dateStr).toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  })
}

function getTimeAgo(dateStr: string | null): string {
  if (!dateStr) return '-'
  const diff = Date.now() - new Date(dateStr).getTime()
  const hours = Math.floor(diff / 3600000)
  const days = Math.floor(diff / 86400000)

  if (days > 0) return `${days}d ago`
  if (hours > 0) return `${hours}h ago`
  return 'Just now'
}

interface ImplementationSummary {
  totalCustomers: number
  liveCustomers: number
  blockedCustomers: number
  avgCompletionPct: number
}

export default function CustomersPage() {
  const [customers, setCustomers] = useState<CustomerImplementation[]>([])
  const [healthScores, setHealthScores] = useState<TenantHealth[]>([])
  const [summary, setSummary] = useState<ImplementationSummary | null>(null)
  const [loading, setLoading] = useState(true)
  const [activeTab, setActiveTab] = useState<'all' | 'blocked' | 'live'>('all')
  const [stageFilter, setStageFilter] = useState<string>('all')
  const [tierFilter, setTierFilter] = useState<string>('all')
  const [selectedCustomer, setSelectedCustomer] = useState<CustomerImplementation | null>(null)

  useEffect(() => {
    Promise.all([
      getCustomerImplementationStatus(),
      getImplementationSummary(),
      getTenantHealthScores(),
    ]).then(([customersData, summaryData, healthData]) => {
      setCustomers(customersData || [])
      setSummary(summaryData)
      setHealthScores(healthData || [])
      setLoading(false)
    })
  }, [])

  const getHealthForCustomer = (customerId: string): TenantHealth | undefined => {
    return healthScores.find(h => h.customerId === customerId)
  }

  const filteredCustomers = customers.filter(c => {
    if (activeTab === 'blocked' && !c.isBlocked) return false
    if (activeTab === 'live' && c.currentStage !== 'live') return false
    if (stageFilter !== 'all' && c.currentStage !== stageFilter) return false
    if (tierFilter !== 'all' && c.tier !== tierFilter) return false
    return true
  })

  const stageCounts = customers.reduce((acc, c) => {
    acc[c.currentStage] = (acc[c.currentStage] || 0) + 1
    return acc
  }, {} as Record<string, number>)

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 p-8">
        <div className="animate-pulse">Loading Customer Workspaces...</div>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 px-6 py-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Link href="/ops" className="text-gray-500 hover:text-gray-700">
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" />
              </svg>
            </Link>
            <div>
              <h1 className="text-2xl font-bold text-gray-900">Customer Workspaces</h1>
              <p className="text-sm text-gray-500">Implementation status and rollout tracking</p>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <button className="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 text-sm font-medium flex items-center gap-2">
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
              </svg>
              Add Customer
            </button>
          </div>
        </div>
      </header>

      <main className="p-6 space-y-6">
        {/* Summary Cards */}
        <div className="grid grid-cols-5 gap-4">
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Total Customers</p>
            <p className="text-3xl font-bold text-gray-900">{summary?.totalCustomers || customers.length}</p>
          </div>
          <div className="bg-green-50 rounded-lg p-4 border border-green-200">
            <p className="text-sm text-green-700">Live</p>
            <p className="text-3xl font-bold text-green-600">{summary?.liveCustomers || stageCounts['live'] || 0}</p>
          </div>
          <div className="bg-red-50 rounded-lg p-4 border border-red-200">
            <p className="text-sm text-red-700">Blocked</p>
            <p className="text-3xl font-bold text-red-600">{summary?.blockedCustomers || customers.filter(c => c.isBlocked).length}</p>
          </div>
          <div className="bg-blue-50 rounded-lg p-4 border border-blue-200">
            <p className="text-sm text-blue-700">In Progress</p>
            <p className="text-3xl font-bold text-blue-600">
              {customers.filter(c => c.currentStage !== 'live' && !c.isBlocked).length}
            </p>
          </div>
          <div className="bg-purple-50 rounded-lg p-4 border border-purple-200">
            <p className="text-sm text-purple-700">Avg Completion</p>
            <p className="text-3xl font-bold text-purple-600">
              {Math.round(summary?.avgCompletionPct || customers.reduce((s, c) => s + c.completionPct, 0) / customers.length)}%
            </p>
          </div>
        </div>

        {/* Implementation Pipeline Visualization */}
        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Implementation Pipeline</h2>
          <div className="flex items-center justify-between">
            {Object.entries(STAGE_CONFIG).map(([stage, config], idx) => (
              <div key={stage} className="flex-1 relative">
                <div className="flex flex-col items-center">
                  <div className={`w-12 h-12 rounded-full ${config.bgColor} flex items-center justify-center text-lg font-bold ${config.color}`}>
                    {stageCounts[stage] || 0}
                  </div>
                  <span className={`text-sm mt-2 ${config.color}`}>{config.label}</span>
                </div>
                {idx < Object.keys(STAGE_CONFIG).length - 1 && (
                  <div className="absolute top-6 left-1/2 w-full h-0.5 bg-gray-200" />
                )}
              </div>
            ))}
          </div>
        </div>

        {/* Filters and Tabs */}
        <div className="flex items-center justify-between">
          <div className="flex gap-1 bg-gray-100 rounded-lg p-1">
            <button
              onClick={() => setActiveTab('all')}
              className={`px-4 py-2 rounded-lg text-sm font-medium ${
                activeTab === 'all' ? 'bg-white shadow text-gray-900' : 'text-gray-600'
              }`}
            >
              All ({customers.length})
            </button>
            <button
              onClick={() => setActiveTab('blocked')}
              className={`px-4 py-2 rounded-lg text-sm font-medium ${
                activeTab === 'blocked' ? 'bg-white shadow text-gray-900' : 'text-gray-600'
              }`}
            >
              Blocked ({customers.filter(c => c.isBlocked).length})
            </button>
            <button
              onClick={() => setActiveTab('live')}
              className={`px-4 py-2 rounded-lg text-sm font-medium ${
                activeTab === 'live' ? 'bg-white shadow text-gray-900' : 'text-gray-600'
              }`}
            >
              Live ({stageCounts['live'] || 0})
            </button>
          </div>
          <div className="flex gap-3">
            <select
              value={stageFilter}
              onChange={(e) => setStageFilter(e.target.value)}
              className="text-sm border border-gray-200 rounded-lg px-3 py-2"
            >
              <option value="all">All Stages</option>
              {Object.entries(STAGE_CONFIG).map(([stage, config]) => (
                <option key={stage} value={stage}>{config.label}</option>
              ))}
            </select>
            <select
              value={tierFilter}
              onChange={(e) => setTierFilter(e.target.value)}
              className="text-sm border border-gray-200 rounded-lg px-3 py-2"
            >
              <option value="all">All Tiers</option>
              {Object.entries(TIER_CONFIG).map(([tier, config]) => (
                <option key={tier} value={tier}>{config.label}</option>
              ))}
            </select>
          </div>
        </div>

        {/* Customer List */}
        <div className="bg-white rounded-lg border border-gray-200">
          <div className="divide-y divide-gray-100">
            {filteredCustomers.map((customer) => {
              const health = getHealthForCustomer(customer.customerId)
              const stageConfig = STAGE_CONFIG[customer.currentStage] || STAGE_CONFIG.onboarding
              const tierConfig = TIER_CONFIG[customer.tier] || TIER_CONFIG.starter
              const healthConfig = health ? HEALTH_STATUS_CONFIG[health.healthStatus] || HEALTH_STATUS_CONFIG.monitor : null

              return (
                <div
                  key={customer.customerId}
                  className={`p-4 hover:bg-gray-50 cursor-pointer ${
                    customer.isBlocked ? 'bg-red-50 border-l-4 border-red-500' : ''
                  }`}
                  onClick={() => setSelectedCustomer(customer)}
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-4">
                      {/* Company Avatar */}
                      <div className="w-12 h-12 rounded-lg bg-gradient-to-br from-indigo-500 to-purple-600 flex items-center justify-center text-white font-bold text-lg">
                        {customer.customerName.charAt(0)}
                      </div>
                      <div>
                        <div className="flex items-center gap-2">
                          <h3 className="font-semibold text-gray-900">{customer.customerName}</h3>
                          <span className={`px-2 py-0.5 rounded text-xs ${tierConfig.bgColor} ${tierConfig.color}`}>
                            {tierConfig.label}
                          </span>
                          {customer.isBlocked && (
                            <span className="px-2 py-0.5 rounded text-xs bg-red-100 text-red-700 flex items-center gap-1">
                              <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                              </svg>
                              Blocked
                            </span>
                          )}
                        </div>
                        <div className="flex items-center gap-3 mt-1 text-sm text-gray-500">
                          <span>{customer.customerId}</span>
                          <span>Onboarded {formatDate(customer.onboardedAt)}</span>
                          {customer.csOwner && <span>CS: {customer.csOwner}</span>}
                        </div>
                      </div>
                    </div>

                    <div className="flex items-center gap-6">
                      {/* Health Score */}
                      {healthConfig && (
                        <div className="text-center">
                          <div className={`w-10 h-10 rounded-full ${healthConfig.bgColor} flex items-center justify-center`}>
                            <span className={`text-sm font-bold ${healthConfig.color}`}>
                              {health?.compositeScore || '-'}
                            </span>
                          </div>
                          <span className="text-xs text-gray-500">Health</span>
                        </div>
                      )}

                      {/* Stage */}
                      <div className="text-center min-w-[100px]">
                        <span className={`px-3 py-1 rounded-full text-xs font-medium ${stageConfig.bgColor} ${stageConfig.color}`}>
                          {stageConfig.label}
                        </span>
                        <p className="text-xs text-gray-500 mt-1">{customer.daysInStage}d in stage</p>
                      </div>

                      {/* Progress Bar */}
                      <div className="w-32">
                        <div className="flex justify-between text-xs text-gray-500 mb-1">
                          <span>Progress</span>
                          <span>{customer.completionPct}%</span>
                        </div>
                        <div className="h-2 bg-gray-100 rounded-full overflow-hidden">
                          <div
                            className={`h-full rounded-full ${
                              customer.completionPct === 100 ? 'bg-green-500' :
                              customer.isBlocked ? 'bg-red-500' : 'bg-indigo-500'
                            }`}
                            style={{ width: `${customer.completionPct}%` }}
                          />
                        </div>
                      </div>

                      {/* Actions */}
                      <button className="p-2 hover:bg-gray-100 rounded-lg">
                        <svg className="w-5 h-5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                        </svg>
                      </button>
                    </div>
                  </div>
                </div>
              )
            })}
            {filteredCustomers.length === 0 && (
              <div className="p-8 text-center text-gray-500">
                No customers found matching filters
              </div>
            )}
          </div>
        </div>

        {/* Customer Detail Slideout */}
        {selectedCustomer && (
          <div className="fixed inset-0 bg-black/50 z-50" onClick={() => setSelectedCustomer(null)}>
            <div
              className="fixed right-0 top-0 bottom-0 w-[600px] bg-white shadow-xl overflow-y-auto"
              onClick={(e) => e.stopPropagation()}
            >
              <div className="p-6 border-b border-gray-200 flex items-center justify-between">
                <div>
                  <h2 className="text-xl font-bold text-gray-900">{selectedCustomer.customerName}</h2>
                  <p className="text-sm text-gray-500">{selectedCustomer.customerId}</p>
                </div>
                <button
                  onClick={() => setSelectedCustomer(null)}
                  className="p-2 hover:bg-gray-100 rounded-lg"
                >
                  <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>

              <div className="p-6 space-y-6">
                {/* Status */}
                <div>
                  <h3 className="text-sm font-medium text-gray-500 mb-3">Implementation Status</h3>
                  <div className="grid grid-cols-3 gap-4">
                    <div className="bg-gray-50 rounded-lg p-3">
                      <p className="text-xs text-gray-500">Stage</p>
                      <p className={`font-semibold ${STAGE_CONFIG[selectedCustomer.currentStage]?.color}`}>
                        {STAGE_CONFIG[selectedCustomer.currentStage]?.label}
                      </p>
                    </div>
                    <div className="bg-gray-50 rounded-lg p-3">
                      <p className="text-xs text-gray-500">Days in Stage</p>
                      <p className="font-semibold text-gray-900">{selectedCustomer.daysInStage}</p>
                    </div>
                    <div className="bg-gray-50 rounded-lg p-3">
                      <p className="text-xs text-gray-500">Completion</p>
                      <p className="font-semibold text-gray-900">{selectedCustomer.completionPct}%</p>
                    </div>
                  </div>
                </div>

                {/* Stage Checklist */}
                <div>
                  <h3 className="text-sm font-medium text-gray-500 mb-3">Stage Checklist</h3>
                  <div className="space-y-2">
                    <ChecklistItem label="Workspace Created" checked={selectedCustomer.workspaceCreated} />
                    <ChecklistItem label="Admin User Added" checked={selectedCustomer.adminUserAdded} />
                    <ChecklistItem label="CRM Connected" checked={selectedCustomer.crmConnected} />
                    <ChecklistItem label="First Sync Complete" checked={selectedCustomer.firstSyncComplete} />
                    <ChecklistItem label="Pipeline Dashboard Enabled" checked={selectedCustomer.pipelineDashboardEnabled} />
                    <ChecklistItem label="Hygiene Dashboard Enabled" checked={selectedCustomer.hygieneDashboardEnabled} />
                    <ChecklistItem label="Ops Agent Enabled" checked={selectedCustomer.opsAgentEnabled} />
                    <ChecklistItem label="Agent First Action" checked={selectedCustomer.agentFirstAction} />
                    <ChecklistItem label="Go-Live Approved" checked={selectedCustomer.goLiveApproved} />
                    <ChecklistItem label="CS Handoff Complete" checked={selectedCustomer.csHandoffComplete} />
                  </div>
                </div>

                {/* Activity Stats */}
                <div>
                  <h3 className="text-sm font-medium text-gray-500 mb-3">Activity</h3>
                  <div className="grid grid-cols-2 gap-4">
                    <div className="bg-gray-50 rounded-lg p-3">
                      <p className="text-xs text-gray-500">Dashboards Enabled</p>
                      <p className="text-xl font-bold text-gray-900">{selectedCustomer.dashboardsEnabledCount}</p>
                    </div>
                    <div className="bg-gray-50 rounded-lg p-3">
                      <p className="text-xs text-gray-500">Agent Actions</p>
                      <p className="text-xl font-bold text-gray-900">{selectedCustomer.totalAgentActions}</p>
                    </div>
                    <div className="bg-gray-50 rounded-lg p-3">
                      <p className="text-xs text-gray-500">Last Sync</p>
                      <p className="text-sm font-semibold text-gray-900">{getTimeAgo(selectedCustomer.lastSyncAt)}</p>
                    </div>
                    <div className="bg-gray-50 rounded-lg p-3">
                      <p className="text-xs text-gray-500">Last Agent Action</p>
                      <p className="text-sm font-semibold text-gray-900">{getTimeAgo(selectedCustomer.lastAgentActionAt)}</p>
                    </div>
                  </div>
                </div>

                {/* CS Owner */}
                {selectedCustomer.csOwner && (
                  <div>
                    <h3 className="text-sm font-medium text-gray-500 mb-3">Customer Success</h3>
                    <div className="flex items-center gap-3 bg-gray-50 rounded-lg p-3">
                      <div className="w-10 h-10 rounded-full bg-indigo-100 flex items-center justify-center text-indigo-700 font-semibold">
                        {selectedCustomer.csOwner.charAt(0)}
                      </div>
                      <div>
                        <p className="font-semibold text-gray-900">{selectedCustomer.csOwner}</p>
                        <p className="text-sm text-gray-500">CS Owner</p>
                      </div>
                    </div>
                  </div>
                )}

                {/* Actions */}
                <div className="flex gap-3">
                  {selectedCustomer.currentStage !== 'live' && (
                    <button className="flex-1 px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 text-sm font-medium">
                      Advance Stage
                    </button>
                  )}
                  <button className="px-4 py-2 border border-gray-200 rounded-lg hover:bg-gray-50 text-sm font-medium">
                    View Dashboard
                  </button>
                  <button className="px-4 py-2 border border-gray-200 rounded-lg hover:bg-gray-50 text-sm font-medium">
                    Edit Config
                  </button>
                </div>
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  )
}

function ChecklistItem({ label, checked }: { label: string; checked: boolean }) {
  return (
    <div className="flex items-center gap-3">
      <div className={`w-5 h-5 rounded-full flex items-center justify-center ${
        checked ? 'bg-green-100 text-green-600' : 'bg-gray-100 text-gray-400'
      }`}>
        {checked ? (
          <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
          </svg>
        ) : (
          <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
          </svg>
        )}
      </div>
      <span className={checked ? 'text-gray-900' : 'text-gray-500'}>{label}</span>
    </div>
  )
}
