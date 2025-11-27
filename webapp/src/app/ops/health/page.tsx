'use client'

import { useState, useEffect } from 'react'
import {
  getTenantHealthScores,
  getTenantHealthTrends,
  type TenantHealth,
} from '@/lib/foundry/ops'

// =============================================================================
// TYPES
// =============================================================================

type HealthFilter = 'all' | 'critical' | 'warning' | 'healthy'
type TrendFilter = 'all' | 'improving' | 'stable' | 'declining'

interface HealthTrendPoint {
  date: string
  score: number
}

// =============================================================================
// HEALTH SCORE BADGE
// =============================================================================

function HealthBadge({ score }: { score: number }) {
  let color = 'bg-green-100 text-green-800'
  let label = 'Healthy'

  if (score < 50) {
    color = 'bg-red-100 text-red-800'
    label = 'Critical'
  } else if (score < 70) {
    color = 'bg-yellow-100 text-yellow-800'
    label = 'Warning'
  }

  return (
    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${color}`}>
      {label}
    </span>
  )
}

// =============================================================================
// TREND INDICATOR
// =============================================================================

function TrendIndicator({ trend }: { trend: string }) {
  if (trend === 'improving') {
    return (
      <span className="inline-flex items-center text-green-600">
        <svg className="w-4 h-4 mr-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 10l7-7m0 0l7 7m-7-7v18" />
        </svg>
        Improving
      </span>
    )
  } else if (trend === 'declining') {
    return (
      <span className="inline-flex items-center text-red-600">
        <svg className="w-4 h-4 mr-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 14l-7 7m0 0l-7-7m7 7V3" />
        </svg>
        Declining
      </span>
    )
  }

  return (
    <span className="inline-flex items-center text-gray-500">
      <svg className="w-4 h-4 mr-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 12h14" />
      </svg>
      Stable
    </span>
  )
}

// =============================================================================
// SCORE GAUGE
// =============================================================================

function ScoreGauge({ score, label }: { score: number; label: string }) {
  const circumference = 2 * Math.PI * 40
  const offset = circumference - (score / 100) * circumference

  let strokeColor = '#22c55e' // green
  if (score < 50) strokeColor = '#ef4444' // red
  else if (score < 70) strokeColor = '#eab308' // yellow

  return (
    <div className="flex flex-col items-center">
      <div className="relative w-24 h-24">
        <svg className="w-24 h-24 transform -rotate-90">
          <circle
            cx="48"
            cy="48"
            r="40"
            stroke="#e5e7eb"
            strokeWidth="8"
            fill="none"
          />
          <circle
            cx="48"
            cy="48"
            r="40"
            stroke={strokeColor}
            strokeWidth="8"
            fill="none"
            strokeDasharray={circumference}
            strokeDashoffset={offset}
            strokeLinecap="round"
          />
        </svg>
        <div className="absolute inset-0 flex items-center justify-center">
          <span className="text-xl font-bold">{Math.round(score)}</span>
        </div>
      </div>
      <span className="mt-2 text-sm text-gray-600">{label}</span>
    </div>
  )
}

// =============================================================================
// SUMMARY CARDS
// =============================================================================

function SummaryCards({ healthData }: { healthData: TenantHealth[] }) {
  const critical = healthData.filter(h => h.overallScore < 50).length
  const warning = healthData.filter(h => h.overallScore >= 50 && h.overallScore < 70).length
  const healthy = healthData.filter(h => h.overallScore >= 70).length
  const avgScore = healthData.reduce((sum, h) => sum + h.overallScore, 0) / healthData.length

  const cards = [
    { label: 'Total Customers', value: healthData.length, color: 'bg-blue-50 text-blue-700' },
    { label: 'Critical', value: critical, color: 'bg-red-50 text-red-700' },
    { label: 'Warning', value: warning, color: 'bg-yellow-50 text-yellow-700' },
    { label: 'Healthy', value: healthy, color: 'bg-green-50 text-green-700' },
    { label: 'Avg Score', value: `${Math.round(avgScore)}%`, color: 'bg-purple-50 text-purple-700' },
  ]

  return (
    <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-6">
      {cards.map((card) => (
        <div key={card.label} className={`rounded-lg p-4 ${card.color}`}>
          <div className="text-2xl font-bold">{card.value}</div>
          <div className="text-sm opacity-75">{card.label}</div>
        </div>
      ))}
    </div>
  )
}

// =============================================================================
// CUSTOMER HEALTH ROW
// =============================================================================

function CustomerHealthRow({
  health,
  onSelect,
}: {
  health: TenantHealth
  onSelect: (customerId: string) => void
}) {
  return (
    <tr
      className="hover:bg-gray-50 cursor-pointer"
      onClick={() => onSelect(health.customerId)}
    >
      <td className="px-6 py-4 whitespace-nowrap">
        <div className="font-medium text-gray-900">{health.customerName}</div>
        <div className="text-sm text-gray-500">{health.customerId}</div>
      </td>
      <td className="px-6 py-4 whitespace-nowrap">
        <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
          health.tier === 'enterprise' ? 'bg-purple-100 text-purple-800' :
          health.tier === 'growth' ? 'bg-blue-100 text-blue-800' :
          'bg-gray-100 text-gray-800'
        }`}>
          {health.tier}
        </span>
      </td>
      <td className="px-6 py-4 whitespace-nowrap">
        <div className="flex items-center">
          <div className="w-16 bg-gray-200 rounded-full h-2 mr-2">
            <div
              className={`h-2 rounded-full ${
                health.overallScore >= 70 ? 'bg-green-500' :
                health.overallScore >= 50 ? 'bg-yellow-500' : 'bg-red-500'
              }`}
              style={{ width: `${health.overallScore}%` }}
            />
          </div>
          <span className="text-sm font-medium">{Math.round(health.overallScore)}</span>
        </div>
      </td>
      <td className="px-6 py-4 whitespace-nowrap">
        <HealthBadge score={health.overallScore} />
      </td>
      <td className="px-6 py-4 whitespace-nowrap">
        <TrendIndicator trend={health.trend} />
      </td>
      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
        {new Date(health.lastUpdated).toLocaleDateString()}
      </td>
    </tr>
  )
}

// =============================================================================
// CUSTOMER DETAIL PANEL
// =============================================================================

function CustomerDetailPanel({
  health,
  trends,
  onClose,
}: {
  health: TenantHealth
  trends: HealthTrendPoint[]
  onClose: () => void
}) {
  return (
    <div className="fixed inset-y-0 right-0 w-full max-w-lg bg-white shadow-xl overflow-y-auto">
      <div className="p-6">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div>
            <h2 className="text-xl font-bold text-gray-900">{health.customerName}</h2>
            <p className="text-sm text-gray-500">{health.customerId}</p>
          </div>
          <button
            onClick={onClose}
            className="p-2 text-gray-400 hover:text-gray-600"
          >
            <svg className="w-6 h-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Overall Score */}
        <div className="bg-gray-50 rounded-lg p-6 mb-6">
          <div className="flex items-center justify-between">
            <div>
              <div className="text-4xl font-bold">{Math.round(health.overallScore)}</div>
              <div className="text-sm text-gray-500">Overall Health Score</div>
            </div>
            <div className="flex flex-col items-end">
              <HealthBadge score={health.overallScore} />
              <div className="mt-2">
                <TrendIndicator trend={health.trend} />
              </div>
            </div>
          </div>
        </div>

        {/* Dimension Scores */}
        <h3 className="text-lg font-semibold mb-4">Health Dimensions</h3>
        <div className="grid grid-cols-2 gap-4 mb-6">
          <ScoreGauge score={health.dataFreshness} label="Data Freshness" />
          <ScoreGauge score={health.adoption} label="Adoption" />
          <ScoreGauge score={health.aiAccuracy} label="AI Accuracy" />
          <ScoreGauge score={health.buildHealth} label="Build Health" />
        </div>

        {/* Trend Chart (simplified) */}
        <h3 className="text-lg font-semibold mb-4">30-Day Trend</h3>
        <div className="bg-gray-50 rounded-lg p-4 mb-6">
          <div className="flex items-end h-32 space-x-1">
            {trends.slice(-30).map((point, i) => (
              <div
                key={i}
                className={`flex-1 rounded-t ${
                  point.score >= 70 ? 'bg-green-400' :
                  point.score >= 50 ? 'bg-yellow-400' : 'bg-red-400'
                }`}
                style={{ height: `${point.score}%` }}
                title={`${point.date}: ${point.score}`}
              />
            ))}
          </div>
          <div className="flex justify-between mt-2 text-xs text-gray-500">
            <span>30 days ago</span>
            <span>Today</span>
          </div>
        </div>

        {/* Actions */}
        <h3 className="text-lg font-semibold mb-4">Recommended Actions</h3>
        <div className="space-y-3">
          {health.dataFreshness < 70 && (
            <div className="flex items-start p-3 bg-yellow-50 rounded-lg">
              <svg className="w-5 h-5 text-yellow-500 mr-2 mt-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
              <div>
                <div className="font-medium text-yellow-800">Check Data Sync</div>
                <div className="text-sm text-yellow-700">Data freshness is below target. Verify connector status.</div>
              </div>
            </div>
          )}
          {health.adoption < 70 && (
            <div className="flex items-start p-3 bg-blue-50 rounded-lg">
              <svg className="w-5 h-5 text-blue-500 mr-2 mt-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z" />
              </svg>
              <div>
                <div className="font-medium text-blue-800">Boost User Adoption</div>
                <div className="text-sm text-blue-700">Schedule training session or share quick-start guide.</div>
              </div>
            </div>
          )}
          {health.buildHealth < 70 && (
            <div className="flex items-start p-3 bg-red-50 rounded-lg">
              <svg className="w-5 h-5 text-red-500 mr-2 mt-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
              </svg>
              <div>
                <div className="font-medium text-red-800">Address Build Failures</div>
                <div className="text-sm text-red-700">Pipeline builds are failing. Review transform logs.</div>
              </div>
            </div>
          )}
          {health.overallScore >= 70 && (
            <div className="flex items-start p-3 bg-green-50 rounded-lg">
              <svg className="w-5 h-5 text-green-500 mr-2 mt-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
              <div>
                <div className="font-medium text-green-800">Looking Good!</div>
                <div className="text-sm text-green-700">This customer is healthy. Consider upsell opportunities.</div>
              </div>
            </div>
          )}
        </div>

        {/* Contact Info */}
        <div className="mt-6 pt-6 border-t">
          <h3 className="text-lg font-semibold mb-4">Quick Actions</h3>
          <div className="space-y-2">
            <button className="w-full px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700">
              Schedule Check-in Call
            </button>
            <button className="w-full px-4 py-2 text-sm font-medium text-blue-600 bg-blue-50 rounded-lg hover:bg-blue-100">
              View Full Analytics
            </button>
            <button className="w-full px-4 py-2 text-sm font-medium text-gray-600 bg-gray-100 rounded-lg hover:bg-gray-200">
              Export Health Report
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}

// =============================================================================
// MAIN PAGE
// =============================================================================

export default function CSHealthConsolePage() {
  const [healthData, setHealthData] = useState<TenantHealth[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  // Filters
  const [healthFilter, setHealthFilter] = useState<HealthFilter>('all')
  const [trendFilter, setTrendFilter] = useState<TrendFilter>('all')
  const [searchQuery, setSearchQuery] = useState('')

  // Detail panel
  const [selectedCustomerId, setSelectedCustomerId] = useState<string | null>(null)
  const [selectedTrends, setSelectedTrends] = useState<HealthTrendPoint[]>([])

  useEffect(() => {
    async function loadData() {
      try {
        setLoading(true)
        const data = await getTenantHealthScores()
        setHealthData(data)
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load health data')
      } finally {
        setLoading(false)
      }
    }
    loadData()
  }, [])

  // Load trends when customer selected
  useEffect(() => {
    async function loadTrends() {
      if (selectedCustomerId) {
        try {
          const trends = await getTenantHealthTrends(selectedCustomerId)
          setSelectedTrends(trends)
        } catch {
          setSelectedTrends([])
        }
      }
    }
    loadTrends()
  }, [selectedCustomerId])

  // Filter data
  const filteredData = healthData.filter(h => {
    // Health filter
    if (healthFilter === 'critical' && h.overallScore >= 50) return false
    if (healthFilter === 'warning' && (h.overallScore < 50 || h.overallScore >= 70)) return false
    if (healthFilter === 'healthy' && h.overallScore < 70) return false

    // Trend filter
    if (trendFilter !== 'all' && h.trend !== trendFilter) return false

    // Search
    if (searchQuery) {
      const query = searchQuery.toLowerCase()
      if (!h.customerName.toLowerCase().includes(query) &&
          !h.customerId.toLowerCase().includes(query)) {
        return false
      }
    }

    return true
  })

  const selectedHealth = selectedCustomerId
    ? healthData.find(h => h.customerId === selectedCustomerId)
    : null

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4" />
          <p className="text-gray-600">Loading health data...</p>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="bg-red-50 border border-red-200 rounded-lg p-6 max-w-md">
          <h2 className="text-lg font-semibold text-red-800 mb-2">Error Loading Data</h2>
          <p className="text-red-600">{error}</p>
        </div>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <div className="bg-white shadow">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-gray-900">CS Health Console</h1>
              <p className="mt-1 text-sm text-gray-500">
                Monitor customer health and proactively address issues
              </p>
            </div>
            <div className="flex items-center space-x-4">
              <button className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700">
                Export Report
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Summary Cards */}
        <SummaryCards healthData={healthData} />

        {/* Filters */}
        <div className="bg-white rounded-lg shadow mb-6 p-4">
          <div className="flex flex-wrap items-center gap-4">
            {/* Search */}
            <div className="flex-1 min-w-[200px]">
              <input
                type="text"
                placeholder="Search customers..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
            </div>

            {/* Health Filter */}
            <div>
              <select
                value={healthFilter}
                onChange={(e) => setHealthFilter(e.target.value as HealthFilter)}
                className="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              >
                <option value="all">All Health</option>
                <option value="critical">Critical (&lt;50)</option>
                <option value="warning">Warning (50-70)</option>
                <option value="healthy">Healthy (&gt;70)</option>
              </select>
            </div>

            {/* Trend Filter */}
            <div>
              <select
                value={trendFilter}
                onChange={(e) => setTrendFilter(e.target.value as TrendFilter)}
                className="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              >
                <option value="all">All Trends</option>
                <option value="improving">Improving</option>
                <option value="stable">Stable</option>
                <option value="declining">Declining</option>
              </select>
            </div>
          </div>
        </div>

        {/* Customer Table */}
        <div className="bg-white rounded-lg shadow overflow-hidden">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Customer
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Tier
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Score
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Status
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Trend
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Last Updated
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {filteredData.map((health) => (
                <CustomerHealthRow
                  key={health.customerId}
                  health={health}
                  onSelect={setSelectedCustomerId}
                />
              ))}
            </tbody>
          </table>

          {filteredData.length === 0 && (
            <div className="text-center py-12">
              <svg className="mx-auto h-12 w-12 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.172 16.172a4 4 0 015.656 0M9 10h.01M15 10h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
              <h3 className="mt-2 text-sm font-medium text-gray-900">No customers found</h3>
              <p className="mt-1 text-sm text-gray-500">Try adjusting your filters.</p>
            </div>
          )}
        </div>
      </div>

      {/* Detail Panel */}
      {selectedHealth && (
        <>
          <div
            className="fixed inset-0 bg-black bg-opacity-50 z-40"
            onClick={() => setSelectedCustomerId(null)}
          />
          <div className="z-50">
            <CustomerDetailPanel
              health={selectedHealth}
              trends={selectedTrends}
              onClose={() => setSelectedCustomerId(null)}
            />
          </div>
        </>
      )}
    </div>
  )
}
