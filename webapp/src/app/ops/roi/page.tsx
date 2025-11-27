'use client'

import { useState, useEffect } from 'react'
import { getTenantROIMetrics, type TenantROI } from '@/lib/foundry/ops'

// =============================================================================
// TYPES
// =============================================================================

interface ROIFilter {
  tier: 'all' | 'starter' | 'growth' | 'enterprise'
  minValue: number
}

// =============================================================================
// ROI METRIC CARD
// =============================================================================

function ROIMetricCard({
  label,
  value,
  format,
  trend,
  benchmark,
}: {
  label: string
  value: number
  format: 'percentage' | 'currency' | 'days'
  trend?: number
  benchmark?: number
}) {
  const formatValue = (val: number) => {
    switch (format) {
      case 'percentage':
        return `${val >= 0 ? '+' : ''}${val.toFixed(1)}%`
      case 'currency':
        return `$${val.toLocaleString()}`
      case 'days':
        return `${val.toFixed(0)} days`
      default:
        return val.toString()
    }
  }

  return (
    <div className="bg-white rounded-lg shadow p-6">
      <div className="text-sm text-gray-500 mb-1">{label}</div>
      <div className="text-3xl font-bold text-gray-900">
        {formatValue(value)}
      </div>
      {trend !== undefined && (
        <div className={`text-sm mt-2 flex items-center ${trend >= 0 ? 'text-green-600' : 'text-red-600'}`}>
          <svg
            className={`w-4 h-4 mr-1 ${trend < 0 ? 'rotate-180' : ''}`}
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 10l7-7m0 0l7 7m-7-7v18" />
          </svg>
          {Math.abs(trend).toFixed(1)}% vs last period
        </div>
      )}
      {benchmark !== undefined && (
        <div className="text-xs text-gray-400 mt-1">
          Benchmark: {formatValue(benchmark)}
        </div>
      )}
    </div>
  )
}

// =============================================================================
// CUSTOMER ROI ROW
// =============================================================================

function CustomerROIRow({
  roi,
  onGenerateReport,
}: {
  roi: TenantROI
  onGenerateReport: (customerId: string) => void
}) {
  const totalValue = roi.estimatedAnnualValue

  return (
    <tr className="hover:bg-gray-50">
      <td className="px-6 py-4 whitespace-nowrap">
        <div className="font-medium text-gray-900">{roi.customerName}</div>
        <div className="text-sm text-gray-500">{roi.customerId}</div>
      </td>
      <td className="px-6 py-4 whitespace-nowrap">
        <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
          roi.tier === 'enterprise' ? 'bg-purple-100 text-purple-800' :
          roi.tier === 'growth' ? 'bg-blue-100 text-blue-800' :
          'bg-gray-100 text-gray-800'
        }`}>
          {roi.tier}
        </span>
      </td>
      <td className="px-6 py-4 whitespace-nowrap">
        <span className={roi.winRateImprovement >= 0 ? 'text-green-600' : 'text-red-600'}>
          {roi.winRateImprovement >= 0 ? '+' : ''}{roi.winRateImprovement.toFixed(1)}%
        </span>
      </td>
      <td className="px-6 py-4 whitespace-nowrap">
        <span className={roi.cycleTimeReduction >= 0 ? 'text-green-600' : 'text-red-600'}>
          {roi.cycleTimeReduction >= 0 ? '-' : '+'}{Math.abs(roi.cycleTimeReduction).toFixed(0)}%
        </span>
      </td>
      <td className="px-6 py-4 whitespace-nowrap">
        <span className={roi.forecastAccuracyGain >= 0 ? 'text-green-600' : 'text-red-600'}>
          {roi.forecastAccuracyGain >= 0 ? '+' : ''}{roi.forecastAccuracyGain.toFixed(1)}%
        </span>
      </td>
      <td className="px-6 py-4 whitespace-nowrap font-medium text-gray-900">
        ${totalValue.toLocaleString()}
      </td>
      <td className="px-6 py-4 whitespace-nowrap">
        <button
          onClick={() => onGenerateReport(roi.customerId)}
          className="text-blue-600 hover:text-blue-800 text-sm font-medium"
        >
          Generate Report
        </button>
      </td>
    </tr>
  )
}

// =============================================================================
// AGGREGATE STATS
// =============================================================================

function AggregateStats({ roiData }: { roiData: TenantROI[] }) {
  const avgWinRate = roiData.reduce((sum, r) => sum + r.winRateImprovement, 0) / roiData.length
  const avgCycleTime = roiData.reduce((sum, r) => sum + r.cycleTimeReduction, 0) / roiData.length
  const avgForecast = roiData.reduce((sum, r) => sum + r.forecastAccuracyGain, 0) / roiData.length
  const totalValue = roiData.reduce((sum, r) => sum + r.estimatedAnnualValue, 0)

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
      <ROIMetricCard
        label="Avg Win Rate Improvement"
        value={avgWinRate}
        format="percentage"
        benchmark={5.2}
      />
      <ROIMetricCard
        label="Avg Cycle Time Reduction"
        value={avgCycleTime}
        format="percentage"
        benchmark={15.0}
      />
      <ROIMetricCard
        label="Avg Forecast Accuracy Gain"
        value={avgForecast}
        format="percentage"
        benchmark={8.5}
      />
      <ROIMetricCard
        label="Total Annual Value Created"
        value={totalValue}
        format="currency"
      />
    </div>
  )
}

// =============================================================================
// VALUE BREAKDOWN CHART
// =============================================================================

function ValueBreakdownChart({ roiData }: { roiData: TenantROI[] }) {
  // Group by tier
  const byTier = roiData.reduce((acc, roi) => {
    acc[roi.tier] = (acc[roi.tier] || 0) + roi.estimatedAnnualValue
    return acc
  }, {} as Record<string, number>)

  const total = Object.values(byTier).reduce((sum, val) => sum + val, 0)
  const tiers = ['starter', 'growth', 'enterprise']
  const colors = {
    starter: 'bg-gray-400',
    growth: 'bg-blue-500',
    enterprise: 'bg-purple-600',
  }

  return (
    <div className="bg-white rounded-lg shadow p-6 mb-6">
      <h3 className="text-lg font-semibold mb-4">Value by Tier</h3>
      <div className="flex h-8 rounded-full overflow-hidden mb-4">
        {tiers.map(tier => {
          const value = byTier[tier] || 0
          const percentage = total > 0 ? (value / total) * 100 : 0
          if (percentage === 0) return null
          return (
            <div
              key={tier}
              className={`${colors[tier as keyof typeof colors]}`}
              style={{ width: `${percentage}%` }}
              title={`${tier}: $${value.toLocaleString()}`}
            />
          )
        })}
      </div>
      <div className="flex justify-between text-sm">
        {tiers.map(tier => (
          <div key={tier} className="flex items-center">
            <div className={`w-3 h-3 rounded-full ${colors[tier as keyof typeof colors]} mr-2`} />
            <span className="capitalize">{tier}: ${(byTier[tier] || 0).toLocaleString()}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

// =============================================================================
// MAIN PAGE
// =============================================================================

export default function ROIDashboardPage() {
  const [roiData, setRoiData] = useState<TenantROI[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [filter, setFilter] = useState<ROIFilter>({ tier: 'all', minValue: 0 })
  const [generatingReport, setGeneratingReport] = useState<string | null>(null)

  useEffect(() => {
    async function loadData() {
      try {
        setLoading(true)
        const data = await getTenantROIMetrics()
        setRoiData(data)
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load ROI data')
      } finally {
        setLoading(false)
      }
    }
    loadData()
  }, [])

  const filteredData = roiData.filter(roi => {
    if (filter.tier !== 'all' && roi.tier !== filter.tier) return false
    if (roi.estimatedAnnualValue < filter.minValue) return false
    return true
  })

  const handleGenerateReport = async (customerId: string) => {
    setGeneratingReport(customerId)
    // Simulate report generation
    await new Promise(resolve => setTimeout(resolve, 2000))
    setGeneratingReport(null)
    // In production, this would trigger a download or open a new tab
    alert(`ROI Report generated for ${customerId}`)
  }

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4" />
          <p className="text-gray-600">Loading ROI data...</p>
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
              <h1 className="text-2xl font-bold text-gray-900">ROI Dashboard</h1>
              <p className="mt-1 text-sm text-gray-500">
                Track value creation across all customers
              </p>
            </div>
            <div className="flex items-center space-x-4">
              <button className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50">
                Export All
              </button>
              <button className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700">
                Generate Sales Deck
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Aggregate Stats */}
        <AggregateStats roiData={roiData} />

        {/* Value Breakdown */}
        <ValueBreakdownChart roiData={roiData} />

        {/* Filters */}
        <div className="bg-white rounded-lg shadow mb-6 p-4">
          <div className="flex flex-wrap items-center gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Tier</label>
              <select
                value={filter.tier}
                onChange={(e) => setFilter({ ...filter, tier: e.target.value as ROIFilter['tier'] })}
                className="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              >
                <option value="all">All Tiers</option>
                <option value="starter">Starter</option>
                <option value="growth">Growth</option>
                <option value="enterprise">Enterprise</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Min Value</label>
              <select
                value={filter.minValue}
                onChange={(e) => setFilter({ ...filter, minValue: parseInt(e.target.value) })}
                className="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              >
                <option value="0">Any</option>
                <option value="10000">$10K+</option>
                <option value="50000">$50K+</option>
                <option value="100000">$100K+</option>
                <option value="500000">$500K+</option>
              </select>
            </div>

            <div className="ml-auto text-sm text-gray-500">
              Showing {filteredData.length} of {roiData.length} customers
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
                  Win Rate
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Cycle Time
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Forecast Acc.
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Annual Value
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {filteredData.map((roi) => (
                <CustomerROIRow
                  key={roi.customerId}
                  roi={roi}
                  onGenerateReport={handleGenerateReport}
                />
              ))}
            </tbody>
          </table>

          {filteredData.length === 0 && (
            <div className="text-center py-12">
              <svg className="mx-auto h-12 w-12 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
              </svg>
              <h3 className="mt-2 text-sm font-medium text-gray-900">No customers match filters</h3>
              <p className="mt-1 text-sm text-gray-500">Try adjusting your filters.</p>
            </div>
          )}
        </div>

        {/* Loading overlay for report generation */}
        {generatingReport && (
          <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
            <div className="bg-white rounded-lg p-6 text-center">
              <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4" />
              <p className="text-gray-900 font-medium">Generating ROI Report...</p>
              <p className="text-gray-500 text-sm">This may take a moment</p>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
