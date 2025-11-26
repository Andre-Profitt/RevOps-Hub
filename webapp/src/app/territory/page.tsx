'use client'

import { useState, useEffect } from 'react'
import Link from 'next/link'
import {
  getTerritorySummary,
  getAccountScores,
  getTerritoryBalance,
  getWhiteSpaceAccounts,
} from '@/lib/foundry'
import type { TerritorySummary, AccountScore, TerritoryBalance, WhiteSpaceAccount } from '@/types'

// Format currency
function formatCurrency(amount: number): string {
  if (amount >= 1000000000) {
    return `$${(amount / 1000000000).toFixed(1)}B`
  }
  if (amount >= 1000000) {
    return `$${(amount / 1000000).toFixed(1)}M`
  }
  if (amount >= 1000) {
    return `$${(amount / 1000).toFixed(0)}K`
  }
  return `$${amount.toFixed(0)}`
}

// Get tier color classes
function getTierColor(tier: string): string {
  switch (tier) {
    case 'High':
      return 'bg-emerald-100 text-emerald-800'
    case 'Medium':
      return 'bg-amber-100 text-amber-800'
    case 'Low':
      return 'bg-red-100 text-red-800'
    default:
      return 'bg-gray-100 text-gray-800'
  }
}

// Get balance status color
function getBalanceColor(status: string): string {
  switch (status) {
    case 'Balanced':
      return 'bg-emerald-100 text-emerald-800'
    case 'Underloaded':
      return 'bg-blue-100 text-blue-800'
    case 'Overloaded':
      return 'bg-red-100 text-red-800'
    default:
      return 'bg-gray-100 text-gray-800'
  }
}

export default function TerritoryPage() {
  const [summary, setSummary] = useState<TerritorySummary | null>(null)
  const [accounts, setAccounts] = useState<AccountScore[]>([])
  const [territories, setTerritories] = useState<TerritoryBalance[]>([])
  const [whiteSpace, setWhiteSpace] = useState<WhiteSpaceAccount[]>([])
  const [loading, setLoading] = useState(true)
  const [activeTab, setActiveTab] = useState<'accounts' | 'territories' | 'whitespace'>('accounts')

  useEffect(() => {
    async function loadData() {
      try {
        const [summaryData, accountsData, territoriesData, whiteSpaceData] = await Promise.all([
          getTerritorySummary(),
          getAccountScores(),
          getTerritoryBalance(),
          getWhiteSpaceAccounts(),
        ])
        setSummary(summaryData)
        setAccounts(accountsData)
        setTerritories(territoriesData)
        setWhiteSpace(whiteSpaceData)
      } catch (error) {
        console.error('Error loading territory data:', error)
      } finally {
        setLoading(false)
      }
    }
    loadData()
  }, [])

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
          <p className="mt-4 text-gray-600">Loading territory data...</p>
        </div>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white shadow-sm border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center py-4">
            <div className="flex items-center space-x-4">
              <Link href="/" className="text-gray-500 hover:text-gray-700">
                <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" />
                </svg>
              </Link>
              <div>
                <h1 className="text-2xl font-bold text-gray-900">Territory & Account Planning</h1>
                <p className="text-sm text-gray-500">Account propensity scoring, territory balance, and white space analysis</p>
              </div>
            </div>
            <div className="text-sm text-gray-500">
              Last updated: {summary?.snapshotDate || 'N/A'}
            </div>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Summary KPIs */}
        {summary && (
          <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-4 mb-8">
            <div className="bg-white rounded-lg shadow p-4">
              <p className="text-xs text-gray-500 uppercase tracking-wide">Territories</p>
              <p className="text-2xl font-bold text-gray-900">{summary.totalTerritories}</p>
              <p className="text-xs text-gray-500">{summary.balancedTerritories} balanced</p>
            </div>
            <div className="bg-white rounded-lg shadow p-4">
              <p className="text-xs text-gray-500 uppercase tracking-wide">Total Accounts</p>
              <p className="text-2xl font-bold text-gray-900">{summary.totalAccounts.toLocaleString()}</p>
              <p className="text-xs text-gray-500">{summary.avgAccountsPerTerritory} avg/territory</p>
            </div>
            <div className="bg-white rounded-lg shadow p-4">
              <p className="text-xs text-gray-500 uppercase tracking-wide">High Propensity</p>
              <p className="text-2xl font-bold text-emerald-600">{summary.highPropensityCount}</p>
              <p className="text-xs text-gray-500">{formatCurrency(summary.highPropensityRevenuePotential)} potential</p>
            </div>
            <div className="bg-white rounded-lg shadow p-4">
              <p className="text-xs text-gray-500 uppercase tracking-wide">Avg Propensity</p>
              <p className="text-2xl font-bold text-gray-900">{summary.avgPropensityScore.toFixed(1)}</p>
              <p className="text-xs text-gray-500">out of 100</p>
            </div>
            <div className="bg-white rounded-lg shadow p-4">
              <p className="text-xs text-gray-500 uppercase tracking-wide">White Space</p>
              <p className="text-2xl font-bold text-blue-600">{summary.whiteSpaceAccounts}</p>
              <p className="text-xs text-gray-500">{formatCurrency(summary.whiteSpaceRevenuePotential)} potential</p>
            </div>
            <div className="bg-white rounded-lg shadow p-4">
              <p className="text-xs text-gray-500 uppercase tracking-wide">Territory Balance</p>
              <p className="text-2xl font-bold text-gray-900">{(summary.avgTerritoryBalance * 100).toFixed(0)}%</p>
              <div className="flex gap-1 mt-1">
                <span className="text-xs text-red-600">{summary.overloadedTerritories} over</span>
                <span className="text-xs text-gray-400">|</span>
                <span className="text-xs text-blue-600">{summary.underloadedTerritories} under</span>
              </div>
            </div>
          </div>
        )}

        {/* Tab Navigation */}
        <div className="bg-white rounded-lg shadow mb-6">
          <div className="border-b border-gray-200">
            <nav className="flex -mb-px">
              <button
                onClick={() => setActiveTab('accounts')}
                className={`px-6 py-4 text-sm font-medium border-b-2 ${
                  activeTab === 'accounts'
                    ? 'border-blue-500 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }`}
              >
                Account Scoring ({accounts.length})
              </button>
              <button
                onClick={() => setActiveTab('territories')}
                className={`px-6 py-4 text-sm font-medium border-b-2 ${
                  activeTab === 'territories'
                    ? 'border-blue-500 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }`}
              >
                Territory Balance ({territories.length})
              </button>
              <button
                onClick={() => setActiveTab('whitespace')}
                className={`px-6 py-4 text-sm font-medium border-b-2 ${
                  activeTab === 'whitespace'
                    ? 'border-blue-500 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }`}
              >
                White Space Analysis ({whiteSpace.length})
              </button>
            </nav>
          </div>

          {/* Account Scoring Tab */}
          {activeTab === 'accounts' && (
            <div className="p-6">
              <div className="mb-4">
                <h3 className="text-lg font-semibold text-gray-900">Account Propensity Scores</h3>
                <p className="text-sm text-gray-500">ML-driven scoring based on firmographic, behavioral, engagement, and pipeline signals</p>
              </div>
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Account</th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Segment</th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Territory</th>
                      <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">Propensity</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Current ARR</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Expansion</th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Recommended Action</th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {accounts.map((account) => (
                      <tr key={account.accountId} className="hover:bg-gray-50">
                        <td className="px-4 py-4">
                          <div className="font-medium text-gray-900">{account.accountName}</div>
                          <div className="text-xs text-gray-500">{account.industry}</div>
                        </td>
                        <td className="px-4 py-4">
                          <span className="text-sm text-gray-600">{account.segment}</span>
                          <div className="text-xs text-gray-400">{account.region}</div>
                        </td>
                        <td className="px-4 py-4">
                          <div className="text-sm text-gray-900">{account.territoryName}</div>
                          <div className="text-xs text-gray-500">{account.ownerName}</div>
                        </td>
                        <td className="px-4 py-4 text-center">
                          <div className="flex flex-col items-center">
                            <span className={`inline-flex px-2 py-1 text-xs font-semibold rounded-full ${getTierColor(account.propensityTier)}`}>
                              {account.propensityScore.toFixed(1)}
                            </span>
                            <div className="flex gap-1 mt-1 text-xs text-gray-400">
                              <span title="Firmographic">F:{account.firmographicScore}</span>
                              <span title="Behavioral">B:{account.behavioralScore}</span>
                              <span title="Engagement">E:{account.engagementScore}</span>
                            </div>
                          </div>
                        </td>
                        <td className="px-4 py-4 text-right">
                          <span className="text-sm font-medium text-gray-900">
                            {account.currentArr > 0 ? formatCurrency(account.currentArr) : '-'}
                          </span>
                        </td>
                        <td className="px-4 py-4 text-right">
                          <div className="text-sm font-medium text-emerald-600">{formatCurrency(account.expansionPotential)}</div>
                          <div className="text-xs text-gray-500">{account.topExpansionProduct}</div>
                        </td>
                        <td className="px-4 py-4">
                          <p className="text-sm text-gray-600 max-w-xs">{account.recommendedAction}</p>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Territory Balance Tab */}
          {activeTab === 'territories' && (
            <div className="p-6">
              <div className="mb-4">
                <h3 className="text-lg font-semibold text-gray-900">Territory Balance Analysis</h3>
                <p className="text-sm text-gray-500">Workload distribution and capacity optimization across territories</p>
              </div>
              <div className="grid gap-4">
                {territories.map((territory) => (
                  <div key={territory.territoryId} className="border border-gray-200 rounded-lg p-4 hover:shadow-md transition-shadow">
                    <div className="flex justify-between items-start mb-4">
                      <div>
                        <h4 className="font-semibold text-gray-900">{territory.territoryName}</h4>
                        <p className="text-sm text-gray-500">{territory.ownerName} • {territory.segment}</p>
                      </div>
                      <span className={`px-3 py-1 text-xs font-semibold rounded-full ${getBalanceColor(territory.balanceStatus)}`}>
                        {territory.balanceStatus}
                      </span>
                    </div>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4">
                      <div>
                        <p className="text-xs text-gray-500">Accounts</p>
                        <p className="text-lg font-semibold text-gray-900">{territory.accountCount}</p>
                        <p className="text-xs text-emerald-600">{territory.highPropensityCount} high propensity</p>
                      </div>
                      <div>
                        <p className="text-xs text-gray-500">Total ARR</p>
                        <p className="text-lg font-semibold text-gray-900">{formatCurrency(territory.totalArr)}</p>
                        <p className="text-xs text-gray-500">{formatCurrency(territory.totalPotential)} potential</p>
                      </div>
                      <div>
                        <p className="text-xs text-gray-500">Pipeline</p>
                        <p className="text-lg font-semibold text-gray-900">{formatCurrency(territory.pipelineValue)}</p>
                        <p className="text-xs text-gray-500">{formatCurrency(territory.quotaAmount)} quota</p>
                      </div>
                      <div>
                        <p className="text-xs text-gray-500">Workload Index</p>
                        <p className={`text-lg font-semibold ${
                          territory.workloadIndex > 1.2 ? 'text-red-600' :
                          territory.workloadIndex < 0.8 ? 'text-blue-600' : 'text-gray-900'
                        }`}>
                          {territory.workloadIndex.toFixed(2)}x
                        </p>
                        <p className="text-xs text-gray-500">{(territory.capacityScore * 100).toFixed(0)}% capacity</p>
                      </div>
                    </div>
                    {/* Workload bar */}
                    <div className="mb-3">
                      <div className="flex justify-between text-xs text-gray-500 mb-1">
                        <span>Workload Distribution</span>
                        <span>{(territory.workloadIndex * 100).toFixed(0)}% of target</span>
                      </div>
                      <div className="w-full bg-gray-200 rounded-full h-2">
                        <div
                          className={`h-2 rounded-full ${
                            territory.workloadIndex > 1.2 ? 'bg-red-500' :
                            territory.workloadIndex < 0.8 ? 'bg-blue-500' : 'bg-emerald-500'
                          }`}
                          style={{ width: `${Math.min(territory.workloadIndex * 100, 150)}%`, maxWidth: '100%' }}
                        ></div>
                      </div>
                    </div>
                    <div className="bg-gray-50 rounded p-3">
                      <p className="text-sm text-gray-600">
                        <span className="font-medium">Recommendation:</span> {territory.recommendedAction}
                      </p>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* White Space Tab */}
          {activeTab === 'whitespace' && (
            <div className="p-6">
              <div className="mb-4">
                <h3 className="text-lg font-semibold text-gray-900">White Space Opportunities</h3>
                <p className="text-sm text-gray-500">Net-new accounts with high ICP fit and expansion potential</p>
              </div>
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase w-12">#</th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Account</th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Segment</th>
                      <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">ICP Fit</th>
                      <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">Propensity</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Potential</th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Products</th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Approach</th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {whiteSpace.map((account) => (
                      <tr key={account.accountId} className="hover:bg-gray-50">
                        <td className="px-4 py-4 text-center">
                          <span className="inline-flex items-center justify-center w-6 h-6 rounded-full bg-blue-100 text-blue-800 text-xs font-semibold">
                            {account.priorityRank}
                          </span>
                        </td>
                        <td className="px-4 py-4">
                          <div className="font-medium text-gray-900">{account.accountName}</div>
                          <div className="text-xs text-gray-500">{account.industry}</div>
                          <div className="text-xs text-gray-400">
                            {account.employeeCount.toLocaleString()} employees • {formatCurrency(account.annualRevenue)} rev
                          </div>
                        </td>
                        <td className="px-4 py-4">
                          <span className="text-sm text-gray-600">{account.segment}</span>
                          <div className="text-xs text-gray-400">{account.region}</div>
                        </td>
                        <td className="px-4 py-4 text-center">
                          <div className="flex flex-col items-center">
                            <span className="text-lg font-bold text-gray-900">{account.icpFitScore}</span>
                            <span className="text-xs text-gray-500">ICP Score</span>
                          </div>
                        </td>
                        <td className="px-4 py-4 text-center">
                          <span className={`inline-flex px-2 py-1 text-xs font-semibold rounded-full ${getTierColor(account.propensityTier)}`}>
                            {account.propensityScore.toFixed(1)}
                          </span>
                        </td>
                        <td className="px-4 py-4 text-right">
                          <span className="text-sm font-bold text-emerald-600">{formatCurrency(account.estimatedPotential)}</span>
                        </td>
                        <td className="px-4 py-4">
                          <div className="flex flex-wrap gap-1">
                            {account.whitespaceProducts.map((product, idx) => (
                              <span key={idx} className="inline-flex px-2 py-0.5 text-xs bg-gray-100 text-gray-700 rounded">
                                {product}
                              </span>
                            ))}
                          </div>
                          {account.competitorPresent && (
                            <div className="mt-1 text-xs text-red-600">
                              Competitor: {account.competitorName}
                            </div>
                          )}
                        </td>
                        <td className="px-4 py-4">
                          <p className="text-sm text-gray-600 max-w-xs">{account.recommendedApproach}</p>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>

        {/* Insights Section */}
        {summary && (
          <div className="bg-white rounded-lg shadow p-6">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Territory Planning Insights</h3>
            <div className="grid md:grid-cols-3 gap-6">
              <div className="border-l-4 border-emerald-500 pl-4">
                <h4 className="font-medium text-gray-900">Top Expansion Opportunity</h4>
                <p className="text-sm text-gray-600 mt-1">
                  {summary.topExpansionSegment} segment in {summary.topExpansionIndustry} shows highest expansion potential
                  with {summary.highPropensityCount} high-propensity accounts representing {formatCurrency(summary.highPropensityRevenuePotential)} in opportunity.
                </p>
              </div>
              <div className="border-l-4 border-blue-500 pl-4">
                <h4 className="font-medium text-gray-900">White Space Focus</h4>
                <p className="text-sm text-gray-600 mt-1">
                  {summary.whiteSpaceAccounts} net-new accounts identified with {formatCurrency(summary.whiteSpaceRevenuePotential)} total addressable revenue.
                  Prioritize accounts with highest ICP fit and lowest competitive presence.
                </p>
              </div>
              <div className="border-l-4 border-amber-500 pl-4">
                <h4 className="font-medium text-gray-900">Territory Rebalancing</h4>
                <p className="text-sm text-gray-600 mt-1">
                  {summary.overloadedTerritories} overloaded and {summary.underloadedTerritories} underloaded territories need attention.
                  Consider redistributing accounts to achieve {(summary.avgTerritoryBalance * 100).toFixed(0)}%+ balance score.
                </p>
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  )
}
