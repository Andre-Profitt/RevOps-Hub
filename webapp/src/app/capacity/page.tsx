'use client'

import { useState, useEffect } from 'react'
import Link from 'next/link'
import {
  getCapacityPlanningSummary,
  getRepCapacity,
  getTeamCapacitySummary,
  getRampCohortAnalysis,
} from '@/lib/foundry'
import type { CapacityPlanningSummary, RepCapacity, TeamCapacitySummary, RampCohortAnalysis } from '@/types'

// Format currency
function formatCurrency(amount: number): string {
  if (amount >= 1000000) {
    return `$${(amount / 1000000).toFixed(1)}M`
  }
  if (amount >= 1000) {
    return `$${(amount / 1000).toFixed(0)}K`
  }
  return `$${amount.toFixed(0)}`
}

// Get status color
function getCapacityStatusColor(status: string): string {
  switch (status) {
    case 'Over Capacity':
      return 'bg-red-100 text-red-800'
    case 'At Capacity':
      return 'bg-amber-100 text-amber-800'
    case 'Healthy':
      return 'bg-emerald-100 text-emerald-800'
    case 'Under Utilized':
      return 'bg-blue-100 text-blue-800'
    default:
      return 'bg-gray-100 text-gray-800'
  }
}

function getRampStatusColor(status: string): string {
  switch (status) {
    case 'Fully Ramped':
      return 'bg-emerald-100 text-emerald-800'
    case 'Developing':
      return 'bg-amber-100 text-amber-800'
    case 'Ramping':
      return 'bg-blue-100 text-blue-800'
    default:
      return 'bg-gray-100 text-gray-800'
  }
}

function getHealthColor(health: string): string {
  switch (health) {
    case 'Strong':
      return 'bg-emerald-100 text-emerald-800'
    case 'Healthy':
      return 'bg-green-100 text-green-800'
    case 'Strained':
      return 'bg-amber-100 text-amber-800'
    case 'Under Utilized':
      return 'bg-blue-100 text-blue-800'
    default:
      return 'bg-gray-100 text-gray-800'
  }
}

function getUrgencyColor(urgency: string): string {
  switch (urgency) {
    case 'Critical':
      return 'text-red-600'
    case 'High':
      return 'text-amber-600'
    case 'Medium':
      return 'text-yellow-600'
    case 'Low':
      return 'text-green-600'
    default:
      return 'text-gray-600'
  }
}

export default function CapacityPage() {
  const [summary, setSummary] = useState<CapacityPlanningSummary | null>(null)
  const [reps, setReps] = useState<RepCapacity[]>([])
  const [teams, setTeams] = useState<TeamCapacitySummary[]>([])
  const [cohorts, setCohorts] = useState<RampCohortAnalysis[]>([])
  const [loading, setLoading] = useState(true)
  const [activeTab, setActiveTab] = useState<'reps' | 'teams' | 'ramp'>('reps')

  useEffect(() => {
    async function loadData() {
      try {
        const [summaryData, repsData, teamsData, cohortsData] = await Promise.all([
          getCapacityPlanningSummary(),
          getRepCapacity(),
          getTeamCapacitySummary(),
          getRampCohortAnalysis(),
        ])
        setSummary(summaryData)
        setReps(repsData)
        setTeams(teamsData)
        setCohorts(cohortsData)
      } catch (error) {
        console.error('Error loading capacity data:', error)
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
          <p className="mt-4 text-gray-600">Loading capacity data...</p>
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
                <h1 className="text-2xl font-bold text-gray-900">Capacity & Hiring Planning</h1>
                <p className="text-sm text-gray-500">Sales capacity modeling, ramp tracking, and hiring impact analysis</p>
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
              <p className="text-xs text-gray-500 uppercase tracking-wide">Total Reps</p>
              <p className="text-2xl font-bold text-gray-900">{summary.totalReps}</p>
              <div className="flex gap-1 mt-1 text-xs">
                <span className="text-emerald-600">{summary.rampedReps} ramped</span>
                <span className="text-gray-400">|</span>
                <span className="text-amber-600">{summary.rampingReps + summary.developingReps} ramping</span>
              </div>
            </div>
            <div className="bg-white rounded-lg shadow p-4">
              <p className="text-xs text-gray-500 uppercase tracking-wide">Effective Quota</p>
              <p className="text-2xl font-bold text-gray-900">{formatCurrency(summary.effectiveQuota)}</p>
              <p className="text-xs text-red-500">-{(summary.rampCapacityLossPct * 100).toFixed(0)}% from ramp</p>
            </div>
            <div className="bg-white rounded-lg shadow p-4">
              <p className="text-xs text-gray-500 uppercase tracking-wide">Avg Utilization</p>
              <p className={`text-2xl font-bold ${
                summary.avgUtilization > 1 ? 'text-red-600' :
                summary.avgUtilization > 0.85 ? 'text-amber-600' : 'text-emerald-600'
              }`}>
                {(summary.avgUtilization * 100).toFixed(0)}%
              </p>
              <p className="text-xs text-gray-500">{summary.overCapacityCount} over capacity</p>
            </div>
            <div className="bg-white rounded-lg shadow p-4">
              <p className="text-xs text-gray-500 uppercase tracking-wide">Team Attainment</p>
              <p className="text-2xl font-bold text-gray-900">{(summary.teamAttainment * 100).toFixed(0)}%</p>
              <p className="text-xs text-gray-500">{formatCurrency(summary.totalClosedWon)} closed</p>
            </div>
            <div className="bg-white rounded-lg shadow p-4">
              <p className="text-xs text-gray-500 uppercase tracking-wide">Hiring Need</p>
              <p className={`text-2xl font-bold ${getUrgencyColor(summary.hiringUrgency)}`}>
                {summary.totalRecommendedHires}
              </p>
              <p className="text-xs text-gray-500">{summary.hiringUrgency} urgency</p>
            </div>
            <div className="bg-white rounded-lg shadow p-4">
              <p className="text-xs text-gray-500 uppercase tracking-wide">Capacity Health</p>
              <span className={`inline-flex px-2 py-1 text-sm font-semibold rounded-full ${getHealthColor(summary.capacityHealth)}`}>
                {summary.capacityHealth}
              </span>
            </div>
          </div>
        )}

        {/* Hiring Impact Card */}
        {summary && summary.totalRecommendedHires > 0 && (
          <div className="bg-gradient-to-r from-blue-50 to-indigo-50 rounded-lg shadow p-6 mb-8 border border-blue-200">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Hiring Impact Projection</h3>
            <div className="grid md:grid-cols-4 gap-6">
              <div>
                <p className="text-sm text-gray-600">Recommended Hires</p>
                <p className="text-3xl font-bold text-blue-600">{summary.totalRecommendedHires}</p>
              </div>
              <div>
                <p className="text-sm text-gray-600">Investment Required</p>
                <p className="text-3xl font-bold text-gray-900">{formatCurrency(summary.totalHiringInvestment)}</p>
                <p className="text-xs text-gray-500">at $150K/rep annual cost</p>
              </div>
              <div>
                <p className="text-sm text-gray-600">Projected Y1 Revenue</p>
                <p className="text-3xl font-bold text-emerald-600">{formatCurrency(summary.totalProjectedRevenue)}</p>
                <p className="text-xs text-gray-500">at 50% first-year productivity</p>
              </div>
              <div>
                <p className="text-sm text-gray-600">Capacity Gap Filled</p>
                <p className="text-3xl font-bold text-gray-900">{formatCurrency(summary.totalCapacityGap)}</p>
                <p className="text-xs text-gray-500">in pipeline capacity</p>
              </div>
            </div>
          </div>
        )}

        {/* Tab Navigation */}
        <div className="bg-white rounded-lg shadow mb-6">
          <div className="border-b border-gray-200">
            <nav className="flex -mb-px">
              <button
                onClick={() => setActiveTab('reps')}
                className={`px-6 py-4 text-sm font-medium border-b-2 ${
                  activeTab === 'reps'
                    ? 'border-blue-500 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }`}
              >
                Rep Capacity ({reps.length})
              </button>
              <button
                onClick={() => setActiveTab('teams')}
                className={`px-6 py-4 text-sm font-medium border-b-2 ${
                  activeTab === 'teams'
                    ? 'border-blue-500 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }`}
              >
                Team Summary ({teams.length})
              </button>
              <button
                onClick={() => setActiveTab('ramp')}
                className={`px-6 py-4 text-sm font-medium border-b-2 ${
                  activeTab === 'ramp'
                    ? 'border-blue-500 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }`}
              >
                Ramp Analysis ({cohorts.length})
              </button>
            </nav>
          </div>

          {/* Rep Capacity Tab */}
          {activeTab === 'reps' && (
            <div className="p-6">
              <div className="mb-4">
                <h3 className="text-lg font-semibold text-gray-900">Individual Rep Capacity</h3>
                <p className="text-sm text-gray-500">Workload, utilization, and productivity by rep</p>
              </div>
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Rep</th>
                      <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">Ramp Status</th>
                      <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">Utilization</th>
                      <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">Attainment</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Quota</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Pipeline</th>
                      <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">Accounts</th>
                      <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">Productivity</th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {reps.map((rep) => (
                      <tr key={rep.repId} className="hover:bg-gray-50">
                        <td className="px-4 py-4">
                          <div className="font-medium text-gray-900">{rep.repName}</div>
                          <div className="text-xs text-gray-500">{rep.region} • {rep.segment}</div>
                        </td>
                        <td className="px-4 py-4 text-center">
                          <span className={`inline-flex px-2 py-1 text-xs font-semibold rounded-full ${getRampStatusColor(rep.rampStatus)}`}>
                            {rep.rampStatus}
                          </span>
                          <div className="text-xs text-gray-400 mt-1">{rep.daysSinceHire}d tenure</div>
                        </td>
                        <td className="px-4 py-4 text-center">
                          <div className="flex flex-col items-center">
                            <span className={`inline-flex px-2 py-1 text-xs font-semibold rounded-full ${getCapacityStatusColor(rep.capacityStatus)}`}>
                              {(rep.capacityUtilization * 100).toFixed(0)}%
                            </span>
                            <div className="w-16 bg-gray-200 rounded-full h-1.5 mt-1">
                              <div
                                className={`h-1.5 rounded-full ${
                                  rep.capacityUtilization > 1 ? 'bg-red-500' :
                                  rep.capacityUtilization > 0.85 ? 'bg-amber-500' : 'bg-emerald-500'
                                }`}
                                style={{ width: `${Math.min(rep.capacityUtilization * 100, 100)}%` }}
                              ></div>
                            </div>
                          </div>
                        </td>
                        <td className="px-4 py-4 text-center">
                          <span className={`text-lg font-bold ${
                            rep.quotaAttainment >= 1 ? 'text-emerald-600' :
                            rep.quotaAttainment >= 0.8 ? 'text-amber-600' : 'text-red-600'
                          }`}>
                            {(rep.quotaAttainment * 100).toFixed(0)}%
                          </span>
                        </td>
                        <td className="px-4 py-4 text-right">
                          <div className="text-sm text-gray-900">{formatCurrency(rep.effectiveQuota)}</div>
                          {rep.rampFactor < 1 && (
                            <div className="text-xs text-gray-400">({(rep.rampFactor * 100).toFixed(0)}% of full)</div>
                          )}
                        </td>
                        <td className="px-4 py-4 text-right">
                          <div className="text-sm font-medium text-gray-900">{formatCurrency(rep.openPipeline)}</div>
                          <div className="text-xs text-gray-500">{rep.coverageRatio.toFixed(1)}x coverage</div>
                        </td>
                        <td className="px-4 py-4 text-center">
                          <span className="text-sm font-medium text-gray-900">{rep.activeAccounts}</span>
                        </td>
                        <td className="px-4 py-4 text-center">
                          <div className="flex items-center justify-center">
                            <div className="w-12 h-12 relative">
                              <svg className="w-12 h-12 transform -rotate-90">
                                <circle
                                  cx="24"
                                  cy="24"
                                  r="20"
                                  fill="none"
                                  stroke="#e5e7eb"
                                  strokeWidth="4"
                                />
                                <circle
                                  cx="24"
                                  cy="24"
                                  r="20"
                                  fill="none"
                                  stroke={rep.productivityIndex >= 0.8 ? '#10b981' : rep.productivityIndex >= 0.5 ? '#f59e0b' : '#ef4444'}
                                  strokeWidth="4"
                                  strokeDasharray={`${rep.productivityIndex * 125.6} 125.6`}
                                />
                              </svg>
                              <span className="absolute inset-0 flex items-center justify-center text-xs font-bold">
                                {(rep.productivityIndex * 100).toFixed(0)}
                              </span>
                            </div>
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Team Summary Tab */}
          {activeTab === 'teams' && (
            <div className="p-6">
              <div className="mb-4">
                <h3 className="text-lg font-semibold text-gray-900">Team Capacity Summary</h3>
                <p className="text-sm text-gray-500">Aggregated capacity and hiring needs by region</p>
              </div>
              <div className="grid gap-4">
                {teams.map((team, idx) => (
                  <div key={idx} className="border border-gray-200 rounded-lg p-4 hover:shadow-md transition-shadow">
                    <div className="flex justify-between items-start mb-4">
                      <div>
                        <h4 className="font-semibold text-gray-900">{team.region} {team.segment}</h4>
                        <p className="text-sm text-gray-500">{team.totalReps} reps • {team.rampedReps} ramped</p>
                      </div>
                      <div className="flex items-center gap-2">
                        <span className={`px-3 py-1 text-xs font-semibold rounded-full ${getHealthColor(team.capacityHealth)}`}>
                          {team.capacityHealth}
                        </span>
                        {team.recommendedHires > 0 && (
                          <span className="px-3 py-1 text-xs font-semibold rounded-full bg-blue-100 text-blue-800">
                            +{team.recommendedHires} recommended
                          </span>
                        )}
                      </div>
                    </div>
                    <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
                      <div>
                        <p className="text-xs text-gray-500">Total Quota</p>
                        <p className="text-lg font-semibold text-gray-900">{formatCurrency(team.totalQuota)}</p>
                        {team.rampCapacityLoss > 0 && (
                          <p className="text-xs text-red-500">-{formatCurrency(team.rampCapacityLoss)} ramp loss</p>
                        )}
                      </div>
                      <div>
                        <p className="text-xs text-gray-500">Attainment</p>
                        <p className={`text-lg font-semibold ${
                          team.teamAttainment >= 1 ? 'text-emerald-600' :
                          team.teamAttainment >= 0.8 ? 'text-amber-600' : 'text-red-600'
                        }`}>
                          {(team.teamAttainment * 100).toFixed(0)}%
                        </p>
                        <p className="text-xs text-gray-500">{formatCurrency(team.totalClosedWon)} closed</p>
                      </div>
                      <div>
                        <p className="text-xs text-gray-500">Avg Utilization</p>
                        <p className={`text-lg font-semibold ${
                          team.avgCapacityUtilization > 1 ? 'text-red-600' :
                          team.avgCapacityUtilization > 0.85 ? 'text-amber-600' : 'text-emerald-600'
                        }`}>
                          {(team.avgCapacityUtilization * 100).toFixed(0)}%
                        </p>
                        <p className="text-xs text-gray-500">{team.overCapacityCount} over capacity</p>
                      </div>
                      <div>
                        <p className="text-xs text-gray-500">Coverage Ratio</p>
                        <p className="text-lg font-semibold text-gray-900">{team.teamCoverageRatio.toFixed(1)}x</p>
                        <p className="text-xs text-gray-500">{formatCurrency(team.totalPipeline)} pipeline</p>
                      </div>
                      <div>
                        <p className="text-xs text-gray-500">Avg Accounts/Rep</p>
                        <p className="text-lg font-semibold text-gray-900">{team.avgAccountsPerRep}</p>
                        <p className="text-xs text-gray-500">Productivity: {(team.avgProductivity * 100).toFixed(0)}%</p>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Ramp Analysis Tab */}
          {activeTab === 'ramp' && (
            <div className="p-6">
              <div className="mb-4">
                <h3 className="text-lg font-semibold text-gray-900">Ramp Cohort Analysis</h3>
                <p className="text-sm text-gray-500">Performance by tenure cohort vs expectations</p>
              </div>
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Cohort</th>
                      <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">Reps</th>
                      <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">Avg Attainment</th>
                      <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">Expected</th>
                      <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">vs Expected</th>
                      <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">Productivity</th>
                      <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">Coverage</th>
                      <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">Days to Close</th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {cohorts.filter(c => c.repCount > 0).map((cohort) => (
                      <tr key={cohort.rampCohort} className="hover:bg-gray-50">
                        <td className="px-4 py-4">
                          <span className="font-medium text-gray-900">{cohort.rampCohort}</span>
                        </td>
                        <td className="px-4 py-4 text-center">
                          <span className="text-sm text-gray-900">{cohort.repCount}</span>
                        </td>
                        <td className="px-4 py-4 text-center">
                          <span className={`text-lg font-bold ${
                            cohort.avgAttainment >= cohort.expectedAttainment ? 'text-emerald-600' : 'text-amber-600'
                          }`}>
                            {(cohort.avgAttainment * 100).toFixed(0)}%
                          </span>
                        </td>
                        <td className="px-4 py-4 text-center">
                          <span className="text-sm text-gray-500">{(cohort.expectedAttainment * 100).toFixed(0)}%</span>
                        </td>
                        <td className="px-4 py-4 text-center">
                          <span className={`inline-flex px-2 py-1 text-xs font-semibold rounded-full ${
                            cohort.performanceVsExpected >= 0 ? 'bg-emerald-100 text-emerald-800' : 'bg-red-100 text-red-800'
                          }`}>
                            {cohort.performanceVsExpected >= 0 ? '+' : ''}{(cohort.performanceVsExpected * 100).toFixed(0)}%
                          </span>
                        </td>
                        <td className="px-4 py-4 text-center">
                          <span className="text-sm text-gray-900">{(cohort.avgProductivity * 100).toFixed(0)}%</span>
                        </td>
                        <td className="px-4 py-4 text-center">
                          <span className="text-sm text-gray-900">{cohort.avgCoverage.toFixed(1)}x</span>
                        </td>
                        <td className="px-4 py-4 text-center">
                          <span className="text-sm text-gray-900">
                            {cohort.avgDaysToFirstClose ? `${cohort.avgDaysToFirstClose}d` : '-'}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {/* Ramp Curve Visualization */}
              <div className="mt-8 bg-gray-50 rounded-lg p-6">
                <h4 className="font-medium text-gray-900 mb-4">Ramp Productivity Curve</h4>
                <div className="flex items-end gap-2 h-48">
                  {cohorts.filter(c => c.repCount > 0 || c.expectedAttainment > 0).map((cohort) => (
                    <div key={cohort.rampCohort} className="flex-1 flex flex-col items-center">
                      <div className="w-full flex gap-1 justify-center" style={{ height: `${Math.max(cohort.avgAttainment, cohort.expectedAttainment) * 180}px` }}>
                        <div
                          className="w-6 bg-blue-500 rounded-t"
                          style={{ height: `${(cohort.avgAttainment / Math.max(cohort.avgAttainment, cohort.expectedAttainment, 0.01)) * 100}%` }}
                          title={`Actual: ${(cohort.avgAttainment * 100).toFixed(0)}%`}
                        ></div>
                        <div
                          className="w-6 bg-gray-300 rounded-t"
                          style={{ height: `${(cohort.expectedAttainment / Math.max(cohort.avgAttainment, cohort.expectedAttainment, 0.01)) * 100}%` }}
                          title={`Expected: ${(cohort.expectedAttainment * 100).toFixed(0)}%`}
                        ></div>
                      </div>
                      <p className="text-xs text-gray-500 mt-2 text-center">{cohort.rampCohort.split(' ')[0]}</p>
                    </div>
                  ))}
                </div>
                <div className="flex justify-center gap-6 mt-4">
                  <div className="flex items-center gap-2">
                    <div className="w-4 h-4 bg-blue-500 rounded"></div>
                    <span className="text-sm text-gray-600">Actual</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <div className="w-4 h-4 bg-gray-300 rounded"></div>
                    <span className="text-sm text-gray-600">Expected</span>
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Insights Section */}
        {summary && (
          <div className="bg-white rounded-lg shadow p-6">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Capacity Planning Insights</h3>
            <div className="grid md:grid-cols-3 gap-6">
              <div className="border-l-4 border-emerald-500 pl-4">
                <h4 className="font-medium text-gray-900">Productivity Status</h4>
                <p className="text-sm text-gray-600 mt-1">
                  Average team productivity is at {(summary.avgProductivity * 100).toFixed(0)}%.
                  {summary.avgProductivity >= 0.8 ? ' Team is performing well above targets.' :
                   summary.avgProductivity >= 0.6 ? ' Team is tracking close to expectations.' :
                   ' Consider coaching interventions for underperforming reps.'}
                </p>
              </div>
              <div className="border-l-4 border-blue-500 pl-4">
                <h4 className="font-medium text-gray-900">Ramp Impact</h4>
                <p className="text-sm text-gray-600 mt-1">
                  {summary.rampingReps + summary.developingReps} reps are still ramping, reducing effective quota capacity
                  by {(summary.rampCapacityLossPct * 100).toFixed(0)}% ({formatCurrency(summary.totalQuota - summary.effectiveQuota)}).
                  Plan for full productivity in 3-6 months.
                </p>
              </div>
              <div className="border-l-4 border-amber-500 pl-4">
                <h4 className="font-medium text-gray-900">Hiring Recommendation</h4>
                <p className="text-sm text-gray-600 mt-1">
                  {summary.totalRecommendedHires > 0 ?
                    `Recommend hiring ${summary.totalRecommendedHires} additional reps to address ${formatCurrency(summary.totalCapacityGap)} capacity gap. Expected Y1 ROI: ${((summary.totalProjectedRevenue / summary.totalHiringInvestment) * 100).toFixed(0)}%.` :
                    'Current headcount is sufficient for pipeline coverage. Focus on improving existing rep productivity.'}
                </p>
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  )
}
