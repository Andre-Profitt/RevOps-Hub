'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import {
  getCompPlanSummary,
  getRepAttainment,
  getAttainmentTrend,
} from '@/lib/foundry'
import type {
  CompPlanSummary,
  RepAttainment,
  AttainmentTrend,
} from '@/types'

function formatCurrency(value: number): string {
  if (value >= 1000000) return `$${(value / 1000000).toFixed(1)}M`
  if (value >= 1000) return `$${(value / 1000).toFixed(0)}K`
  return `$${value.toFixed(0)}`
}

function formatPercent(value: number): string {
  return `${value.toFixed(1)}%`
}

export default function CompensationPage() {
  const [summary, setSummary] = useState<CompPlanSummary | null>(null)
  const [reps, setReps] = useState<RepAttainment[]>([])
  const [trend, setTrend] = useState<AttainmentTrend[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    Promise.all([
      getCompPlanSummary(),
      getRepAttainment(),
      getAttainmentTrend(),
    ]).then(([summaryData, repsData, trendData]) => {
      setSummary(summaryData)
      setReps(repsData)
      setTrend(trendData)
      setLoading(false)
    })
  }, [])

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 p-8">
        <div className="animate-pulse">Loading compensation data...</div>
      </div>
    )
  }

  const onTrack = reps.filter(r => r.attainmentTier === 'on_track' || r.attainmentTier === 'overachiever')
  const atRisk = reps.filter(r => r.attainmentTier === 'at_risk' || r.attainmentTier === 'below')

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
              <h1 className="text-2xl font-bold text-gray-900">Compensation & Attainment</h1>
              <p className="text-sm text-gray-500">Quota tracking and commission insights</p>
            </div>
          </div>
        </div>
      </header>

      <main className="p-6 space-y-6">
        {/* KPI Cards */}
        <div className="grid grid-cols-5 gap-4">
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Team Quota</p>
            <p className="text-2xl font-bold text-gray-900">{formatCurrency(summary?.totalQuota || 0)}</p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Closed Revenue</p>
            <p className="text-2xl font-bold text-green-600">{formatCurrency(summary?.totalClosed || 0)}</p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Team Attainment</p>
            <p className="text-2xl font-bold text-gray-900">{formatPercent(summary?.teamAttainment || 0)}</p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Commission Paid</p>
            <p className="text-2xl font-bold text-gray-900">{formatCurrency(summary?.totalCommissionPaid || 0)}</p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Projected Commission</p>
            <p className="text-2xl font-bold text-blue-600">{formatCurrency(summary?.projectedCommission || 0)}</p>
          </div>
        </div>

        {/* Summary Stats */}
        <div className="grid grid-cols-4 gap-4">
          <div className="bg-green-50 rounded-lg p-4 border border-green-200">
            <p className="text-sm text-green-700">Overachievers (100%+)</p>
            <p className="text-3xl font-bold text-green-600">{summary?.overachieversCount}</p>
          </div>
          <div className="bg-blue-50 rounded-lg p-4 border border-blue-200">
            <p className="text-sm text-blue-700">On Track</p>
            <p className="text-3xl font-bold text-blue-600">{summary?.onTrackCount}</p>
          </div>
          <div className="bg-yellow-50 rounded-lg p-4 border border-yellow-200">
            <p className="text-sm text-yellow-700">At Risk</p>
            <p className="text-3xl font-bold text-yellow-600">{summary?.atRiskCount}</p>
          </div>
          <div className="bg-purple-50 rounded-lg p-4 border border-purple-200">
            <p className="text-sm text-purple-700">Accelerator Eligible</p>
            <p className="text-3xl font-bold text-purple-600">{summary?.acceleratorEligible}</p>
          </div>
        </div>

        {/* Attainment Table */}
        <div className="bg-white rounded-lg border border-gray-200">
          <div className="p-4 border-b border-gray-200">
            <h2 className="font-semibold text-gray-900">Rep Attainment Leaderboard</h2>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-gray-50">
                <tr className="text-left text-gray-500">
                  <th className="px-4 py-3 w-12">#</th>
                  <th className="px-4 py-3">Rep</th>
                  <th className="px-4 py-3">Region</th>
                  <th className="px-4 py-3 text-right">Quota</th>
                  <th className="px-4 py-3 text-right">Closed</th>
                  <th className="px-4 py-3 text-right">Attainment</th>
                  <th className="px-4 py-3 text-right">Gap</th>
                  <th className="px-4 py-3 text-right">Commission</th>
                  <th className="px-4 py-3 text-right">Accelerator</th>
                  <th className="px-4 py-3 text-center">Trend</th>
                  <th className="px-4 py-3">Status</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {reps.map((rep, idx) => (
                  <tr key={idx} className="hover:bg-gray-50">
                    <td className="px-4 py-3 text-gray-500">{rep.rank}</td>
                    <td className="px-4 py-3 font-medium text-gray-900">{rep.repName}</td>
                    <td className="px-4 py-3 text-gray-600">{rep.region}</td>
                    <td className="px-4 py-3 text-right">{formatCurrency(rep.quotaAmount)}</td>
                    <td className="px-4 py-3 text-right font-medium">{formatCurrency(rep.closedWon)}</td>
                    <td className="px-4 py-3 text-right">
                      <div className="flex items-center justify-end gap-2">
                        <div className="w-16 bg-gray-100 rounded-full h-2">
                          <div
                            className={`h-2 rounded-full ${
                              rep.attainmentPct >= 100 ? 'bg-green-500' :
                              rep.attainmentPct >= 80 ? 'bg-blue-500' :
                              rep.attainmentPct >= 60 ? 'bg-yellow-500' :
                              'bg-red-500'
                            }`}
                            style={{ width: `${Math.min(rep.attainmentPct, 100)}%` }}
                          />
                        </div>
                        <span className={`font-medium ${
                          rep.attainmentPct >= 100 ? 'text-green-600' :
                          rep.attainmentPct >= 80 ? 'text-blue-600' :
                          'text-gray-900'
                        }`}>
                          {formatPercent(rep.attainmentPct)}
                        </span>
                      </div>
                    </td>
                    <td className={`px-4 py-3 text-right ${rep.gapToQuota > 0 ? 'text-red-600' : 'text-green-600'}`}>
                      {rep.gapToQuota > 0 ? '-' : '+'}{formatCurrency(Math.abs(rep.gapToQuota))}
                    </td>
                    <td className="px-4 py-3 text-right">{formatCurrency(rep.commissionEarned)}</td>
                    <td className="px-4 py-3 text-right">
                      {rep.acceleratorRate > 1 ? (
                        <span className="text-purple-600 font-medium">{rep.acceleratorRate}x</span>
                      ) : (
                        <span className="text-gray-400">-</span>
                      )}
                    </td>
                    <td className="px-4 py-3 text-center">
                      {rep.trend === 'up' ? (
                        <span className="text-green-500">↑</span>
                      ) : rep.trend === 'down' ? (
                        <span className="text-red-500">↓</span>
                      ) : (
                        <span className="text-gray-400">→</span>
                      )}
                    </td>
                    <td className="px-4 py-3">
                      <span className={`px-2 py-0.5 rounded text-xs ${
                        rep.attainmentTier === 'overachiever' ? 'bg-green-100 text-green-700' :
                        rep.attainmentTier === 'on_track' ? 'bg-blue-100 text-blue-700' :
                        rep.attainmentTier === 'at_risk' ? 'bg-yellow-100 text-yellow-700' :
                        'bg-red-100 text-red-700'
                      }`}>
                        {rep.attainmentTier === 'overachiever' ? 'Overachiever' :
                         rep.attainmentTier === 'on_track' ? 'On Track' :
                         rep.attainmentTier === 'at_risk' ? 'At Risk' : 'Below'}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Attainment Trend Chart */}
        <div className="bg-white rounded-lg border border-gray-200">
          <div className="p-4 border-b border-gray-200">
            <h2 className="font-semibold text-gray-900">Attainment Trend</h2>
          </div>
          <div className="p-4">
            <div className="grid grid-cols-12 gap-2">
              {trend.map((month, idx) => (
                <div key={idx} className="text-center">
                  <div className="text-xs text-gray-500 mb-2">{month.month}</div>
                  <div className="relative h-32 bg-gray-100 rounded">
                    <div
                      className={`absolute bottom-0 left-0 right-0 rounded-b ${
                        month.attainment >= 100 ? 'bg-green-500' :
                        month.attainment >= 80 ? 'bg-blue-500' :
                        month.attainment >= 60 ? 'bg-yellow-500' :
                        'bg-red-500'
                      }`}
                      style={{ height: `${Math.min(month.attainment, 100)}%` }}
                    />
                    <div
                      className="absolute bottom-0 left-1/2 w-0.5 bg-gray-400"
                      style={{ height: `${month.teamAvg}%`, transform: 'translateX(-50%)' }}
                    />
                  </div>
                  <div className="text-xs text-gray-600 mt-1">{formatPercent(month.attainment)}</div>
                </div>
              ))}
            </div>
            <div className="flex items-center justify-center gap-6 mt-4">
              <div className="flex items-center gap-2">
                <div className="w-3 h-3 bg-blue-500 rounded" />
                <span className="text-xs text-gray-600">Your Attainment</span>
              </div>
              <div className="flex items-center gap-2">
                <div className="w-3 h-3 bg-gray-400 rounded" />
                <span className="text-xs text-gray-600">Team Average</span>
              </div>
            </div>
          </div>
        </div>
      </main>
    </div>
  )
}
