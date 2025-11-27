'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import {
  getWinLossSummary,
  getLossReasons,
  getWinFactors,
  getCompetitiveBattles,
  getWinLossBySegment,
} from '@/lib/foundry'
import type {
  WinLossSummary,
  LossReason,
  WinFactor,
  CompetitiveBattle,
  WinLossBySegment,
} from '@/types'

function formatCurrency(value: number): string {
  if (value >= 1000000) return `$${(value / 1000000).toFixed(1)}M`
  if (value >= 1000) return `$${(value / 1000).toFixed(0)}K`
  return `$${value.toFixed(0)}`
}

function formatPercent(value: number): string {
  return `${value.toFixed(1)}%`
}

export default function WinLossAnalysisPage() {
  const [summary, setSummary] = useState<WinLossSummary | null>(null)
  const [lossReasons, setLossReasons] = useState<LossReason[]>([])
  const [winFactors, setWinFactors] = useState<WinFactor[]>([])
  const [battles, setBattles] = useState<CompetitiveBattle[]>([])
  const [bySegment, setBySegment] = useState<WinLossBySegment[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    Promise.all([
      getWinLossSummary(),
      getLossReasons(),
      getWinFactors(),
      getCompetitiveBattles(),
      getWinLossBySegment(),
    ]).then(([summaryData, lossData, factorsData, battlesData, segmentData]) => {
      setSummary(summaryData)
      setLossReasons(lossData)
      setWinFactors(factorsData)
      setBattles(battlesData)
      setBySegment(segmentData)
      setLoading(false)
    })
  }, [])

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 p-8">
        <div className="animate-pulse">Loading win/loss analysis...</div>
      </div>
    )
  }

  const segmentData = bySegment.filter(s => s.dimension === 'segment')
  const sizeData = bySegment.filter(s => s.dimension === 'size_band')

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
              <h1 className="text-2xl font-bold text-gray-900">Win/Loss Analysis</h1>
              <p className="text-sm text-gray-500">Competitive intelligence and pattern analysis</p>
            </div>
          </div>
        </div>
      </header>

      <main className="p-6 space-y-6">
        {/* KPI Cards */}
        <div className="grid grid-cols-6 gap-4">
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Win Rate</p>
            <p className="text-2xl font-bold text-green-600">{formatPercent(summary?.winRate || 0)}</p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Total Won</p>
            <p className="text-2xl font-bold text-gray-900">{summary?.totalWon}</p>
            <p className="text-xs text-gray-400">{formatCurrency(summary?.wonRevenue || 0)}</p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Total Lost</p>
            <p className="text-2xl font-bold text-gray-900">{summary?.totalLost}</p>
            <p className="text-xs text-gray-400">{formatCurrency(summary?.lostRevenue || 0)}</p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Avg Won Deal</p>
            <p className="text-2xl font-bold text-gray-900">{formatCurrency(summary?.avgWonDealSize || 0)}</p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Avg Won Cycle</p>
            <p className="text-2xl font-bold text-gray-900">{summary?.avgWonCycle?.toFixed(0)} days</p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Deal Size Gap</p>
            <p className={`text-2xl font-bold ${(summary?.dealSizeGap || 0) > 0 ? 'text-green-600' : 'text-red-600'}`}>
              {formatCurrency(summary?.dealSizeGap || 0)}
            </p>
          </div>
        </div>

        <div className="grid grid-cols-2 gap-6">
          {/* Win Factors */}
          <div className="bg-white rounded-lg border border-gray-200">
            <div className="p-4 border-b border-gray-200">
              <h2 className="font-semibold text-gray-900">Win Factors Analysis</h2>
              <p className="text-sm text-gray-500">What differentiates wins from losses</p>
            </div>
            <div className="p-4 space-y-4">
              {winFactors.map((factor, idx) => (
                <div key={idx} className="p-3 bg-gray-50 rounded-lg">
                  <div className="flex items-center justify-between mb-2">
                    <span className="font-medium text-gray-900">{factor.factor}</span>
                    <span className={`px-2 py-0.5 rounded text-xs ${
                      factor.impact === 'positive' ? 'bg-green-100 text-green-700' :
                      factor.impact === 'negative' ? 'bg-red-100 text-red-700' :
                      'bg-gray-100 text-gray-700'
                    }`}>
                      {factor.impact}
                    </span>
                  </div>
                  <div className="grid grid-cols-3 gap-4 text-sm">
                    <div>
                      <span className="text-gray-500">Won Avg:</span>
                      <span className="ml-2 font-medium text-green-600">{factor.wonAvg.toFixed(1)}</span>
                    </div>
                    <div>
                      <span className="text-gray-500">Lost Avg:</span>
                      <span className="ml-2 font-medium text-red-600">{factor.lostAvg.toFixed(1)}</span>
                    </div>
                    <div>
                      <span className="text-gray-500">Delta:</span>
                      <span className={`ml-2 font-medium ${factor.difference > 0 ? 'text-green-600' : 'text-red-600'}`}>
                        {factor.difference > 0 ? '+' : ''}{factor.difference.toFixed(1)}
                      </span>
                    </div>
                  </div>
                  <p className="text-xs text-gray-500 mt-2">{factor.recommendation}</p>
                </div>
              ))}
            </div>
          </div>

          {/* Competitive Battles */}
          <div className="bg-white rounded-lg border border-gray-200">
            <div className="p-4 border-b border-gray-200">
              <h2 className="font-semibold text-gray-900">Head-to-Head Battles</h2>
              <p className="text-sm text-gray-500">Win rate against each competitor</p>
            </div>
            <div className="p-4">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-gray-500">
                    <th className="pb-2">Competitor</th>
                    <th className="pb-2 text-right">Battles</th>
                    <th className="pb-2 text-right">Win Rate</th>
                    <th className="pb-2 text-right">Net Revenue</th>
                    <th className="pb-2 text-right">Position</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {battles.map((battle, idx) => (
                    <tr key={idx}>
                      <td className="py-2 font-medium text-gray-900">{battle.primaryCompetitor}</td>
                      <td className="py-2 text-right">{battle.totalBattles}</td>
                      <td className="py-2 text-right">
                        <span className={battle.winRate >= 50 ? 'text-green-600' : 'text-red-600'}>
                          {formatPercent(battle.winRate)}
                        </span>
                      </td>
                      <td className={`py-2 text-right ${battle.netRevenue >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                        {formatCurrency(battle.netRevenue)}
                      </td>
                      <td className="py-2 text-right">
                        <span className={`px-2 py-0.5 rounded text-xs ${
                          battle.competitivePosition === 'Strong' ? 'bg-green-100 text-green-700' :
                          battle.competitivePosition === 'Competitive' ? 'bg-yellow-100 text-yellow-700' :
                          'bg-red-100 text-red-700'
                        }`}>
                          {battle.competitivePosition}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>

        {/* Loss Reasons */}
        <div className="bg-white rounded-lg border border-gray-200">
          <div className="p-4 border-b border-gray-200">
            <h2 className="font-semibold text-gray-900">Loss Reasons by Competitor</h2>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-gray-50">
                <tr className="text-left text-gray-500">
                  <th className="px-4 py-3">Competitor</th>
                  <th className="px-4 py-3 text-right">Deals Lost</th>
                  <th className="px-4 py-3 text-right">Revenue Lost</th>
                  <th className="px-4 py-3 text-right">Avg Deal Size</th>
                  <th className="px-4 py-3 text-right">Avg Cycle</th>
                  <th className="px-4 py-3 text-right">Loss Share</th>
                  <th className="px-4 py-3 text-right">Threat Level</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {lossReasons.map((reason, idx) => (
                  <tr key={idx} className="hover:bg-gray-50">
                    <td className="px-4 py-3 font-medium text-gray-900">{reason.primaryCompetitor}</td>
                    <td className="px-4 py-3 text-right">{reason.dealsLost}</td>
                    <td className="px-4 py-3 text-right text-red-600">{formatCurrency(reason.revenueLost)}</td>
                    <td className="px-4 py-3 text-right">{formatCurrency(reason.avgDealSize)}</td>
                    <td className="px-4 py-3 text-right">{reason.avgCycleDays.toFixed(0)} days</td>
                    <td className="px-4 py-3 text-right">{formatPercent(reason.lossSharePct)}</td>
                    <td className="px-4 py-3 text-right">
                      <span className={`px-2 py-0.5 rounded text-xs ${
                        reason.threatLevel === 'Critical' ? 'bg-red-100 text-red-700' :
                        reason.threatLevel === 'High' ? 'bg-orange-100 text-orange-700' :
                        reason.threatLevel === 'Medium' ? 'bg-yellow-100 text-yellow-700' :
                        'bg-gray-100 text-gray-700'
                      }`}>
                        {reason.threatLevel}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Win/Loss by Segment */}
        <div className="grid grid-cols-2 gap-6">
          <div className="bg-white rounded-lg border border-gray-200">
            <div className="p-4 border-b border-gray-200">
              <h2 className="font-semibold text-gray-900">Win Rate by Segment</h2>
            </div>
            <div className="p-4 space-y-3">
              {segmentData.map((seg, idx) => (
                <div key={idx} className="flex items-center gap-4">
                  <span className="text-sm text-gray-600 w-28">{seg.dimensionValue}</span>
                  <div className="flex-1 bg-gray-100 rounded-full h-4 overflow-hidden">
                    <div
                      className={`h-4 rounded-full ${
                        seg.winRate >= 40 ? 'bg-green-500' :
                        seg.winRate >= 25 ? 'bg-yellow-500' :
                        'bg-red-500'
                      }`}
                      style={{ width: `${seg.winRate}%` }}
                    />
                  </div>
                  <span className="text-sm font-medium text-gray-900 w-16 text-right">{formatPercent(seg.winRate)}</span>
                  <span className={`px-2 py-0.5 rounded text-xs ${
                    seg.performanceTier === 'Strong' ? 'bg-green-100 text-green-700' :
                    seg.performanceTier === 'Average' ? 'bg-yellow-100 text-yellow-700' :
                    'bg-red-100 text-red-700'
                  }`}>
                    {seg.performanceTier}
                  </span>
                </div>
              ))}
            </div>
          </div>

          <div className="bg-white rounded-lg border border-gray-200">
            <div className="p-4 border-b border-gray-200">
              <h2 className="font-semibold text-gray-900">Win Rate by Deal Size</h2>
            </div>
            <div className="p-4 space-y-3">
              {sizeData.map((size, idx) => (
                <div key={idx} className="flex items-center gap-4">
                  <span className="text-sm text-gray-600 w-40">{size.dimensionValue}</span>
                  <div className="flex-1 bg-gray-100 rounded-full h-4 overflow-hidden">
                    <div
                      className={`h-4 rounded-full ${
                        size.winRate >= 40 ? 'bg-green-500' :
                        size.winRate >= 25 ? 'bg-yellow-500' :
                        'bg-red-500'
                      }`}
                      style={{ width: `${size.winRate}%` }}
                    />
                  </div>
                  <span className="text-sm font-medium text-gray-900 w-16 text-right">{formatPercent(size.winRate)}</span>
                  <span className={`px-2 py-0.5 rounded text-xs ${
                    size.performanceTier === 'Strong' ? 'bg-green-100 text-green-700' :
                    size.performanceTier === 'Average' ? 'bg-yellow-100 text-yellow-700' :
                    'bg-red-100 text-red-700'
                  }`}>
                    {size.performanceTier}
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </main>
    </div>
  )
}
