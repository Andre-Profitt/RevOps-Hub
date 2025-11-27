'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import {
  getForecastSummary,
  getForecastBySegment,
  getForecastHistory,
  getForecastAccuracy,
} from '@/lib/foundry'
import type {
  ForecastSummary,
  ForecastBySegment,
  ForecastHistoryPoint,
  ForecastAccuracy,
} from '@/types'

function formatCurrency(value: number): string {
  if (value >= 1000000) return `$${(value / 1000000).toFixed(1)}M`
  if (value >= 1000) return `$${(value / 1000).toFixed(0)}K`
  return `$${value.toFixed(0)}`
}

function formatPercent(value: number): string {
  return `${value.toFixed(1)}%`
}

export default function ForecastingHubPage() {
  const [summary, setSummary] = useState<ForecastSummary | null>(null)
  const [segments, setSegments] = useState<ForecastBySegment[]>([])
  const [history, setHistory] = useState<ForecastHistoryPoint[]>([])
  const [accuracy, setAccuracy] = useState<ForecastAccuracy[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    Promise.all([
      getForecastSummary(),
      getForecastBySegment(),
      getForecastHistory(),
      getForecastAccuracy(),
    ]).then(([summaryData, segmentData, historyData, accuracyData]) => {
      setSummary(summaryData)
      setSegments(segmentData)
      setHistory(historyData)
      setAccuracy(accuracyData)
      setLoading(false)
    })
  }, [])

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 p-8">
        <div className="animate-pulse">Loading forecasting data...</div>
      </div>
    )
  }

  const bySegment = segments.filter(s => s.dimension === 'segment')
  const byRep = segments.filter(s => s.dimension === 'rep')
  const byStage = segments.filter(s => s.dimension === 'stage')

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
              <h1 className="text-2xl font-bold text-gray-900">Forecasting Hub</h1>
              <p className="text-sm text-gray-500">Multi-methodology forecast with AI insights</p>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <span className={`px-3 py-1 rounded-full text-sm font-medium ${
              summary?.forecastConfidence === 'High' ? 'bg-green-100 text-green-800' :
              summary?.forecastConfidence === 'Medium' ? 'bg-yellow-100 text-yellow-800' :
              'bg-red-100 text-red-800'
            }`}>
              {summary?.forecastConfidence} Confidence
            </span>
          </div>
        </div>
      </header>

      <main className="p-6 space-y-6">
        {/* KPI Cards */}
        <div className="grid grid-cols-5 gap-4">
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Quarterly Quota</p>
            <p className="text-2xl font-bold text-gray-900">{formatCurrency(summary?.quota || 0)}</p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">AI Forecast</p>
            <p className="text-2xl font-bold text-blue-600">{formatCurrency(summary?.aiForecast || 0)}</p>
            <p className="text-xs text-gray-400">{summary?.methodAccuracy ? `${(summary.methodAccuracy * 100).toFixed(0)}% accurate` : ''}</p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Bottom-Up Commit</p>
            <p className="text-2xl font-bold text-gray-900">{formatCurrency(summary?.bottomUpForecast || 0)}</p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Pipeline Coverage</p>
            <p className="text-2xl font-bold text-gray-900">{summary?.pipelineCoverage?.toFixed(1)}x</p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Gap to Target</p>
            <p className={`text-2xl font-bold ${(summary?.gapToTarget || 0) > 0 ? 'text-red-600' : 'text-green-600'}`}>
              {formatCurrency(summary?.gapToTarget || 0)}
            </p>
          </div>
        </div>

        <div className="grid grid-cols-3 gap-6">
          {/* Forecast Methods Comparison */}
          <div className="col-span-2 bg-white rounded-lg border border-gray-200">
            <div className="p-4 border-b border-gray-200">
              <h2 className="font-semibold text-gray-900">Forecast Methods Comparison</h2>
            </div>
            <div className="p-4">
              <div className="space-y-4">
                {/* Visual bar comparison */}
                <div className="space-y-3">
                  <div className="flex items-center gap-4">
                    <span className="text-sm text-gray-600 w-32">AI Weighted</span>
                    <div className="flex-1 bg-gray-100 rounded-full h-6 overflow-hidden">
                      <div
                        className="bg-blue-500 h-6 rounded-full flex items-center justify-end pr-2"
                        style={{ width: `${Math.min(((summary?.aiForecast || 0) / (summary?.quota || 1)) * 100, 100)}%` }}
                      >
                        <span className="text-xs text-white font-medium">{formatCurrency(summary?.aiForecast || 0)}</span>
                      </div>
                    </div>
                  </div>
                  <div className="flex items-center gap-4">
                    <span className="text-sm text-gray-600 w-32">Bottom-Up</span>
                    <div className="flex-1 bg-gray-100 rounded-full h-6 overflow-hidden">
                      <div
                        className="bg-green-500 h-6 rounded-full flex items-center justify-end pr-2"
                        style={{ width: `${Math.min(((summary?.bottomUpForecast || 0) / (summary?.quota || 1)) * 100, 100)}%` }}
                      >
                        <span className="text-xs text-white font-medium">{formatCurrency(summary?.bottomUpForecast || 0)}</span>
                      </div>
                    </div>
                  </div>
                  <div className="flex items-center gap-4">
                    <span className="text-sm text-gray-600 w-32">Best Case</span>
                    <div className="flex-1 bg-gray-100 rounded-full h-6 overflow-hidden">
                      <div
                        className="bg-purple-500 h-6 rounded-full flex items-center justify-end pr-2"
                        style={{ width: `${Math.min(((summary?.bestCaseForecast || 0) / (summary?.quota || 1)) * 100, 100)}%` }}
                      >
                        <span className="text-xs text-white font-medium">{formatCurrency(summary?.bestCaseForecast || 0)}</span>
                      </div>
                    </div>
                  </div>
                  <div className="flex items-center gap-4">
                    <span className="text-sm text-gray-600 w-32">High Confidence</span>
                    <div className="flex-1 bg-gray-100 rounded-full h-6 overflow-hidden">
                      <div
                        className="bg-gray-500 h-6 rounded-full flex items-center justify-end pr-2"
                        style={{ width: `${Math.min(((summary?.highConfidenceAmount || 0) / (summary?.quota || 1)) * 100, 100)}%` }}
                      >
                        <span className="text-xs text-white font-medium">{formatCurrency(summary?.highConfidenceAmount || 0)}</span>
                      </div>
                    </div>
                  </div>
                </div>
                <div className="flex items-center gap-4 pt-4 border-t border-gray-200">
                  <span className="text-sm text-gray-600 w-32">Target</span>
                  <div className="flex-1 border-l-2 border-dashed border-red-400 h-6" style={{ marginLeft: '100%' }} />
                  <span className="text-sm font-medium text-gray-900">{formatCurrency(summary?.quota || 0)}</span>
                </div>
              </div>
            </div>
          </div>

          {/* Methodology Accuracy */}
          <div className="bg-white rounded-lg border border-gray-200">
            <div className="p-4 border-b border-gray-200">
              <h2 className="font-semibold text-gray-900">Methodology Accuracy</h2>
            </div>
            <div className="p-4">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-gray-500">
                    <th className="pb-2">Method</th>
                    <th className="pb-2 text-right">Accuracy</th>
                    <th className="pb-2 text-right">Status</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {accuracy.map((method, idx) => (
                    <tr key={idx}>
                      <td className="py-2 font-medium text-gray-900">{method.method}</td>
                      <td className="py-2 text-right">{formatPercent(method.avgAccuracy * 100)}</td>
                      <td className="py-2 text-right">
                        {method.recommended ? (
                          <span className="px-2 py-0.5 bg-green-100 text-green-700 rounded text-xs">Recommended</span>
                        ) : (
                          <span className={`px-2 py-0.5 rounded text-xs ${
                            method.accuracyTier === 'High' ? 'bg-blue-100 text-blue-700' :
                            method.accuracyTier === 'Medium' ? 'bg-yellow-100 text-yellow-700' :
                            'bg-gray-100 text-gray-700'
                          }`}>{method.accuracyTier}</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>

        {/* Forecast by Segment */}
        <div className="bg-white rounded-lg border border-gray-200">
          <div className="p-4 border-b border-gray-200">
            <h2 className="font-semibold text-gray-900">Forecast by Segment</h2>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-gray-50">
                <tr className="text-left text-gray-500">
                  <th className="px-4 py-3">Segment</th>
                  <th className="px-4 py-3 text-right">Pipeline</th>
                  <th className="px-4 py-3 text-right">Commit</th>
                  <th className="px-4 py-3 text-right">Weighted Forecast</th>
                  <th className="px-4 py-3 text-right">Deals</th>
                  <th className="px-4 py-3 text-right">Health</th>
                  <th className="px-4 py-3 text-right">Confidence</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {bySegment.map((seg, idx) => (
                  <tr key={idx} className="hover:bg-gray-50">
                    <td className="px-4 py-3 font-medium text-gray-900">{seg.dimensionValue}</td>
                    <td className="px-4 py-3 text-right">{formatCurrency(seg.pipelineAmount)}</td>
                    <td className="px-4 py-3 text-right">{formatCurrency(seg.commitAmount)}</td>
                    <td className="px-4 py-3 text-right font-medium text-blue-600">{formatCurrency(seg.weightedForecast)}</td>
                    <td className="px-4 py-3 text-right">{seg.dealCount}</td>
                    <td className="px-4 py-3 text-right">
                      <span className={`${seg.avgHealth >= 70 ? 'text-green-600' : seg.avgHealth >= 50 ? 'text-yellow-600' : 'text-red-600'}`}>
                        {seg.avgHealth.toFixed(0)}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-right">
                      <span className={`px-2 py-0.5 rounded text-xs ${
                        seg.confidenceScore >= 70 ? 'bg-green-100 text-green-700' :
                        seg.confidenceScore >= 50 ? 'bg-yellow-100 text-yellow-700' :
                        'bg-red-100 text-red-700'
                      }`}>
                        {seg.confidenceScore}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Forecast by Rep */}
        <div className="bg-white rounded-lg border border-gray-200">
          <div className="p-4 border-b border-gray-200">
            <h2 className="font-semibold text-gray-900">Forecast by Rep</h2>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-gray-50">
                <tr className="text-left text-gray-500">
                  <th className="px-4 py-3">Rep</th>
                  <th className="px-4 py-3 text-right">Pipeline</th>
                  <th className="px-4 py-3 text-right">Commit</th>
                  <th className="px-4 py-3 text-right">Weighted Forecast</th>
                  <th className="px-4 py-3 text-right">Deals</th>
                  <th className="px-4 py-3 text-right">Commit Rate</th>
                  <th className="px-4 py-3 text-right">Confidence</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {byRep.slice(0, 8).map((rep, idx) => (
                  <tr key={idx} className="hover:bg-gray-50">
                    <td className="px-4 py-3 font-medium text-gray-900">{rep.dimensionValue}</td>
                    <td className="px-4 py-3 text-right">{formatCurrency(rep.pipelineAmount)}</td>
                    <td className="px-4 py-3 text-right">{formatCurrency(rep.commitAmount)}</td>
                    <td className="px-4 py-3 text-right font-medium text-blue-600">{formatCurrency(rep.weightedForecast)}</td>
                    <td className="px-4 py-3 text-right">{rep.dealCount}</td>
                    <td className="px-4 py-3 text-right">{formatPercent(rep.commitRate)}</td>
                    <td className="px-4 py-3 text-right">
                      <span className={`px-2 py-0.5 rounded text-xs ${
                        rep.confidenceScore >= 70 ? 'bg-green-100 text-green-700' :
                        rep.confidenceScore >= 50 ? 'bg-yellow-100 text-yellow-700' :
                        'bg-red-100 text-red-700'
                      }`}>
                        {rep.confidenceScore}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Forecast History */}
        <div className="bg-white rounded-lg border border-gray-200">
          <div className="p-4 border-b border-gray-200">
            <h2 className="font-semibold text-gray-900">Forecast Progression</h2>
          </div>
          <div className="p-4">
            <div className="grid grid-cols-12 gap-2">
              {history.map((week, idx) => (
                <div key={idx} className="text-center">
                  <div className="text-xs text-gray-500 mb-2">{week.weekLabel}</div>
                  <div className="relative h-32 bg-gray-100 rounded">
                    <div
                      className="absolute bottom-0 left-0 right-0 bg-blue-500 rounded-b"
                      style={{ height: `${(week.aiForecast / week.target) * 100}%` }}
                    />
                    <div
                      className="absolute bottom-0 left-0 right-0 bg-green-500 rounded-b opacity-50"
                      style={{ height: `${(week.closedWon / week.target) * 100}%` }}
                    />
                  </div>
                  <div className="text-xs text-gray-600 mt-1">{formatCurrency(week.aiForecast)}</div>
                </div>
              ))}
            </div>
            <div className="flex items-center justify-center gap-6 mt-4">
              <div className="flex items-center gap-2">
                <div className="w-3 h-3 bg-green-500 rounded" />
                <span className="text-xs text-gray-600">Closed Won</span>
              </div>
              <div className="flex items-center gap-2">
                <div className="w-3 h-3 bg-blue-500 rounded" />
                <span className="text-xs text-gray-600">AI Forecast</span>
              </div>
            </div>
          </div>
        </div>
      </main>
    </div>
  )
}
