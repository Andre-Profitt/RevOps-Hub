'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import {
  getDealDeskSummary,
  getDealApprovals,
  getDiscountAnalysis,
} from '@/lib/foundry'
import type {
  DealDeskSummary,
  DealApproval,
  DiscountAnalysis,
} from '@/types'

function formatCurrency(value: number): string {
  if (value >= 1000000) return `$${(value / 1000000).toFixed(1)}M`
  if (value >= 1000) return `$${(value / 1000).toFixed(0)}K`
  return `$${value.toFixed(0)}`
}

function formatPercent(value: number): string {
  return `${value.toFixed(1)}%`
}

function formatDate(dateStr: string): string {
  return new Date(dateStr).toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

export default function DealDeskPage() {
  const [summary, setSummary] = useState<DealDeskSummary | null>(null)
  const [approvals, setApprovals] = useState<DealApproval[]>([])
  const [discounts, setDiscounts] = useState<DiscountAnalysis[]>([])
  const [loading, setLoading] = useState(true)
  const [activeTab, setActiveTab] = useState<'pending' | 'approved' | 'rejected'>('pending')

  useEffect(() => {
    Promise.all([
      getDealDeskSummary(),
      getDealApprovals(),
      getDiscountAnalysis(),
    ]).then(([summaryData, approvalsData, discountsData]) => {
      setSummary(summaryData)
      setApprovals(approvalsData)
      setDiscounts(discountsData)
      setLoading(false)
    })
  }, [])

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 p-8">
        <div className="animate-pulse">Loading deal desk data...</div>
      </div>
    )
  }

  const filteredApprovals = approvals.filter(a => {
    if (activeTab === 'pending') return a.approvalStatus === 'pending'
    if (activeTab === 'approved') return a.approvalStatus === 'approved'
    return a.approvalStatus === 'rejected'
  })

  const pendingCount = approvals.filter(a => a.approvalStatus === 'pending').length
  const approvedCount = approvals.filter(a => a.approvalStatus === 'approved').length
  const rejectedCount = approvals.filter(a => a.approvalStatus === 'rejected').length

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
              <h1 className="text-2xl font-bold text-gray-900">Deal Desk</h1>
              <p className="text-sm text-gray-500">Approval workflows and discount management</p>
            </div>
          </div>
          {(summary?.slaBreachCount ?? 0) > 0 && (
            <div className="flex items-center gap-2 bg-red-50 px-3 py-1.5 rounded-lg">
              <svg className="w-5 h-5 text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
              <span className="text-sm font-medium text-red-700">{summary?.slaBreachCount} SLA Breaches</span>
            </div>
          )}
        </div>
      </header>

      <main className="p-6 space-y-6">
        {/* KPI Cards */}
        <div className="grid grid-cols-5 gap-4">
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Pending Approvals</p>
            <p className="text-2xl font-bold text-orange-600">{summary?.pendingApprovals}</p>
            <p className="text-xs text-gray-400">{formatCurrency(summary?.pendingValue || 0)} total</p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Avg Approval Time</p>
            <p className="text-2xl font-bold text-gray-900">{summary?.avgApprovalTime?.toFixed(1)}h</p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Approved Today</p>
            <p className="text-2xl font-bold text-green-600">{summary?.approvedToday}</p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Rejected Today</p>
            <p className="text-2xl font-bold text-red-600">{summary?.rejectedToday}</p>
          </div>
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <p className="text-sm text-gray-500">Avg Discount</p>
            <p className="text-2xl font-bold text-gray-900">{formatPercent(summary?.avgDiscount || 0)}</p>
            <p className={`text-xs ${summary?.discountTrend === 'up' ? 'text-red-500' : 'text-green-500'}`}>
              {summary?.discountTrend === 'up' ? 'Increasing' : 'Stable'}
            </p>
          </div>
        </div>

        <div className="grid grid-cols-3 gap-6">
          {/* Approval Queue */}
          <div className="col-span-2 bg-white rounded-lg border border-gray-200">
            <div className="p-4 border-b border-gray-200">
              <div className="flex items-center justify-between">
                <h2 className="font-semibold text-gray-900">Approval Queue</h2>
                <div className="flex gap-1 bg-gray-100 rounded-lg p-1">
                  <button
                    onClick={() => setActiveTab('pending')}
                    className={`px-3 py-1 rounded text-sm ${
                      activeTab === 'pending' ? 'bg-white shadow text-gray-900' : 'text-gray-600'
                    }`}
                  >
                    Pending ({pendingCount})
                  </button>
                  <button
                    onClick={() => setActiveTab('approved')}
                    className={`px-3 py-1 rounded text-sm ${
                      activeTab === 'approved' ? 'bg-white shadow text-gray-900' : 'text-gray-600'
                    }`}
                  >
                    Approved ({approvedCount})
                  </button>
                  <button
                    onClick={() => setActiveTab('rejected')}
                    className={`px-3 py-1 rounded text-sm ${
                      activeTab === 'rejected' ? 'bg-white shadow text-gray-900' : 'text-gray-600'
                    }`}
                  >
                    Rejected ({rejectedCount})
                  </button>
                </div>
              </div>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead className="bg-gray-50">
                  <tr className="text-left text-gray-500">
                    <th className="px-4 py-3">Deal</th>
                    <th className="px-4 py-3">Owner</th>
                    <th className="px-4 py-3 text-right">Amount</th>
                    <th className="px-4 py-3 text-right">Discount</th>
                    <th className="px-4 py-3 text-center">Urgency</th>
                    <th className="px-4 py-3">Submitted</th>
                    <th className="px-4 py-3">Status</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {filteredApprovals.map((deal, idx) => (
                    <tr key={idx} className={`hover:bg-gray-50 ${deal.isSlaBreach ? 'bg-red-50' : ''}`}>
                      <td className="px-4 py-3">
                        <div className="font-medium text-gray-900">{deal.dealName}</div>
                        <div className="text-xs text-gray-500">{deal.accountName}</div>
                      </td>
                      <td className="px-4 py-3 text-gray-600">{deal.ownerName}</td>
                      <td className="px-4 py-3 text-right font-medium">{formatCurrency(deal.amount)}</td>
                      <td className="px-4 py-3 text-right">
                        <span className={deal.discountPercent > 20 ? 'text-red-600 font-medium' : 'text-gray-600'}>
                          {formatPercent(deal.discountPercent)}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-center">
                        <span className={`px-2 py-0.5 rounded text-xs ${
                          deal.urgency === 'critical' ? 'bg-red-100 text-red-700' :
                          deal.urgency === 'high' ? 'bg-orange-100 text-orange-700' :
                          deal.urgency === 'medium' ? 'bg-yellow-100 text-yellow-700' :
                          'bg-gray-100 text-gray-700'
                        }`}>
                          {deal.urgency}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-gray-600">{formatDate(deal.submittedAt)}</td>
                      <td className="px-4 py-3">
                        {deal.isSlaBreach && (
                          <span className="px-2 py-0.5 bg-red-100 text-red-700 rounded text-xs mr-2">SLA Breach</span>
                        )}
                        <span className={`px-2 py-0.5 rounded text-xs ${
                          deal.approvalStatus === 'approved' ? 'bg-green-100 text-green-700' :
                          deal.approvalStatus === 'rejected' ? 'bg-red-100 text-red-700' :
                          'bg-yellow-100 text-yellow-700'
                        }`}>
                          {deal.approvalStatus}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {filteredApprovals.length === 0 && (
                <div className="p-8 text-center text-gray-500">
                  No {activeTab} deals
                </div>
              )}
            </div>
          </div>

          {/* Discount Analysis */}
          <div className="bg-white rounded-lg border border-gray-200">
            <div className="p-4 border-b border-gray-200">
              <h2 className="font-semibold text-gray-900">Discount Analysis</h2>
              <p className="text-sm text-gray-500">By segment</p>
            </div>
            <div className="p-4 space-y-4">
              {discounts.map((segment, idx) => (
                <div key={idx} className="p-3 bg-gray-50 rounded-lg">
                  <div className="flex items-center justify-between mb-2">
                    <span className="font-medium text-gray-900">{segment.segment}</span>
                    <span className="text-sm text-gray-500">{segment.dealCount} deals</span>
                  </div>
                  <div className="grid grid-cols-2 gap-4 text-sm mb-2">
                    <div>
                      <span className="text-gray-500">Avg:</span>
                      <span className="ml-2 font-medium">{formatPercent(segment.avgDiscount)}</span>
                    </div>
                    <div>
                      <span className="text-gray-500">Max:</span>
                      <span className={`ml-2 font-medium ${segment.maxDiscount > 25 ? 'text-red-600' : ''}`}>
                        {formatPercent(segment.maxDiscount)}
                      </span>
                    </div>
                  </div>
                  <div className="flex items-center gap-2 text-xs">
                    <span className="text-gray-500">Win rate with discount:</span>
                    <span className={`font-medium ${
                      segment.winRateWithDiscount > segment.winRateWithoutDiscount ? 'text-green-600' : 'text-red-600'
                    }`}>
                      {formatPercent(segment.winRateWithDiscount)}
                    </span>
                    <span className="text-gray-400">vs</span>
                    <span className="font-medium">{formatPercent(segment.winRateWithoutDiscount)}</span>
                    <span className="text-gray-500">without</span>
                  </div>
                  {segment.recommendation && (
                    <p className="text-xs text-blue-600 mt-2">{segment.recommendation}</p>
                  )}
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Urgency Distribution */}
        <div className="bg-white rounded-lg border border-gray-200">
          <div className="p-4 border-b border-gray-200">
            <h2 className="font-semibold text-gray-900">Deals by Urgency</h2>
          </div>
          <div className="p-4">
            <div className="flex items-center gap-4">
              {summary?.dealsByUrgency && Object.entries(summary.dealsByUrgency).map(([urgency, count]) => (
                <div key={urgency} className="flex-1 p-4 bg-gray-50 rounded-lg text-center">
                  <p className={`text-3xl font-bold ${
                    urgency === 'critical' ? 'text-red-600' :
                    urgency === 'high' ? 'text-orange-600' :
                    urgency === 'medium' ? 'text-yellow-600' :
                    'text-gray-600'
                  }`}>
                    {count}
                  </p>
                  <p className="text-sm text-gray-500 capitalize">{urgency}</p>
                </div>
              ))}
            </div>
          </div>
        </div>
      </main>
    </div>
  )
}
