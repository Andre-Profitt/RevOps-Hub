'use client'

import { useState } from 'react'
import Link from 'next/link'
import { KPICard } from '@/components/ui/KPICard'
import { HealthBadge } from '@/components/ui/HealthIndicator'
import { ComparisonBars } from '@/components/ui/ComparisonBars'
import { AIInsightsCard } from '@/components/ui/AIInsightsCard'
import {
  repPerformance,
  driverComparisons,
  activityMetrics,
  coachingStrengths,
  coachingImprovements,
  coachingActions,
  pipelineDeals,
  nextBestActions,
  salesReps,
} from '@/data/mockData'
import { formatCurrency, formatPercent, formatDelta, cn } from '@/lib/utils'
import {
  ChevronDown,
  ArrowLeft,
  Phone,
  Mail,
  Calendar,
  TrendingUp,
  TrendingDown,
} from 'lucide-react'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  ResponsiveContainer,
  Tooltip,
} from 'recharts'

export default function RepCoachingDashboard() {
  const [selectedRep, setSelectedRep] = useState(salesReps[0])

  const rep = repPerformance

  return (
    <div className="min-h-screen bg-bg-primary">
      {/* Header */}
      <header className="border-b border-border-subtle bg-bg-secondary/50 backdrop-blur-sm sticky top-0 z-50">
        <div className="max-w-[1600px] mx-auto px-6 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4">
              <Link
                href="/"
                className="p-2 rounded-lg hover:bg-bg-elevated text-text-muted hover:text-text-primary transition-colors"
              >
                <ArrowLeft className="w-5 h-5" />
              </Link>
              <div>
                <h1 className="text-xl font-semibold text-text-primary">
                  Rep Coaching Dashboard
                </h1>
                <p className="text-sm text-text-muted">
                  Individual Performance Analysis
                </p>
              </div>
            </div>

            <div className="flex items-center gap-4">
              {/* Rep selector */}
              <div className="relative">
                <button className="flex items-center gap-3 px-4 py-2 bg-bg-elevated rounded-lg text-text-primary hover:bg-bg-interactive transition-colors">
                  <div className="w-8 h-8 bg-accent-blue/20 rounded-full flex items-center justify-center text-accent-blue font-medium">
                    {selectedRep.name.split(' ').map(n => n[0]).join('')}
                  </div>
                  <div className="text-left">
                    <p className="text-sm font-medium">{selectedRep.name}</p>
                    <p className="text-xs text-text-muted">{selectedRep.team} Region</p>
                  </div>
                  <ChevronDown className="w-4 h-4 text-text-muted" />
                </button>
              </div>

              {/* Quarter */}
              <button className="flex items-center gap-2 px-3 py-2 bg-bg-elevated rounded-lg text-sm text-text-secondary hover:bg-bg-interactive transition-colors">
                Q4 2024
                <ChevronDown className="w-4 h-4" />
              </button>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-[1600px] mx-auto px-6 py-8">
        {/* Performance Summary */}
        <div className="card mb-8">
          <h2 className="text-sm font-medium uppercase tracking-wider text-text-muted mb-4">
            Performance Summary
          </h2>
          <div className="grid grid-cols-5 gap-4">
            {/* Attainment */}
            <div className="p-4 bg-bg-elevated rounded-lg">
              <p className="text-xs font-medium uppercase tracking-wider text-text-muted mb-2">
                Attainment
              </p>
              <p className="text-3xl font-semibold font-mono tabular-nums text-text-primary">
                {formatPercent(rep.ytdAttainment)}
              </p>
              <div className="mt-2 h-2 bg-bg-primary rounded-full overflow-hidden">
                <div
                  className="h-full bg-accent-blue rounded-full"
                  style={{ width: `${Math.min(rep.ytdAttainment * 100, 100)}%` }}
                />
              </div>
              <p className="text-sm text-status-healthy mt-2">
                ↑ +12% vs Q3
              </p>
            </div>

            {/* Win Rate */}
            <div className="p-4 bg-bg-elevated rounded-lg">
              <p className="text-xs font-medium uppercase tracking-wider text-text-muted mb-2">
                Win Rate
              </p>
              <p className="text-3xl font-semibold font-mono tabular-nums text-text-primary">
                {formatPercent(rep.winRate)}
              </p>
              <p className="text-sm text-text-muted mt-2">
                Team: {formatPercent(rep.teamAvgWinRate)}
              </p>
              <p className="text-sm text-status-healthy mt-1">
                ↑ +4% vs team
              </p>
            </div>

            {/* Avg Deal Size */}
            <div className="p-4 bg-bg-elevated rounded-lg">
              <p className="text-xs font-medium uppercase tracking-wider text-text-muted mb-2">
                Avg Deal
              </p>
              <p className="text-3xl font-semibold font-mono tabular-nums text-text-primary">
                {formatCurrency(rep.avgDealSize)}
              </p>
              <p className="text-sm text-text-muted mt-2">
                Team: {formatCurrency(rep.teamAvgDealSize)}
              </p>
              <p className="text-sm text-status-healthy mt-1">
                ↑ +16%
              </p>
            </div>

            {/* Sales Cycle */}
            <div className="p-4 bg-bg-elevated rounded-lg">
              <p className="text-xs font-medium uppercase tracking-wider text-text-muted mb-2">
                Sales Cycle
              </p>
              <p className="text-3xl font-semibold font-mono tabular-nums text-text-primary">
                {rep.avgSalesCycle}d
              </p>
              <p className="text-sm text-text-muted mt-2">
                Team: {rep.teamAvgCycle}d
              </p>
              <p className="text-sm text-status-healthy mt-1">
                ↓ -4 days
              </p>
            </div>

            {/* Rank */}
            <div className="p-4 bg-bg-elevated rounded-lg">
              <p className="text-xs font-medium uppercase tracking-wider text-text-muted mb-2">
                Rank
              </p>
              <p className="text-3xl font-semibold font-mono tabular-nums text-text-primary">
                #{rep.overallRank}/{rep.totalReps}
              </p>
              <p className="text-sm text-status-healthy mt-4">
                ↑ +{rep.rankChange} from Q3
              </p>
            </div>
          </div>
        </div>

        {/* Two Column Layout */}
        <div className="grid grid-cols-2 gap-6 mb-8">
          {/* Win Rate Drivers */}
          <div className="card">
            <h2 className="text-lg font-semibold text-text-primary mb-2">
              Win Rate Drivers
            </h2>
            <p className="text-sm text-text-muted mb-6">
              Your pattern vs Top Performers
            </p>
            <ComparisonBars data={driverComparisons} />
          </div>

          {/* Activity Analysis */}
          <div className="card">
            <h2 className="text-lg font-semibold text-text-primary mb-2">
              Activity Analysis
            </h2>
            <p className="text-sm text-text-muted mb-6">
              Last 30 Days
            </p>

            {/* Activity Mix */}
            <div className="mb-6">
              <p className="text-sm font-medium text-text-secondary mb-3">Activity Mix</p>
              <div className="space-y-2">
                <ActivityBar label="Calls" value={activityMetrics.calls} max={activityMetrics.emails} color="#3b82f6" />
                <ActivityBar label="Emails" value={activityMetrics.emails} max={activityMetrics.emails} color="#8b5cf6" />
                <ActivityBar label="Meetings" value={activityMetrics.meetings} max={activityMetrics.emails} color="#22c55e" />
                <ActivityBar label="Demos" value={activityMetrics.demos} max={activityMetrics.emails} color="#f59e0b" />
              </div>
            </div>

            {/* Daily Trend */}
            <div className="mb-6">
              <p className="text-sm font-medium text-text-secondary mb-3">Daily Activity Trend</p>
              <div className="h-24">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={activityMetrics.dailyTrend}>
                    <XAxis
                      dataKey="day"
                      stroke="#71717a"
                      fontSize={12}
                      tickLine={false}
                      axisLine={false}
                    />
                    <YAxis hide domain={[0, 'auto']} />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: '#27272a',
                        border: '1px solid #3f3f46',
                        borderRadius: '8px',
                      }}
                    />
                    <Line
                      type="monotone"
                      dataKey="count"
                      stroke="#3b82f6"
                      strokeWidth={2}
                      dot={false}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </div>

            {/* Benchmarks */}
            <div className="flex items-center gap-6 text-sm">
              <div className="flex items-center gap-2">
                <span className="text-lg">📍</span>
                <span className="text-text-secondary">You:</span>
                <span className="font-mono text-text-primary">{activityMetrics.avgDaily}/day</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-lg">📊</span>
                <span className="text-text-secondary">Team:</span>
                <span className="font-mono text-text-muted">{activityMetrics.teamAvgDaily}/day</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-lg">⭐</span>
                <span className="text-text-secondary">Top:</span>
                <span className="font-mono text-text-muted">{activityMetrics.topPerformerDaily}/day</span>
              </div>
            </div>
          </div>
        </div>

        {/* AI Insights */}
        <div className="mb-8">
          <AIInsightsCard
            strengths={coachingStrengths}
            improvements={coachingImprovements}
            actions={coachingActions}
            lastUpdated="2 min ago"
            onRefresh={() => console.log('Refresh')}
            onAskQuestion={() => console.log('Ask')}
          />
        </div>

        {/* Bottom Row */}
        <div className="grid grid-cols-2 gap-6">
          {/* Pipeline Review */}
          <div className="card">
            <h2 className="text-lg font-semibold text-text-primary mb-4">
              Pipeline Review
            </h2>
            <table className="w-full">
              <thead>
                <tr className="text-left text-xs font-medium uppercase tracking-wider text-text-muted">
                  <th className="pb-3">Deal</th>
                  <th className="pb-3 text-center">Health</th>
                  <th className="pb-3 text-center">Action</th>
                </tr>
              </thead>
              <tbody>
                {pipelineDeals.map((deal) => (
                  <tr key={deal.id} className="border-t border-border-subtle">
                    <td className="py-3">
                      <div className="flex flex-col">
                        <span className="font-medium text-text-primary">{deal.accountName}</span>
                        <span className="text-sm text-text-muted font-mono">
                          {formatCurrency(deal.amount)}
                        </span>
                      </div>
                    </td>
                    <td className="py-3 text-center">
                      <HealthBadge score={deal.healthScore} />
                    </td>
                    <td className="py-3 text-center">
                      <button className="p-2 rounded-lg hover:bg-bg-elevated text-text-muted hover:text-accent-blue transition-colors">
                        {deal.primaryAction === 'call' && <Phone className="w-4 h-4" />}
                        {deal.primaryAction === 'email' && <Mail className="w-4 h-4" />}
                        {deal.primaryAction === 'meeting' && <Calendar className="w-4 h-4" />}
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Next Best Actions */}
          <div className="card">
            <h2 className="text-lg font-semibold text-text-primary mb-4">
              Next Best Actions
            </h2>
            {nextBestActions.length > 0 ? (
              <div className="space-y-4">
                {nextBestActions.map((action, index) => (
                  <div
                    key={action.actionId}
                    className={cn(
                      'p-4 rounded-lg border transition-colors',
                      index === 0
                        ? 'bg-accent-blue/10 border-accent-blue/30'
                        : 'bg-bg-elevated border-border-subtle'
                    )}
                  >
                    <div className="flex items-start gap-3">
                      <div className={cn(
                        'p-2 rounded-lg',
                        index === 0 ? 'bg-accent-blue/20' : 'bg-bg-interactive'
                      )}>
                        {action.actionType === 'call' && <Phone className="w-5 h-5 text-accent-blue" />}
                        {action.actionType === 'email' && <Mail className="w-5 h-5 text-accent-purple" />}
                      </div>
                      <div className="flex-1">
                        <div className="flex items-center gap-2">
                          <span className="font-medium text-text-primary">
                            {action.actionType === 'call' ? 'Call' : 'Email'}: {action.contactName}
                          </span>
                          <span className="text-sm text-text-muted">@ {action.accountName}</span>
                        </div>
                        <p className="text-sm text-text-secondary mt-1 italic">
                          "{action.actionReason}"
                        </p>
                        <p className="text-xs text-text-muted mt-2">
                          Best time: {action.bestTime}
                        </p>
                      </div>
                    </div>
                    {index === 0 && (
                      <div className="flex items-center gap-2 mt-4">
                        <button className="btn-primary flex-1">
                          Start Call
                        </button>
                        <button className="btn-secondary">
                          Defer
                        </button>
                      </div>
                    )}
                  </div>
                ))}
              </div>
            ) : (
              <div className="flex items-center justify-center h-32 text-text-muted">
                No actions scheduled
              </div>
            )}
          </div>
        </div>
      </main>
    </div>
  )
}

function ActivityBar({
  label,
  value,
  max,
  color,
}: {
  label: string
  value: number
  max: number
  color: string
}) {
  const width = (value / max) * 100

  return (
    <div className="flex items-center gap-3">
      <span className="w-16 text-sm text-text-secondary">{label}</span>
      <div className="flex-1 h-5 bg-bg-elevated rounded overflow-hidden">
        <div
          className="h-full rounded transition-all duration-500 flex items-center justify-end pr-2"
          style={{ width: `${width}%`, backgroundColor: color }}
        >
          <span className="text-xs font-medium text-white">{value}</span>
        </div>
      </div>
    </div>
  )
}
