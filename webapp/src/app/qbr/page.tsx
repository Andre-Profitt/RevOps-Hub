'use client'

import { useState, useEffect } from 'react'
import Link from 'next/link'
import {
  getQBRExecutiveSummary,
  getQBRRepPerformance,
  getQBRWinLossAnalysis,
  getDataSourceAsync,
} from '@/lib/foundry'
import {
  defaultQBRExecutiveSummary,
  defaultQBRWinLossAnalysis,
} from '@/lib/defaults'
import { formatCurrency, cn } from '@/lib/utils'
import {
  ArrowLeft,
  RefreshCw,
  Loader2,
  Download,
  FileText,
  Trophy,
  AlertTriangle,
  TrendingUp,
  TrendingDown,
  Target,
  Users,
  BarChart3,
  CheckCircle2,
  XCircle,
  Clock,
  Lightbulb,
  Zap,
} from 'lucide-react'
import type { QBRExecutiveSummary, QBRRepPerformance, QBRWinLossAnalysis } from '@/types'

const QUARTERS = ['Q4 2024', 'Q3 2024', 'Q2 2024', 'Q1 2024']

export default function QBRPage() {
  const [loading, setLoading] = useState(true)
  const [selectedQuarter, setSelectedQuarter] = useState('Q4 2024')
  const [summary, setSummary] = useState<QBRExecutiveSummary>(defaultQBRExecutiveSummary)
  const [repPerformance, setRepPerformance] = useState<QBRRepPerformance[]>([])
  const [winLoss, setWinLoss] = useState<QBRWinLossAnalysis>(defaultQBRWinLossAnalysis)
  const [dataSource, setDataSource] = useState<'foundry' | 'mock'>('mock')

  useEffect(() => {
    async function loadData() {
      setLoading(true)

      // Check server to accurately determine data source
      const source = await getDataSourceAsync()
      setDataSource(source)

      try {
        const [summaryData, repData, winLossData] = await Promise.all([
          getQBRExecutiveSummary(selectedQuarter).catch(() => defaultQBRExecutiveSummary),
          getQBRRepPerformance(selectedQuarter).catch(() => []),
          getQBRWinLossAnalysis(selectedQuarter).catch(() => defaultQBRWinLossAnalysis),
        ])

        setSummary(summaryData)
        setRepPerformance(repData)
        setWinLoss(winLossData)
      } catch (error) {
        console.error('Error loading QBR data:', error)
      } finally {
        setLoading(false)
      }
    }

    loadData()
  }, [selectedQuarter])

  const handleExport = () => {
    // In production, this would generate a PDF or PowerPoint
    alert('Export functionality would generate a PDF/PowerPoint with all QBR data.')
  }

  return (
    <div className="min-h-screen bg-bg-primary">
      {/* Header */}
      <header className="border-b border-border-subtle bg-bg-secondary/50 backdrop-blur-sm sticky top-0 z-50">
        <div className="max-w-[1400px] mx-auto px-6 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4">
              <Link
                href="/"
                className="p-2 rounded-lg hover:bg-bg-elevated text-text-muted hover:text-text-primary transition-colors"
              >
                <ArrowLeft className="w-5 h-5" />
              </Link>
              <div>
                <h1 className="text-xl font-semibold text-text-primary flex items-center gap-2">
                  <FileText className="w-5 h-5 text-accent-purple" />
                  Quarterly Business Review
                </h1>
                <p className="text-sm text-text-muted flex items-center gap-2">
                  RevOps Hub • {selectedQuarter}
                  {loading && <Loader2 className="w-3 h-3 animate-spin" />}
                  <span className={cn(
                    'px-1.5 py-0.5 rounded text-xs',
                    dataSource === 'foundry' ? 'bg-green-500/20 text-green-400' : 'bg-yellow-500/20 text-yellow-400'
                  )}>
                    {dataSource === 'foundry' ? 'Live' : 'Demo'}
                  </span>
                </p>
              </div>
            </div>

            <div className="flex items-center gap-4">
              {/* Quarter selector */}
              <select
                value={selectedQuarter}
                onChange={(e) => setSelectedQuarter(e.target.value)}
                className="px-3 py-2 bg-bg-elevated rounded-lg text-sm text-text-secondary border border-border-subtle"
              >
                {QUARTERS.map((q) => (
                  <option key={q} value={q}>{q}</option>
                ))}
              </select>

              <button
                onClick={handleExport}
                className="flex items-center gap-2 px-4 py-2 bg-accent-purple hover:bg-purple-600 rounded-lg text-sm font-medium text-white transition-colors"
              >
                <Download className="w-4 h-4" />
                Export Pack
              </button>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-[1400px] mx-auto px-6 py-8">
        {/* Executive Summary Header */}
        <div className="card mb-8">
          <div className="flex items-center justify-between mb-6">
            <div>
              <h2 className="text-2xl font-semibold text-text-primary">
                Executive Summary
              </h2>
              <p className="text-sm text-text-muted mt-1">
                Generated {new Date(summary.generatedAt).toLocaleDateString('en-US', {
                  weekday: 'long',
                  year: 'numeric',
                  month: 'long',
                  day: 'numeric',
                })}
              </p>
            </div>
            <div className="flex items-center gap-3">
              <div className="text-right">
                <div className="text-3xl font-bold text-text-primary">
                  {summary.healthGrade}
                </div>
                <div className="text-sm text-text-muted">Overall Grade</div>
              </div>
              <div className={cn(
                'w-16 h-16 rounded-xl flex items-center justify-center',
                summary.healthGrade === 'A' && 'bg-status-healthy/20',
                summary.healthGrade === 'B' && 'bg-accent-blue/20',
                summary.healthGrade === 'C' && 'bg-status-monitor/20',
                summary.healthGrade === 'D' && 'bg-status-at-risk/20',
                summary.healthGrade === 'F' && 'bg-status-critical/20',
              )}>
                <span className={cn(
                  'text-2xl font-bold',
                  summary.healthGrade === 'A' && 'text-status-healthy',
                  summary.healthGrade === 'B' && 'text-accent-blue',
                  summary.healthGrade === 'C' && 'text-status-monitor',
                  summary.healthGrade === 'D' && 'text-status-at-risk',
                  summary.healthGrade === 'F' && 'text-status-critical',
                )}>
                  {Math.round(summary.overallHealthScore)}
                </span>
              </div>
            </div>
          </div>

          {/* Key Metrics Grid */}
          <div className="grid grid-cols-4 gap-4">
            <div className="p-4 bg-bg-elevated rounded-lg">
              <div className="flex items-center gap-2 mb-2">
                <Target className="w-4 h-4 text-accent-blue" />
                <span className="text-sm text-text-muted">Team Attainment</span>
              </div>
              <div className="text-2xl font-semibold text-text-primary">
                {Math.round(summary.teamAttainment * 100)}%
              </div>
              <div className="text-xs text-text-muted mt-1">
                {formatCurrency(summary.totalClosedWon)} / {formatCurrency(summary.totalQuota)}
              </div>
            </div>
            <div className="p-4 bg-bg-elevated rounded-lg">
              <div className="flex items-center gap-2 mb-2">
                <BarChart3 className="w-4 h-4 text-accent-purple" />
                <span className="text-sm text-text-muted">Pipeline Health</span>
              </div>
              <div className="text-2xl font-semibold text-text-primary">
                {Math.round(summary.avgHealthScore)}
              </div>
              <div className="text-xs text-text-muted mt-1">
                {formatCurrency(summary.totalPipeline)} total
              </div>
            </div>
            <div className="p-4 bg-bg-elevated rounded-lg">
              <div className="flex items-center gap-2 mb-2">
                <TrendingUp className="w-4 h-4 text-status-healthy" />
                <span className="text-sm text-text-muted">Win Rate</span>
              </div>
              <div className="text-2xl font-semibold text-text-primary">
                {Math.round(summary.winRate * 100)}%
              </div>
              <div className="text-xs text-text-muted mt-1">
                {formatCurrency(summary.avgDealSize)} avg deal
              </div>
            </div>
            <div className="p-4 bg-bg-elevated rounded-lg">
              <div className="flex items-center gap-2 mb-2">
                <Clock className="w-4 h-4 text-status-monitor" />
                <span className="text-sm text-text-muted">Forecast Accuracy</span>
              </div>
              <div className="text-2xl font-semibold text-text-primary">
                {Math.round(summary.forecastAccuracy * 100)}%
              </div>
              <div className="text-xs text-text-muted mt-1">
                Grade: {summary.forecastGrade}
              </div>
            </div>
          </div>
        </div>

        {/* Insights and Recommendations */}
        <div className="grid grid-cols-2 gap-6 mb-8">
          {/* Key Insights */}
          <div className="card">
            <div className="flex items-center gap-2 mb-4">
              <Lightbulb className="w-5 h-5 text-status-monitor" />
              <h3 className="text-lg font-semibold text-text-primary">Key Insights</h3>
            </div>
            <div className="space-y-3">
              {[summary.insight1, summary.insight2, summary.insight3].filter(Boolean).map((insight, idx) => (
                <div key={idx} className="flex items-start gap-3 p-3 bg-bg-elevated rounded-lg">
                  <div className="w-6 h-6 rounded-full bg-status-monitor/20 flex items-center justify-center flex-shrink-0">
                    <span className="text-xs font-medium text-status-monitor">{idx + 1}</span>
                  </div>
                  <p className="text-sm text-text-secondary">{insight}</p>
                </div>
              ))}
            </div>
          </div>

          {/* Recommendations */}
          <div className="card">
            <div className="flex items-center gap-2 mb-4">
              <Zap className="w-5 h-5 text-accent-purple" />
              <h3 className="text-lg font-semibold text-text-primary">Recommendations</h3>
            </div>
            <div className="space-y-3">
              {[summary.recommendation1, summary.recommendation2, summary.recommendation3].filter(Boolean).map((rec, idx) => (
                <div key={idx} className="flex items-start gap-3 p-3 bg-bg-elevated rounded-lg">
                  <div className="w-6 h-6 rounded-full bg-accent-purple/20 flex items-center justify-center flex-shrink-0">
                    <Zap className="w-3 h-3 text-accent-purple" />
                  </div>
                  <p className="text-sm text-text-secondary">{rec}</p>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Rep Performance Table */}
        <div className="card mb-8">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2">
              <Users className="w-5 h-5 text-accent-blue" />
              <h3 className="text-lg font-semibold text-text-primary">Rep Performance</h3>
            </div>
            <div className="flex items-center gap-4 text-sm">
              <div className="flex items-center gap-1">
                <Trophy className="w-4 h-4 text-status-healthy" />
                <span className="text-text-muted">{summary.topPerformers} Top Performers</span>
              </div>
              <div className="flex items-center gap-1">
                <AlertTriangle className="w-4 h-4 text-status-at-risk" />
                <span className="text-text-muted">{summary.atRiskReps} At Risk</span>
              </div>
            </div>
          </div>
          <table className="w-full">
            <thead>
              <tr className="text-left text-xs font-medium uppercase tracking-wider text-text-muted border-b border-border-subtle">
                <th className="pb-3 pl-2">Rep</th>
                <th className="pb-3">Region</th>
                <th className="pb-3 text-right">Closed Won</th>
                <th className="pb-3 text-right">Quota</th>
                <th className="pb-3 text-right">Attainment</th>
                <th className="pb-3 text-right">Win Rate</th>
                <th className="pb-3 text-right">Pipeline</th>
                <th className="pb-3 text-center">Status</th>
              </tr>
            </thead>
            <tbody>
              {repPerformance.map((rep) => (
                <tr key={rep.ownerId} className="border-b border-border-subtle hover:bg-bg-elevated/50">
                  <td className="py-3 pl-2">
                    <div className="flex items-center gap-2">
                      {rep.performanceTier === 'Top Performer' && (
                        <Trophy className="w-4 h-4 text-status-healthy" />
                      )}
                      {rep.performanceTier === 'At Risk' && (
                        <AlertTriangle className="w-4 h-4 text-status-at-risk" />
                      )}
                      <span className="text-sm font-medium text-text-primary">{rep.ownerName}</span>
                    </div>
                  </td>
                  <td className="py-3 text-sm text-text-secondary">{rep.region}</td>
                  <td className="py-3 text-sm text-right font-mono tabular-nums text-text-primary">
                    {formatCurrency(rep.closedWon)}
                  </td>
                  <td className="py-3 text-sm text-right font-mono tabular-nums text-text-muted">
                    {formatCurrency(rep.quotaAmount)}
                  </td>
                  <td className="py-3 text-sm text-right">
                    <span className={cn(
                      'font-medium',
                      rep.quotaAttainment >= 1 && 'text-status-healthy',
                      rep.quotaAttainment >= 0.7 && rep.quotaAttainment < 1 && 'text-status-monitor',
                      rep.quotaAttainment < 0.7 && 'text-status-at-risk',
                    )}>
                      {Math.round(rep.quotaAttainment * 100)}%
                    </span>
                  </td>
                  <td className="py-3 text-sm text-right font-mono tabular-nums text-text-secondary">
                    {Math.round(rep.winRate * 100)}%
                  </td>
                  <td className="py-3 text-sm text-right font-mono tabular-nums text-text-muted">
                    {formatCurrency(rep.openPipeline)}
                  </td>
                  <td className="py-3 text-center">
                    <span className={cn(
                      'text-xs px-2 py-1 rounded-full',
                      rep.performanceTier === 'Top Performer' && 'bg-status-healthy/20 text-status-healthy',
                      rep.performanceTier === 'On Track' && 'bg-accent-blue/20 text-accent-blue',
                      rep.performanceTier === 'Needs Improvement' && 'bg-status-monitor/20 text-status-monitor',
                      rep.performanceTier === 'At Risk' && 'bg-status-at-risk/20 text-status-at-risk',
                    )}>
                      {rep.performanceTier}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Win/Loss Analysis */}
        <div className="card">
          <div className="flex items-center gap-2 mb-6">
            <BarChart3 className="w-5 h-5 text-accent-purple" />
            <h3 className="text-lg font-semibold text-text-primary">Win/Loss Analysis</h3>
          </div>

          <div className="grid grid-cols-2 gap-8">
            {/* Win vs Loss Comparison */}
            <div>
              <h4 className="text-sm font-medium text-text-primary mb-4">Comparison</h4>
              <div className="grid grid-cols-3 gap-4">
                <div className="text-center">
                  <div className="text-xs text-text-muted uppercase mb-2">Metric</div>
                </div>
                <div className="text-center">
                  <div className="flex items-center justify-center gap-1 mb-2">
                    <CheckCircle2 className="w-4 h-4 text-status-healthy" />
                    <span className="text-xs text-status-healthy uppercase">Won</span>
                  </div>
                </div>
                <div className="text-center">
                  <div className="flex items-center justify-center gap-1 mb-2">
                    <XCircle className="w-4 h-4 text-status-at-risk" />
                    <span className="text-xs text-status-at-risk uppercase">Lost</span>
                  </div>
                </div>

                <div className="text-sm text-text-muted">Deal Count</div>
                <div className="text-sm font-medium text-center text-status-healthy">{winLoss.winsCount}</div>
                <div className="text-sm font-medium text-center text-status-at-risk">{winLoss.lossesCount}</div>

                <div className="text-sm text-text-muted">Avg Deal Size</div>
                <div className="text-sm font-medium text-center text-text-primary">{formatCurrency(winLoss.winsAvgDealSize)}</div>
                <div className="text-sm font-medium text-center text-text-primary">{formatCurrency(winLoss.lossesAvgDealSize)}</div>

                <div className="text-sm text-text-muted">Avg Activities</div>
                <div className="text-sm font-medium text-center text-text-primary">{winLoss.winsAvgActivities.toFixed(1)}</div>
                <div className="text-sm font-medium text-center text-text-primary">{winLoss.lossesAvgActivities.toFixed(1)}</div>

                <div className="text-sm text-text-muted">Avg Meetings</div>
                <div className="text-sm font-medium text-center text-text-primary">{winLoss.winsAvgMeetings.toFixed(1)}</div>
                <div className="text-sm font-medium text-center text-text-primary">{winLoss.lossesAvgMeetings.toFixed(1)}</div>

                <div className="text-sm text-text-muted">Avg Cycle (days)</div>
                <div className="text-sm font-medium text-center text-text-primary">{Math.round(winLoss.winsAvgCycle)}</div>
                <div className="text-sm font-medium text-center text-text-primary">{Math.round(winLoss.lossesAvgCycle)}</div>
              </div>

              <div className="mt-4 p-3 bg-status-healthy/10 border border-status-healthy/20 rounded-lg">
                <p className="text-sm text-text-secondary">
                  <span className="font-medium text-status-healthy">Key Insight:</span>{' '}
                  Won deals average {winLoss.activityDelta.toFixed(1)} more activities and {winLoss.meetingDelta.toFixed(1)} more meetings than lost deals.
                </p>
              </div>
            </div>

            {/* Loss Reasons */}
            <div>
              <h4 className="text-sm font-medium text-text-primary mb-4">Top Loss Reasons</h4>
              <div className="space-y-3">
                {[
                  { reason: winLoss.topLossReason1, pct: winLoss.topLossReason1Pct },
                  { reason: winLoss.topLossReason2, pct: winLoss.topLossReason2Pct },
                  { reason: winLoss.topLossReason3, pct: winLoss.topLossReason3Pct },
                ].map((item, idx) => (
                  <div key={idx} className="flex items-center gap-3">
                    <div className="w-8 h-8 rounded-lg bg-status-at-risk/20 flex items-center justify-center flex-shrink-0">
                      <span className="text-sm font-medium text-status-at-risk">{idx + 1}</span>
                    </div>
                    <div className="flex-1">
                      <div className="flex items-center justify-between mb-1">
                        <span className="text-sm font-medium text-text-primary">{item.reason}</span>
                        <span className="text-sm text-text-muted">{Math.round(item.pct * 100)}%</span>
                      </div>
                      <div className="h-2 bg-bg-elevated rounded-full overflow-hidden">
                        <div
                          className="h-full bg-status-at-risk rounded-full"
                          style={{ width: `${item.pct * 100}%` }}
                        />
                      </div>
                    </div>
                  </div>
                ))}
              </div>

              <div className="mt-6 p-4 bg-bg-elevated rounded-lg">
                <div className="flex items-center gap-2 mb-2">
                  <TrendingDown className="w-4 h-4 text-status-at-risk" />
                  <span className="text-sm font-medium text-text-primary">Total Lost Revenue</span>
                </div>
                <div className="text-2xl font-semibold text-status-at-risk">
                  {formatCurrency(winLoss.lossesRevenue)}
                </div>
                <p className="text-xs text-text-muted mt-1">
                  {winLoss.lossesCount} deals lost in {selectedQuarter}
                </p>
              </div>
            </div>
          </div>
        </div>
      </main>
    </div>
  )
}
