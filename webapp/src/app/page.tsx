'use client'

import { useState } from 'react'
import Link from 'next/link'
import { KPICard } from '@/components/ui/KPICard'
import { HealthIndicator } from '@/components/ui/HealthIndicator'
import { StuckDealsTable } from '@/components/ui/DataTable'
import { ForecastTrendChart } from '@/components/charts/ForecastTrendChart'
import { HealthDistributionChart } from '@/components/charts/HealthDistributionChart'
import { PipelineFunnel } from '@/components/charts/PipelineFunnel'
import {
  dashboardKPIs,
  forecastTrend,
  stageVelocity,
  pipelineFunnel,
  stuckDeals,
  healthDistribution,
} from '@/data/mockData'
import { formatCurrency, formatPercent, formatDelta, cn } from '@/lib/utils'
import { ChevronDown, RefreshCw, Settings, Bell } from 'lucide-react'
import type { StuckDeal } from '@/types'

export default function PipelineHealthDashboard() {
  const [selectedDeal, setSelectedDeal] = useState<StuckDeal | null>(null)

  return (
    <div className="min-h-screen bg-bg-primary">
      {/* Header */}
      <header className="border-b border-border-subtle bg-bg-secondary/50 backdrop-blur-sm sticky top-0 z-50">
        <div className="max-w-[1600px] mx-auto px-6 py-4">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-xl font-semibold text-text-primary">
                Pipeline Health Dashboard
              </h1>
              <p className="text-sm text-text-muted">
                RevOps Hub • Q4 2024
              </p>
            </div>

            <div className="flex items-center gap-4">
              {/* Quarter selector */}
              <button className="flex items-center gap-2 px-3 py-2 bg-bg-elevated rounded-lg text-sm text-text-secondary hover:bg-bg-interactive transition-colors">
                Q4 2024
                <ChevronDown className="w-4 h-4" />
              </button>

              {/* Team selector */}
              <button className="flex items-center gap-2 px-3 py-2 bg-bg-elevated rounded-lg text-sm text-text-secondary hover:bg-bg-interactive transition-colors">
                All Teams
                <ChevronDown className="w-4 h-4" />
              </button>

              {/* Actions */}
              <button className="p-2 rounded-lg hover:bg-bg-elevated text-text-muted hover:text-text-primary transition-colors">
                <RefreshCw className="w-5 h-5" />
              </button>
              <button className="p-2 rounded-lg hover:bg-bg-elevated text-text-muted hover:text-text-primary transition-colors">
                <Bell className="w-5 h-5" />
              </button>

              {/* Nav to coaching */}
              <Link
                href="/coaching"
                className="px-4 py-2 bg-accent-blue hover:bg-blue-600 rounded-lg text-sm font-medium text-white transition-colors"
              >
                Rep Coaching →
              </Link>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-[1600px] mx-auto px-6 py-8">
        {/* KPI Cards Row */}
        <div className="grid grid-cols-5 gap-4 mb-8">
          <KPICard
            title="QUOTA"
            value={dashboardKPIs.quarterTarget}
            format="currency"
            subtitle="Target"
            progress={{
              current: dashboardKPIs.closedWonAmount,
              target: dashboardKPIs.quarterTarget,
              color: '#3b82f6',
            }}
          />
          <KPICard
            title="AI FORECAST"
            value={dashboardKPIs.aiForecast}
            format="currency"
            subtitle="Projected"
            trend={{
              value: dashboardKPIs.forecastChange1w,
              label: 'vs last week',
            }}
            progress={{
              current: dashboardKPIs.aiForecast,
              target: dashboardKPIs.quarterTarget,
              color: '#3b82f6',
            }}
          />
          <KPICard
            title="COMMIT"
            value={dashboardKPIs.commitAmount}
            format="currency"
            subtitle={`${Math.round(dashboardKPIs.commitConfidence * 100)}% confidence`}
            progress={{
              current: dashboardKPIs.commitAmount * dashboardKPIs.commitConfidence,
              target: dashboardKPIs.commitAmount,
              color: '#22c55e',
            }}
          />
          <KPICard
            title="GAP"
            value={dashboardKPIs.gapToTarget}
            format="currency"
            subtitle={`${Math.round(dashboardKPIs.gapPct * 100)}% to close`}
            comparison={{
              value: `${dashboardKPIs.coverageRatio.toFixed(1)}x`,
              label: 'Coverage',
            }}
          />
          <div className="kpi-card">
            <span className="kpi-label">RISK LEVEL</span>
            <div className="mt-3 mb-2">
              <span className={cn(
                'text-2xl font-semibold',
                dashboardKPIs.riskLevel === 'Critical' && 'text-status-critical',
                dashboardKPIs.riskLevel === 'High' && 'text-status-at-risk',
                dashboardKPIs.riskLevel === 'Medium' && 'text-status-monitor',
                dashboardKPIs.riskLevel === 'Low' && 'text-status-healthy',
              )}>
                {dashboardKPIs.riskLevel}
              </span>
            </div>
            <div className="flex items-center gap-2 text-sm text-text-muted">
              <span>{stuckDeals.filter(d => d.urgency === 'CRITICAL').length} critical deals</span>
            </div>
          </div>
        </div>

        {/* Forecast Trend Chart */}
        <div className="card mb-8">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-semibold text-text-primary">
              Forecast vs Actuals Trend
            </h2>
            <div className="flex items-center gap-4 text-sm text-text-muted">
              <span>October - December 2024</span>
            </div>
          </div>
          <ForecastTrendChart data={forecastTrend} currentWeek={7} />
        </div>

        {/* Two Column Layout */}
        <div className="grid grid-cols-2 gap-6 mb-8">
          {/* Pipeline Funnel */}
          <div className="card">
            <h2 className="text-lg font-semibold text-text-primary mb-4">
              Pipeline Funnel
            </h2>
            <PipelineFunnel data={pipelineFunnel} />
          </div>

          {/* Stage Velocity */}
          <div className="card">
            <h2 className="text-lg font-semibold text-text-primary mb-4">
              Stage Velocity
            </h2>
            <table className="w-full">
              <thead>
                <tr className="text-left text-xs font-medium uppercase tracking-wider text-text-muted">
                  <th className="pb-3">Stage</th>
                  <th className="pb-3 text-right">Avg</th>
                  <th className="pb-3 text-right">Bench</th>
                  <th className="pb-3 text-right">Trend</th>
                </tr>
              </thead>
              <tbody>
                {stageVelocity.map((stage) => (
                  <tr key={stage.stageName} className="border-t border-border-subtle">
                    <td className="py-3 text-sm text-text-primary">{stage.stageName}</td>
                    <td className="py-3 text-sm text-right font-mono tabular-nums">
                      {stage.avgDuration}d
                    </td>
                    <td className="py-3 text-sm text-right font-mono tabular-nums text-text-muted">
                      {stage.benchmark}d
                    </td>
                    <td className="py-3 text-sm text-right">
                      <span className={cn(
                        'inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium',
                        stage.deviation > 3 && 'bg-status-critical/20 text-status-critical',
                        stage.deviation > 0 && stage.deviation <= 3 && 'bg-status-monitor/20 text-status-monitor',
                        stage.deviation <= 0 && 'bg-status-healthy/20 text-status-healthy',
                      )}>
                        {stage.deviation > 0 ? '↑' : stage.deviation < 0 ? '↓' : '→'} {formatDelta(stage.deviation, 'd')}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            <div className="mt-4 pt-4 border-t border-border-subtle text-sm text-text-muted">
              <p>
                Conversion Rates:{' '}
                <span className="text-text-secondary">
                  Qual→Disc: 68% • Disc→Sol: 65% • Sol→Prop: 64% • Prop→Neg: 73% • Neg→Won: 45%
                </span>
              </p>
            </div>
          </div>
        </div>

        {/* Stuck Deals Table */}
        <div className="card mb-8">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-semibold text-text-primary">
              Stuck Deals Requiring Action
            </h2>
            <button className="text-sm text-accent-blue hover:underline">
              View All →
            </button>
          </div>
          <StuckDealsTable
            data={stuckDeals}
            selectedId={selectedDeal?.id}
            onRowClick={setSelectedDeal}
            maxRows={5}
          />
        </div>

        {/* Bottom Row */}
        <div className="grid grid-cols-2 gap-6">
          {/* Health Distribution */}
          <div className="card">
            <h2 className="text-lg font-semibold text-text-primary mb-4">
              Health Distribution
            </h2>
            <HealthDistributionChart data={healthDistribution} />
          </div>

          {/* Selected Deal Detail */}
          <div className="card">
            <h2 className="text-lg font-semibold text-text-primary mb-4">
              Deal Detail + AI Insights
            </h2>
            {selectedDeal ? (
              <div>
                <div className="flex items-start justify-between mb-4">
                  <div>
                    <h3 className="text-xl font-semibold text-text-primary">
                      {selectedDeal.accountName}
                    </h3>
                    <p className="text-sm text-text-muted">{selectedDeal.opportunityName}</p>
                  </div>
                  <HealthIndicator
                    score={selectedDeal.healthScore}
                    showBar
                    showLabel
                    size="lg"
                  />
                </div>

                <div className="grid grid-cols-3 gap-4 mb-4">
                  <div>
                    <p className="text-sm text-text-muted">Amount</p>
                    <p className="text-lg font-mono tabular-nums text-text-primary">
                      {formatCurrency(selectedDeal.amount)}
                    </p>
                  </div>
                  <div>
                    <p className="text-sm text-text-muted">Stage</p>
                    <p className="text-lg text-text-primary">{selectedDeal.stageName}</p>
                  </div>
                  <div>
                    <p className="text-sm text-text-muted">Days in Stage</p>
                    <p className="text-lg font-mono tabular-nums text-text-primary">
                      {selectedDeal.daysInStage}
                    </p>
                  </div>
                </div>

                <div className="p-4 bg-accent-purple/10 border border-accent-purple/20 rounded-lg">
                  <div className="flex items-center gap-2 mb-2">
                    <span className="text-accent-purple">🤖</span>
                    <span className="text-sm font-medium text-text-primary">AI Analysis:</span>
                  </div>
                  <p className="text-sm text-text-secondary italic">
                    "Deal stuck in {selectedDeal.stageName} due to {selectedDeal.riskFlags[0]?.toLowerCase()}.
                    {selectedDeal.daysInStage > 20 && ` Last executive meeting was ${selectedDeal.daysInStage - 5} days ago.`}
                    Recommend immediate {selectedDeal.primaryAction} to re-engage."
                  </p>
                </div>
              </div>
            ) : (
              <div className="flex items-center justify-center h-48 text-text-muted">
                Select a deal to view details
              </div>
            )}
          </div>
        </div>
      </main>
    </div>
  )
}
