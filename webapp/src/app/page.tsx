'use client'

import { useState, useEffect } from 'react'
import Link from 'next/link'
import { KPICard } from '@/components/ui/KPICard'
import { HealthIndicator } from '@/components/ui/HealthIndicator'
import { StuckDealsTable } from '@/components/ui/DataTable'
import { ForecastTrendChart } from '@/components/charts/ForecastTrendChart'
import { HealthDistributionChart } from '@/components/charts/HealthDistributionChart'
import { PipelineFunnel } from '@/components/charts/PipelineFunnel'
import {
  getDashboardKPIs,
  getForecastTrend,
  getStuckDeals,
  getHealthDistribution,
  getProcessBottlenecks,
  getCompetitiveLosses,
  getStageVelocity,
  getPipelineFunnel,
  getHygieneSummary,
  getHygieneAlerts,
  getHygieneTrends,
  getDealPredictions,
  getLeadingIndicatorsSummary,
  getTelemetrySummary,
  getFunnelHandoffs,
  getCrossTeamActivity,
  getDataSourceAsync,
  type DashboardFilters,
} from '@/lib/foundry'
import {
  defaultDashboardKPIs,
  defaultHealthDistribution,
  defaultHygieneSummary,
  defaultLeadingIndicatorsSummary,
  defaultTelemetrySummary,
} from '@/lib/defaults'
import { DashboardSkeleton } from '@/components/ui/Skeleton'
import { ErrorState } from '@/components/ui/ErrorState'
import { formatCurrency, formatPercent, formatDelta, cn } from '@/lib/utils'
import { ChevronDown, RefreshCw, Settings, Bell, Loader2, AlertTriangle, CheckCircle2, XCircle, Clock, Users, Calendar, TrendingUp, Shield, Brain, Target, Zap, ArrowUpRight, ArrowDownRight, GitBranch, BarChart3, ArrowRight, Timer } from 'lucide-react'
import type { StuckDeal, DashboardKPIs, ForecastTrendPoint, HealthDistribution, ProcessBottleneck, CompetitiveLoss, StageVelocity, FunnelStage, HygieneSummary, HygieneAlert, HygieneTrendPoint, DealPrediction, LeadingIndicatorsSummary, TelemetrySummary, FunnelHandoffMetrics, CrossTeamActivity } from '@/types'

const QUARTERS = ['Q4 2024', 'Q3 2024', 'Q2 2024', 'Q1 2024']
const TEAMS = ['All Teams', 'West', 'Central', 'East', 'South']

export default function PipelineHealthDashboard() {
  const [selectedDeal, setSelectedDeal] = useState<StuckDeal | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [dashboardKPIs, setDashboardKPIs] = useState<DashboardKPIs>(defaultDashboardKPIs)
  const [forecastTrend, setForecastTrend] = useState<ForecastTrendPoint[]>([])
  const [stuckDeals, setStuckDeals] = useState<StuckDeal[]>([])
  const [healthDistribution, setHealthDistribution] = useState<HealthDistribution>(defaultHealthDistribution)
  const [processBottlenecks, setProcessBottlenecks] = useState<ProcessBottleneck[]>([])
  const [competitiveLosses, setCompetitiveLosses] = useState<CompetitiveLoss[]>([])
  const [stageVelocity, setStageVelocity] = useState<StageVelocity[]>([])
  const [pipelineFunnel, setPipelineFunnel] = useState<FunnelStage[]>([])
  const [hygieneSummary, setHygieneSummary] = useState<HygieneSummary>(defaultHygieneSummary)
  const [hygieneAlerts, setHygieneAlerts] = useState<HygieneAlert[]>([])
  const [hygieneTrends, setHygieneTrends] = useState<HygieneTrendPoint[]>([])
  const [dealPredictions, setDealPredictions] = useState<DealPrediction[]>([])
  const [leadingIndicators, setLeadingIndicators] = useState<LeadingIndicatorsSummary>(defaultLeadingIndicatorsSummary)
  const [telemetrySummary, setTelemetrySummary] = useState<TelemetrySummary>(defaultTelemetrySummary)
  const [funnelHandoffs, setFunnelHandoffs] = useState<FunnelHandoffMetrics[]>([])
  const [crossTeamActivity, setCrossTeamActivity] = useState<CrossTeamActivity[]>([])
  const [dataSource, setDataSource] = useState<'foundry' | 'mock'>('mock')

  // Filter state
  const [selectedQuarter, setSelectedQuarter] = useState('Q4 2024')
  const [selectedTeam, setSelectedTeam] = useState('All Teams')
  const [showQuarterDropdown, setShowQuarterDropdown] = useState(false)
  const [showTeamDropdown, setShowTeamDropdown] = useState(false)
  const [refreshKey, setRefreshKey] = useState(0)

  // Fetch data on mount and when filters change
  useEffect(() => {
    async function loadData() {
      setLoading(true)
      setError(null)

      // Check server to accurately determine data source
      const source = await getDataSourceAsync()
      setDataSource(source)

      // Build filters object from current state
      const filters: DashboardFilters = {
        quarter: selectedQuarter,
        team: selectedTeam,
      }

      try {
        const [kpis, trend, stuck, health, bottlenecks, losses, hygiene, alerts, trends, predictions, indicators, telemetry, handoffs, teamActivity, velocity, funnel] = await Promise.all([
          getDashboardKPIs(filters),
          getForecastTrend(filters),
          getStuckDeals(filters),
          getHealthDistribution(filters),
          getProcessBottlenecks(filters),
          getCompetitiveLosses(filters),
          getHygieneSummary(filters),
          getHygieneAlerts(filters),
          getHygieneTrends(),
          getDealPredictions(filters),
          getLeadingIndicatorsSummary(filters),
          getTelemetrySummary(filters),
          getFunnelHandoffs(filters),
          getCrossTeamActivity(filters),
          getStageVelocity(filters),
          getPipelineFunnel(filters),
        ])

        setDashboardKPIs(kpis)
        setForecastTrend(trend)
        setStuckDeals(stuck)
        setHealthDistribution(health)
        setProcessBottlenecks(bottlenecks)
        setCompetitiveLosses(losses)
        setHygieneSummary(hygiene)
        setHygieneAlerts(alerts)
        setHygieneTrends(trends)
        setDealPredictions(predictions)
        setLeadingIndicators(indicators)
        setTelemetrySummary(telemetry)
        setFunnelHandoffs(handoffs)
        setCrossTeamActivity(teamActivity)
        setStageVelocity(velocity)
        setPipelineFunnel(funnel)
      } catch (err) {
        console.error('Error loading dashboard data:', err)
        setError(err instanceof Error ? err.message : 'Failed to load dashboard data')
      } finally {
        setLoading(false)
      }
    }

    loadData()
  }, [refreshKey, selectedQuarter, selectedTeam]) // Refresh when filters change

  const handleRefresh = () => {
    setRefreshKey((k) => k + 1)
  }

  // Show loading skeleton on initial load
  if (loading && dashboardKPIs.quarterTarget === 0) {
    return (
      <div className="min-h-screen bg-bg-primary">
        <header className="border-b border-border-subtle bg-bg-secondary/50 backdrop-blur-sm sticky top-0 z-50">
          <div className="max-w-[1600px] mx-auto px-6 py-4">
            <div className="flex items-center justify-between">
              <div>
                <h1 className="text-xl font-semibold text-text-primary">Pipeline Health Dashboard</h1>
                <p className="text-sm text-text-muted flex items-center gap-2">
                  RevOps Hub • Loading...
                  <Loader2 className="w-3 h-3 animate-spin" />
                </p>
              </div>
            </div>
          </div>
        </header>
        <DashboardSkeleton />
      </div>
    )
  }

  // Show error state
  if (error) {
    return (
      <div className="min-h-screen bg-bg-primary">
        <header className="border-b border-border-subtle bg-bg-secondary/50 backdrop-blur-sm sticky top-0 z-50">
          <div className="max-w-[1600px] mx-auto px-6 py-4">
            <div className="flex items-center justify-between">
              <div>
                <h1 className="text-xl font-semibold text-text-primary">Pipeline Health Dashboard</h1>
                <p className="text-sm text-text-muted">RevOps Hub • {selectedQuarter}</p>
              </div>
            </div>
          </div>
        </header>
        <div className="max-w-[1600px] mx-auto px-6 py-8">
          <ErrorState
            title="Failed to load dashboard"
            message={error}
            onRetry={handleRefresh}
            variant="full"
          />
        </div>
      </div>
    )
  }

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
              <p className="text-sm text-text-muted flex items-center gap-2">
                RevOps Hub • {selectedQuarter}{selectedTeam !== 'All Teams' && ` • ${selectedTeam}`}
                {loading && <Loader2 className="w-3 h-3 animate-spin" />}
                <span className={cn(
                  'px-1.5 py-0.5 rounded text-xs',
                  dataSource === 'foundry' ? 'bg-green-500/20 text-green-400' : 'bg-yellow-500/20 text-yellow-400'
                )}>
                  {dataSource === 'foundry' ? 'Live' : 'Demo'}
                </span>
              </p>
            </div>

            <div className="flex items-center gap-4">
              {/* Quarter selector */}
              <div className="relative">
                <button
                  onClick={() => {
                    setShowQuarterDropdown(!showQuarterDropdown)
                    setShowTeamDropdown(false)
                  }}
                  className="flex items-center gap-2 px-3 py-2 bg-bg-elevated rounded-lg text-sm text-text-secondary hover:bg-bg-interactive transition-colors"
                >
                  {selectedQuarter}
                  <ChevronDown className={cn("w-4 h-4 transition-transform", showQuarterDropdown && "rotate-180")} />
                </button>
                {showQuarterDropdown && (
                  <div className="absolute top-full mt-2 right-0 w-32 bg-bg-elevated border border-border-subtle rounded-lg shadow-xl z-50 overflow-hidden">
                    {QUARTERS.map((quarter) => (
                      <button
                        key={quarter}
                        onClick={() => {
                          setSelectedQuarter(quarter)
                          setShowQuarterDropdown(false)
                        }}
                        className={cn(
                          "w-full px-3 py-2 text-left text-sm hover:bg-bg-interactive transition-colors",
                          selectedQuarter === quarter && "bg-accent-blue/10 text-accent-blue"
                        )}
                      >
                        {quarter}
                      </button>
                    ))}
                  </div>
                )}
              </div>

              {/* Team selector */}
              <div className="relative">
                <button
                  onClick={() => {
                    setShowTeamDropdown(!showTeamDropdown)
                    setShowQuarterDropdown(false)
                  }}
                  className="flex items-center gap-2 px-3 py-2 bg-bg-elevated rounded-lg text-sm text-text-secondary hover:bg-bg-interactive transition-colors"
                >
                  {selectedTeam}
                  <ChevronDown className={cn("w-4 h-4 transition-transform", showTeamDropdown && "rotate-180")} />
                </button>
                {showTeamDropdown && (
                  <div className="absolute top-full mt-2 right-0 w-32 bg-bg-elevated border border-border-subtle rounded-lg shadow-xl z-50 overflow-hidden">
                    {TEAMS.map((team) => (
                      <button
                        key={team}
                        onClick={() => {
                          setSelectedTeam(team)
                          setShowTeamDropdown(false)
                        }}
                        className={cn(
                          "w-full px-3 py-2 text-left text-sm hover:bg-bg-interactive transition-colors",
                          selectedTeam === team && "bg-accent-blue/10 text-accent-blue"
                        )}
                      >
                        {team}
                      </button>
                    ))}
                  </div>
                )}
              </div>

              {/* Actions */}
              <button
                onClick={handleRefresh}
                disabled={loading}
                className={cn(
                  "p-2 rounded-lg hover:bg-bg-elevated text-text-muted hover:text-text-primary transition-colors",
                  loading && "animate-spin text-accent-blue"
                )}
                title="Refresh data"
              >
                <RefreshCw className="w-5 h-5" />
              </button>
              <button className="p-2 rounded-lg hover:bg-bg-elevated text-text-muted hover:text-text-primary transition-colors">
                <Bell className="w-5 h-5" />
              </button>

              {/* Nav to QBR */}
              <Link
                href="/qbr"
                className="px-4 py-2 bg-accent-purple hover:bg-purple-600 rounded-lg text-sm font-medium text-white transition-colors"
              >
                QBR Pack
              </Link>

              {/* Nav to Territory Planning */}
              <Link
                href="/territory"
                className="px-4 py-2 bg-teal-500 hover:bg-teal-600 rounded-lg text-sm font-medium text-white transition-colors"
              >
                Territory
              </Link>

              {/* Nav to Capacity Planning */}
              <Link
                href="/capacity"
                className="px-4 py-2 bg-indigo-500 hover:bg-indigo-600 rounded-lg text-sm font-medium text-white transition-colors"
              >
                Capacity
              </Link>

              {/* Nav to Scenario Modeling */}
              <Link
                href="/scenarios"
                className="px-4 py-2 bg-orange-500 hover:bg-orange-600 rounded-lg text-sm font-medium text-white transition-colors"
              >
                Scenarios
              </Link>

              {/* Nav to Customer Health */}
              <Link
                href="/customers"
                className="px-4 py-2 bg-emerald-500 hover:bg-emerald-600 rounded-lg text-sm font-medium text-white transition-colors"
              >
                Customers
              </Link>

              {/* Nav to Forecasting Hub */}
              <Link
                href="/forecast"
                className="px-4 py-2 bg-blue-500 hover:bg-blue-600 rounded-lg text-sm font-medium text-white transition-colors"
              >
                Forecast
              </Link>

              {/* Nav to Win/Loss Analysis */}
              <Link
                href="/winloss"
                className="px-4 py-2 bg-rose-500 hover:bg-rose-600 rounded-lg text-sm font-medium text-white transition-colors"
              >
                Win/Loss
              </Link>

              {/* Nav to Deal Desk */}
              <Link
                href="/dealdesk"
                className="px-4 py-2 bg-amber-500 hover:bg-amber-600 rounded-lg text-sm font-medium text-white transition-colors"
              >
                Deal Desk
              </Link>

              {/* Nav to Compensation */}
              <Link
                href="/compensation"
                className="px-4 py-2 bg-violet-500 hover:bg-violet-600 rounded-lg text-sm font-medium text-white transition-colors"
              >
                Comp
              </Link>

              {/* Nav to Data Quality */}
              <Link
                href="/data-quality"
                className="px-4 py-2 bg-cyan-500 hover:bg-cyan-600 rounded-lg text-sm font-medium text-white transition-colors"
              >
                Data Quality
              </Link>

              {/* Nav to Alerts */}
              <Link
                href="/alerts"
                className="px-4 py-2 bg-red-500 hover:bg-red-600 rounded-lg text-sm font-medium text-white transition-colors"
              >
                Alerts
              </Link>

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

        {/* Process Bottlenecks & Competitive Losses Row */}
        <div className="grid grid-cols-2 gap-6 mb-8">
          {/* Process Bottlenecks */}
          <div className="card">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-lg font-semibold text-text-primary">
                Process Bottlenecks
              </h2>
              <span className="text-xs text-text-muted">vs. benchmark</span>
            </div>
            <div className="space-y-3">
              {processBottlenecks.slice(0, 4).map((bottleneck) => (
                <div key={bottleneck.activity} className="flex items-center gap-3">
                  <div className="flex-shrink-0">
                    <AlertTriangle className={cn(
                      'w-4 h-4',
                      bottleneck.severity === 'CRITICAL' && 'text-status-critical',
                      bottleneck.severity === 'HIGH' && 'text-status-at-risk',
                      bottleneck.severity === 'MEDIUM' && 'text-status-monitor',
                    )} />
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center justify-between">
                      <span className="text-sm font-medium text-text-primary truncate">
                        {bottleneck.activity}
                      </span>
                      <span className={cn(
                        'text-xs px-1.5 py-0.5 rounded',
                        bottleneck.severity === 'CRITICAL' && 'bg-status-critical/20 text-status-critical',
                        bottleneck.severity === 'HIGH' && 'bg-status-at-risk/20 text-status-at-risk',
                        bottleneck.severity === 'MEDIUM' && 'bg-status-monitor/20 text-status-monitor',
                      )}>
                        {bottleneck.severity}
                      </span>
                    </div>
                    <div className="flex items-center gap-2 mt-1">
                      <div className="flex-1 h-1.5 bg-bg-elevated rounded-full overflow-hidden">
                        <div
                          className={cn(
                            'h-full rounded-full',
                            bottleneck.severity === 'CRITICAL' && 'bg-status-critical',
                            bottleneck.severity === 'HIGH' && 'bg-status-at-risk',
                            bottleneck.severity === 'MEDIUM' && 'bg-status-monitor',
                          )}
                          style={{ width: `${Math.min(bottleneck.delayRatio * 40, 100)}%` }}
                        />
                      </div>
                      <span className="text-xs text-text-muted whitespace-nowrap">
                        {bottleneck.avgDuration}d / {bottleneck.benchmarkDays}d
                      </span>
                    </div>
                    <p className="text-xs text-text-muted mt-1 truncate">
                      {formatCurrency(bottleneck.bottleneckRevenue)} at risk • {bottleneck.bottleneckCount} deals
                    </p>
                  </div>
                </div>
              ))}
            </div>
            {processBottlenecks.length > 0 && processBottlenecks[0].severity === 'CRITICAL' && (
              <div className="mt-4 p-3 bg-status-critical/10 border border-status-critical/20 rounded-lg">
                <p className="text-xs text-text-secondary">
                  <span className="font-medium text-status-critical">Action Required:</span>{' '}
                  {processBottlenecks[0].recommendedAction}
                </p>
              </div>
            )}
          </div>

          {/* Competitive Losses */}
          <div className="card">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-lg font-semibold text-text-primary">
                Competitive Losses
              </h2>
              <span className="text-xs text-text-muted">This Quarter</span>
            </div>
            <div className="space-y-4">
              {competitiveLosses.map((loss) => (
                <div key={loss.competitor} className="flex items-start gap-3">
                  <div className="w-8 h-8 rounded-lg bg-status-at-risk/20 flex items-center justify-center flex-shrink-0">
                    <span className="text-sm font-medium text-status-at-risk">
                      {loss.dealsLost}
                    </span>
                  </div>
                  <div className="flex-1">
                    <div className="flex items-center justify-between">
                      <span className="text-sm font-medium text-text-primary">
                        {loss.competitor}
                      </span>
                      <span className="text-sm font-mono tabular-nums text-status-at-risk">
                        -{formatCurrency(loss.revenueLost)}
                      </span>
                    </div>
                    <div className="flex flex-wrap gap-1 mt-1">
                      {loss.lossReasons.slice(0, 3).map((reason) => (
                        <span
                          key={reason}
                          className="text-xs px-1.5 py-0.5 bg-bg-elevated rounded text-text-muted"
                        >
                          {reason}
                        </span>
                      ))}
                    </div>
                  </div>
                </div>
              ))}
            </div>
            <div className="mt-4 pt-4 border-t border-border-subtle">
              <div className="flex items-center justify-between text-sm">
                <span className="text-text-muted">Total Lost</span>
                <span className="font-mono tabular-nums text-status-at-risk font-medium">
                  -{formatCurrency(competitiveLosses.reduce((sum, l) => sum + l.revenueLost, 0))}
                </span>
              </div>
              <p className="text-xs text-text-muted mt-2">
                Primary competitor: TechRival (Mid-Market focus, price competitive)
              </p>
            </div>
          </div>
        </div>

        {/* Pipeline Hygiene Section */}
        <div className="card mb-8">
          <div className="flex items-center justify-between mb-6">
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 rounded-lg bg-accent-purple/20 flex items-center justify-center">
                <Shield className="w-5 h-5 text-accent-purple" />
              </div>
              <div>
                <h2 className="text-lg font-semibold text-text-primary">
                  Pipeline Hygiene
                </h2>
                <p className="text-sm text-text-muted">
                  Data quality and process compliance monitoring
                </p>
              </div>
            </div>
            <div className="flex items-center gap-4">
              <div className="text-right">
                <div className="text-2xl font-semibold text-text-primary">
                  {Math.round(hygieneSummary.avgHygieneScore)}
                </div>
                <div className="text-xs text-text-muted">Hygiene Score</div>
              </div>
              <div className={cn(
                'px-3 py-1.5 rounded-lg text-sm font-medium',
                hygieneSummary.avgHygieneScore >= 80 && 'bg-status-healthy/20 text-status-healthy',
                hygieneSummary.avgHygieneScore >= 60 && hygieneSummary.avgHygieneScore < 80 && 'bg-status-monitor/20 text-status-monitor',
                hygieneSummary.avgHygieneScore < 60 && 'bg-status-at-risk/20 text-status-at-risk',
              )}>
                {hygieneSummary.avgHygieneScore >= 80 ? 'Good' : hygieneSummary.avgHygieneScore >= 60 ? 'Needs Work' : 'At Risk'}
              </div>
            </div>
          </div>

          {/* Hygiene Metrics Grid */}
          <div className="grid grid-cols-4 gap-4 mb-6">
            <div className="p-4 bg-bg-elevated rounded-lg">
              <div className="flex items-center gap-2 mb-2">
                <XCircle className="w-4 h-4 text-status-critical" />
                <span className="text-sm text-text-muted">Gone Dark</span>
              </div>
              <div className="text-2xl font-semibold text-text-primary">
                {hygieneSummary.goneDarkCount}
              </div>
              <div className="text-xs text-text-muted mt-1">
                {formatCurrency(hygieneSummary.criticalRevenueAtRisk)} at risk
              </div>
            </div>
            <div className="p-4 bg-bg-elevated rounded-lg">
              <div className="flex items-center gap-2 mb-2">
                <Users className="w-4 h-4 text-status-at-risk" />
                <span className="text-sm text-text-muted">Single-Threaded</span>
              </div>
              <div className="text-2xl font-semibold text-text-primary">
                {hygieneSummary.singleThreadedCount}
              </div>
              <div className="text-xs text-text-muted mt-1">
                Need multi-threading
              </div>
            </div>
            <div className="p-4 bg-bg-elevated rounded-lg">
              <div className="flex items-center gap-2 mb-2">
                <Calendar className="w-4 h-4 text-status-monitor" />
                <span className="text-sm text-text-muted">Stale Close Date</span>
              </div>
              <div className="text-2xl font-semibold text-text-primary">
                {hygieneSummary.staleCloseDateCount}
              </div>
              <div className="text-xs text-text-muted mt-1">
                Need date update
              </div>
            </div>
            <div className="p-4 bg-bg-elevated rounded-lg">
              <div className="flex items-center gap-2 mb-2">
                <Clock className="w-4 h-4 text-status-monitor" />
                <span className="text-sm text-text-muted">Missing Next Steps</span>
              </div>
              <div className="text-2xl font-semibold text-text-primary">
                {hygieneSummary.missingNextStepsCount}
              </div>
              <div className="text-xs text-text-muted mt-1">
                No activity planned
              </div>
            </div>
          </div>

          {/* Two Column: Alerts List & Trend */}
          <div className="grid grid-cols-2 gap-6">
            {/* Top Hygiene Alerts */}
            <div>
              <h3 className="text-sm font-medium text-text-primary mb-3">Priority Alerts</h3>
              <div className="space-y-2">
                {hygieneAlerts.slice(0, 5).map((alert) => (
                  <div
                    key={alert.opportunityId}
                    className="flex items-center gap-3 p-3 bg-bg-elevated rounded-lg hover:bg-bg-interactive transition-colors cursor-pointer"
                    onClick={() => {
                      const deal = stuckDeals.find(d => d.id === alert.opportunityId)
                      if (deal) setSelectedDeal(deal)
                    }}
                  >
                    <div className={cn(
                      'w-2 h-2 rounded-full flex-shrink-0',
                      alert.alertSeverity === 'CRITICAL' && 'bg-status-critical',
                      alert.alertSeverity === 'HIGH' && 'bg-status-at-risk',
                      alert.alertSeverity === 'MEDIUM' && 'bg-status-monitor',
                      alert.alertSeverity === 'LOW' && 'bg-status-healthy',
                    )} />
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center justify-between">
                        <span className="text-sm font-medium text-text-primary truncate">
                          {alert.accountName}
                        </span>
                        <span className="text-xs font-mono text-text-muted">
                          {formatCurrency(alert.amount)}
                        </span>
                      </div>
                      <div className="flex items-center gap-2 mt-0.5">
                        <span className={cn(
                          'text-xs px-1.5 py-0.5 rounded',
                          alert.alertSeverity === 'CRITICAL' && 'bg-status-critical/20 text-status-critical',
                          alert.alertSeverity === 'HIGH' && 'bg-status-at-risk/20 text-status-at-risk',
                          alert.alertSeverity === 'MEDIUM' && 'bg-status-monitor/20 text-status-monitor',
                        )}>
                          {alert.primaryAlert.replace(/_/g, ' ')}
                        </span>
                        <span className="text-xs text-text-muted truncate">
                          {alert.recommendedAction}
                        </span>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Hygiene Trend */}
            <div>
              <h3 className="text-sm font-medium text-text-primary mb-3">8-Week Trend</h3>
              <div className="h-[180px] flex items-end gap-2">
                {hygieneTrends.slice(-8).map((point, idx) => (
                  <div key={point.snapshotDate} className="flex-1 flex flex-col items-center">
                    <div className="w-full flex flex-col items-center gap-1">
                      <span className="text-xs text-text-muted">{Math.round(point.avgHygieneScore)}</span>
                      <div
                        className={cn(
                          'w-full rounded-t transition-all',
                          point.avgHygieneScore >= 70 && 'bg-status-healthy',
                          point.avgHygieneScore >= 60 && point.avgHygieneScore < 70 && 'bg-status-monitor',
                          point.avgHygieneScore < 60 && 'bg-status-at-risk',
                        )}
                        style={{ height: `${point.avgHygieneScore * 1.2}px` }}
                      />
                    </div>
                    <span className="text-xs text-text-muted mt-2">W{point.weekNumber - 38}</span>
                  </div>
                ))}
              </div>
              <div className="flex items-center justify-between mt-4 pt-4 border-t border-border-subtle">
                <div className="flex items-center gap-2">
                  <TrendingUp className="w-4 h-4 text-status-healthy" />
                  <span className="text-sm text-text-secondary">
                    +{((hygieneTrends[hygieneTrends.length - 1]?.avgHygieneScore || 0) - (hygieneTrends[0]?.avgHygieneScore || 0)).toFixed(1)} pts improvement
                  </span>
                </div>
                <span className="text-xs text-text-muted">
                  Alerts: {hygieneTrends[0]?.totalAlerts || 0} → {hygieneTrends[hygieneTrends.length - 1]?.totalAlerts || 0}
                </span>
              </div>
            </div>
          </div>

          {/* Summary Footer */}
          <div className="mt-6 pt-6 border-t border-border-subtle grid grid-cols-4 gap-4">
            <div className="flex items-center gap-2">
              <CheckCircle2 className="w-4 h-4 text-status-healthy" />
              <span className="text-sm text-text-muted">Clean:</span>
              <span className="text-sm font-medium text-text-primary">{hygieneSummary.cleanCount} deals</span>
            </div>
            <div className="flex items-center gap-2">
              <AlertTriangle className="w-4 h-4 text-status-monitor" />
              <span className="text-sm text-text-muted">Minor Issues:</span>
              <span className="text-sm font-medium text-text-primary">{hygieneSummary.minorIssuesCount} deals</span>
            </div>
            <div className="flex items-center gap-2">
              <AlertTriangle className="w-4 h-4 text-status-at-risk" />
              <span className="text-sm text-text-muted">Needs Attention:</span>
              <span className="text-sm font-medium text-text-primary">{hygieneSummary.needsAttentionCount} deals</span>
            </div>
            <div className="flex items-center gap-2">
              <XCircle className="w-4 h-4 text-status-critical" />
              <span className="text-sm text-text-muted">Critical:</span>
              <span className="text-sm font-medium text-text-primary">{hygieneSummary.criticalHygieneCount} deals</span>
            </div>
          </div>
        </div>

        {/* AI Predictions Section */}
        <div className="card mb-8">
          <div className="flex items-center justify-between mb-6">
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 rounded-lg bg-accent-blue/20 flex items-center justify-center">
                <Brain className="w-5 h-5 text-accent-blue" />
              </div>
              <div>
                <h2 className="text-lg font-semibold text-text-primary">
                  AI Deal Predictions
                </h2>
                <p className="text-sm text-text-muted">
                  ML-powered win probability and slip risk analysis
                </p>
              </div>
            </div>
            <div className="flex items-center gap-4">
              <div className="text-right">
                <div className="text-2xl font-semibold text-text-primary">
                  {formatCurrency(leadingIndicators.probabilityWeightedPipeline)}
                </div>
                <div className="text-xs text-text-muted">Weighted Pipeline</div>
              </div>
              <div className={cn(
                'px-3 py-1.5 rounded-lg text-sm font-medium',
                leadingIndicators.forecastConfidence === 'High' && 'bg-status-healthy/20 text-status-healthy',
                leadingIndicators.forecastConfidence === 'Medium' && 'bg-status-monitor/20 text-status-monitor',
                leadingIndicators.forecastConfidence === 'Low' && 'bg-status-at-risk/20 text-status-at-risk',
              )}>
                {leadingIndicators.forecastConfidence} Confidence
              </div>
            </div>
          </div>

          {/* Prediction Summary Cards */}
          <div className="grid grid-cols-4 gap-4 mb-6">
            <div className="p-4 bg-bg-elevated rounded-lg">
              <div className="flex items-center gap-2 mb-2">
                <Target className="w-4 h-4 text-status-healthy" />
                <span className="text-sm text-text-muted">High Probability</span>
              </div>
              <div className="text-2xl font-semibold text-text-primary">
                {leadingIndicators.highProbCount}
              </div>
              <div className="text-xs text-text-muted mt-1">
                {formatCurrency(leadingIndicators.highProbAmount)} pipeline
              </div>
            </div>
            <div className="p-4 bg-bg-elevated rounded-lg">
              <div className="flex items-center gap-2 mb-2">
                <Zap className="w-4 h-4 text-status-monitor" />
                <span className="text-sm text-text-muted">Medium Probability</span>
              </div>
              <div className="text-2xl font-semibold text-text-primary">
                {leadingIndicators.mediumProbCount}
              </div>
              <div className="text-xs text-text-muted mt-1">
                {formatCurrency(leadingIndicators.mediumProbAmount)} pipeline
              </div>
            </div>
            <div className="p-4 bg-bg-elevated rounded-lg">
              <div className="flex items-center gap-2 mb-2">
                <AlertTriangle className="w-4 h-4 text-status-at-risk" />
                <span className="text-sm text-text-muted">Low Probability</span>
              </div>
              <div className="text-2xl font-semibold text-text-primary">
                {leadingIndicators.lowProbCount}
              </div>
              <div className="text-xs text-text-muted mt-1">
                {formatCurrency(leadingIndicators.lowProbAmount)} pipeline
              </div>
            </div>
            <div className="p-4 bg-bg-elevated rounded-lg border border-status-critical/30">
              <div className="flex items-center gap-2 mb-2">
                <ArrowDownRight className="w-4 h-4 text-status-critical" />
                <span className="text-sm text-text-muted">Slip Risk</span>
              </div>
              <div className="text-2xl font-semibold text-status-critical">
                {formatCurrency(leadingIndicators.totalSlipRiskAmount)}
              </div>
              <div className="text-xs text-text-muted mt-1">
                {leadingIndicators.highSlipRiskCount} deals at high risk
              </div>
            </div>
          </div>

          {/* Two Column: Deal Lists */}
          <div className="grid grid-cols-2 gap-6">
            {/* Top Win Probability Deals */}
            <div>
              <h3 className="text-sm font-medium text-text-primary mb-3 flex items-center gap-2">
                <ArrowUpRight className="w-4 h-4 text-status-healthy" />
                Highest Win Probability
              </h3>
              <div className="space-y-2">
                {dealPredictions
                  .filter(d => d.winProbabilityTier === 'High')
                  .slice(0, 5)
                  .map((deal) => (
                  <div
                    key={deal.opportunityId}
                    className="flex items-center gap-3 p-3 bg-bg-elevated rounded-lg hover:bg-bg-interactive transition-colors cursor-pointer"
                    onClick={() => {
                      const stuckDeal = stuckDeals.find(d => d.id === deal.opportunityId)
                      if (stuckDeal) setSelectedDeal(stuckDeal)
                    }}
                  >
                    <div className="flex-shrink-0 w-12 h-12 rounded-lg bg-status-healthy/20 flex items-center justify-center">
                      <span className="text-lg font-semibold text-status-healthy">
                        {Math.round(deal.winProbability * 100)}%
                      </span>
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center justify-between">
                        <span className="text-sm font-medium text-text-primary truncate">
                          {deal.accountName}
                        </span>
                        <span className="text-xs font-mono text-text-muted">
                          {formatCurrency(deal.weightedAmount)}
                        </span>
                      </div>
                      <div className="flex items-center gap-2 mt-0.5">
                        <span className="text-xs text-text-muted truncate">
                          {deal.stageName}
                        </span>
                        <span className={cn(
                          'text-xs px-1.5 py-0.5 rounded',
                          deal.predictionConfidence === 'High' && 'bg-status-healthy/20 text-status-healthy',
                          deal.predictionConfidence === 'Medium' && 'bg-status-monitor/20 text-status-monitor',
                          deal.predictionConfidence === 'Low' && 'bg-status-at-risk/20 text-status-at-risk',
                        )}>
                          {deal.predictionConfidence} confidence
                        </span>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* High Slip Risk Deals */}
            <div>
              <h3 className="text-sm font-medium text-text-primary mb-3 flex items-center gap-2">
                <ArrowDownRight className="w-4 h-4 text-status-critical" />
                High Slip Risk
              </h3>
              <div className="space-y-2">
                {dealPredictions
                  .filter(d => d.slipRiskTier === 'High')
                  .slice(0, 5)
                  .map((deal) => (
                  <div
                    key={deal.opportunityId}
                    className="flex items-center gap-3 p-3 bg-bg-elevated rounded-lg border border-status-critical/20 hover:bg-bg-interactive transition-colors cursor-pointer"
                    onClick={() => {
                      const stuckDeal = stuckDeals.find(d => d.id === deal.opportunityId)
                      if (stuckDeal) setSelectedDeal(stuckDeal)
                    }}
                  >
                    <div className="flex-shrink-0 w-12 h-12 rounded-lg bg-status-critical/20 flex items-center justify-center">
                      <span className="text-lg font-semibold text-status-critical">
                        {Math.round(deal.slipRiskScore * 100)}%
                      </span>
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center justify-between">
                        <span className="text-sm font-medium text-text-primary truncate">
                          {deal.accountName}
                        </span>
                        <span className="text-xs font-mono text-status-critical">
                          {formatCurrency(deal.slipRiskAmount)} at risk
                        </span>
                      </div>
                      <div className="flex items-center gap-2 mt-0.5">
                        <span className="text-xs text-text-muted truncate">
                          {deal.predictionSummary}
                        </span>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Summary Footer */}
          <div className="mt-6 pt-6 border-t border-border-subtle">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-6">
                <div className="flex items-center gap-2">
                  <span className="text-sm text-text-muted">Avg Win Probability:</span>
                  <span className="text-sm font-medium text-text-primary">
                    {Math.round(leadingIndicators.avgWinProbability * 100)}%
                  </span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-sm text-text-muted">Activity Recency:</span>
                  <span className="text-sm font-medium text-text-primary">
                    {leadingIndicators.avgActivityRecency.toFixed(1)}d avg
                  </span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-sm text-text-muted">Stakeholder Score:</span>
                  <span className="text-sm font-medium text-text-primary">
                    {Math.round(leadingIndicators.avgStakeholderEngagement * 100)}%
                  </span>
                </div>
              </div>
              <div className="p-2 bg-accent-blue/10 border border-accent-blue/20 rounded-lg">
                <p className="text-xs text-accent-blue">
                  <span className="font-medium">Primary Risk:</span> {leadingIndicators.primaryRiskFactor}
                </p>
              </div>
            </div>
          </div>
        </div>

        {/* Cross-Functional Telemetry Section */}
        <div className="card mb-8">
          <div className="flex items-center justify-between mb-6">
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 rounded-lg bg-accent-teal/20 flex items-center justify-center">
                <GitBranch className="w-5 h-5 text-accent-teal" />
              </div>
              <div>
                <h2 className="text-lg font-semibold text-text-primary">
                  Cross-Functional Telemetry
                </h2>
                <p className="text-sm text-text-muted">
                  Marketing → Sales → CS handoff metrics and funnel health
                </p>
              </div>
            </div>
            <div className="flex items-center gap-4">
              <div className="text-right">
                <div className="text-2xl font-semibold text-text-primary">
                  {Math.round(telemetrySummary.mqlToSqlRate * 100)}%
                </div>
                <div className="text-xs text-text-muted">MQL→SQL Rate</div>
              </div>
              <div className={cn(
                'px-3 py-1.5 rounded-lg text-sm font-medium',
                telemetrySummary.overallHealth === 'HEALTHY' && 'bg-status-healthy/20 text-status-healthy',
                telemetrySummary.overallHealth === 'MONITOR' && 'bg-status-monitor/20 text-status-monitor',
                telemetrySummary.overallHealth === 'AT_RISK' && 'bg-status-at-risk/20 text-status-at-risk',
              )}>
                {telemetrySummary.overallHealth === 'HEALTHY' ? 'Healthy' : telemetrySummary.overallHealth === 'MONITOR' ? 'Monitor' : 'At Risk'}
              </div>
            </div>
          </div>

          {/* Telemetry Summary Cards */}
          <div className="grid grid-cols-4 gap-4 mb-6">
            <div className="p-4 bg-bg-elevated rounded-lg">
              <div className="flex items-center gap-2 mb-2">
                <BarChart3 className="w-4 h-4 text-accent-blue" />
                <span className="text-sm text-text-muted">Lead Volume</span>
              </div>
              <div className="text-2xl font-semibold text-text-primary">
                {telemetrySummary.totalLeads.toLocaleString()}
              </div>
              <div className="flex items-center gap-1 mt-1">
                <ArrowRight className="w-3 h-3 text-text-muted" />
                <span className="text-xs text-text-muted">
                  {telemetrySummary.totalMqls} MQL → {telemetrySummary.totalSqls} SQL
                </span>
              </div>
            </div>
            <div className="p-4 bg-bg-elevated rounded-lg">
              <div className="flex items-center gap-2 mb-2">
                <Timer className="w-4 h-4 text-status-monitor" />
                <span className="text-sm text-text-muted">Avg Response</span>
              </div>
              <div className="text-2xl font-semibold text-text-primary">
                {telemetrySummary.avgResponseDays.toFixed(1)}d
              </div>
              <div className={cn(
                'text-xs mt-1 px-1.5 py-0.5 rounded inline-block',
                telemetrySummary.speedToLeadHealth === 'HEALTHY' && 'bg-status-healthy/20 text-status-healthy',
                telemetrySummary.speedToLeadHealth === 'MONITOR' && 'bg-status-monitor/20 text-status-monitor',
                telemetrySummary.speedToLeadHealth === 'AT_RISK' && 'bg-status-at-risk/20 text-status-at-risk',
              )}>
                {Math.round(telemetrySummary.fastResponsePct * 100)}% within 1hr
              </div>
            </div>
            <div className="p-4 bg-bg-elevated rounded-lg">
              <div className="flex items-center gap-2 mb-2">
                <Users className="w-4 h-4 text-accent-purple" />
                <span className="text-sm text-text-muted">Team Alignment</span>
              </div>
              <div className="text-2xl font-semibold text-text-primary">
                {Math.round(telemetrySummary.fullEngagementPct * 100)}%
              </div>
              <div className={cn(
                'text-xs mt-1 px-1.5 py-0.5 rounded inline-block',
                telemetrySummary.teamAlignmentHealth === 'HEALTHY' && 'bg-status-healthy/20 text-status-healthy',
                telemetrySummary.teamAlignmentHealth === 'MONITOR' && 'bg-status-monitor/20 text-status-monitor',
                telemetrySummary.teamAlignmentHealth === 'AT_RISK' && 'bg-status-at-risk/20 text-status-at-risk',
              )}>
                Multi-team engaged
              </div>
            </div>
            <div className="p-4 bg-bg-elevated rounded-lg">
              <div className="flex items-center gap-2 mb-2">
                <TrendingUp className="w-4 h-4 text-status-healthy" />
                <span className="text-sm text-text-muted">Health Impact</span>
              </div>
              <div className="text-2xl font-semibold text-status-healthy">
                +{telemetrySummary.healthDelta.toFixed(1)}
              </div>
              <div className="text-xs text-text-muted mt-1">
                Full vs single engagement
              </div>
            </div>
          </div>

          {/* Two Column: Handoff Metrics & Team Activity */}
          <div className="grid grid-cols-2 gap-6">
            {/* Funnel Handoff Metrics */}
            <div>
              <h3 className="text-sm font-medium text-text-primary mb-3 flex items-center gap-2">
                <BarChart3 className="w-4 h-4 text-accent-blue" />
                Conversion by Source/Segment
              </h3>
              <div className="space-y-2">
                {funnelHandoffs.slice(0, 5).map((handoff, idx) => (
                  <div
                    key={`${handoff.leadSource}-${handoff.segment}-${idx}`}
                    className="flex items-center gap-3 p-3 bg-bg-elevated rounded-lg"
                  >
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center justify-between">
                        <span className="text-sm font-medium text-text-primary">
                          {handoff.leadSource} • {handoff.segment}
                        </span>
                        <span className="text-xs font-mono text-text-muted">
                          {handoff.totalLeads} leads
                        </span>
                      </div>
                      <div className="flex items-center gap-2 mt-1">
                        <div className="flex-1 h-1.5 bg-bg-primary rounded-full overflow-hidden">
                          <div
                            className="h-full bg-accent-blue rounded-full"
                            style={{ width: `${handoff.overallConversionRate * 100 * 10}%` }}
                          />
                        </div>
                        <span className="text-xs text-text-muted whitespace-nowrap">
                          {(handoff.overallConversionRate * 100).toFixed(1)}% conv
                        </span>
                      </div>
                      <div className="flex items-center gap-2 mt-1">
                        <span className={cn(
                          'text-xs px-1.5 py-0.5 rounded',
                          handoff.mqlToSqlHealth === 'HEALTHY' && 'bg-status-healthy/20 text-status-healthy',
                          handoff.mqlToSqlHealth === 'MONITOR' && 'bg-status-monitor/20 text-status-monitor',
                          handoff.mqlToSqlHealth === 'AT_RISK' && 'bg-status-at-risk/20 text-status-at-risk',
                        )}>
                          MQL→SQL: {Math.round(handoff.mqlToSqlRate * 100)}%
                        </span>
                        <span className={cn(
                          'text-xs px-1.5 py-0.5 rounded',
                          handoff.responseTimeHealth === 'HEALTHY' && 'bg-status-healthy/20 text-status-healthy',
                          handoff.responseTimeHealth === 'MONITOR' && 'bg-status-monitor/20 text-status-monitor',
                          handoff.responseTimeHealth === 'AT_RISK' && 'bg-status-at-risk/20 text-status-at-risk',
                        )}>
                          {handoff.avgResponseTimeDays.toFixed(1)}d response
                        </span>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Cross-Team Activity */}
            <div>
              <h3 className="text-sm font-medium text-text-primary mb-3 flex items-center gap-2">
                <Users className="w-4 h-4 text-accent-purple" />
                Multi-Team Engagement
              </h3>
              <div className="space-y-2">
                {crossTeamActivity.slice(0, 5).map((activity) => (
                  <div
                    key={activity.opportunityId}
                    className="flex items-center gap-3 p-3 bg-bg-elevated rounded-lg"
                  >
                    <div className={cn(
                      'w-2 h-2 rounded-full flex-shrink-0',
                      activity.multiTeamEngagement === 'FULL' && 'bg-status-healthy',
                      activity.multiTeamEngagement === 'PARTIAL' && 'bg-status-monitor',
                      activity.multiTeamEngagement === 'SINGLE' && 'bg-status-at-risk',
                    )} />
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center justify-between">
                        <span className="text-sm font-medium text-text-primary truncate">
                          {activity.accountName}
                        </span>
                        <span className="text-xs font-mono text-text-muted">
                          {formatCurrency(activity.amount)}
                        </span>
                      </div>
                      <div className="flex items-center gap-3 mt-1">
                        <div className="flex items-center gap-1">
                          <span className="text-xs text-accent-blue">{activity.marketingTouches}</span>
                          <span className="text-xs text-text-muted">Mktg</span>
                        </div>
                        <div className="flex items-center gap-1">
                          <span className="text-xs text-accent-purple">{activity.salesTouches}</span>
                          <span className="text-xs text-text-muted">Sales</span>
                        </div>
                        <div className="flex items-center gap-1">
                          <span className="text-xs text-accent-teal">{activity.csTouches}</span>
                          <span className="text-xs text-text-muted">CS</span>
                        </div>
                        <span className={cn(
                          'text-xs px-1.5 py-0.5 rounded ml-auto',
                          activity.multiTeamEngagement === 'FULL' && 'bg-status-healthy/20 text-status-healthy',
                          activity.multiTeamEngagement === 'PARTIAL' && 'bg-status-monitor/20 text-status-monitor',
                          activity.multiTeamEngagement === 'SINGLE' && 'bg-status-at-risk/20 text-status-at-risk',
                        )}>
                          {activity.multiTeamEngagement}
                        </span>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Recommendation Footer */}
          <div className="mt-6 pt-6 border-t border-border-subtle">
            <div className="flex items-start gap-3 p-4 bg-accent-teal/10 border border-accent-teal/20 rounded-lg">
              <AlertTriangle className="w-5 h-5 text-accent-teal flex-shrink-0 mt-0.5" />
              <div>
                <p className="text-sm font-medium text-text-primary">
                  Bottleneck: {telemetrySummary.primaryBottleneck}
                </p>
                <p className="text-sm text-text-secondary mt-1">
                  {telemetrySummary.recommendation}
                </p>
              </div>
            </div>
          </div>
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
