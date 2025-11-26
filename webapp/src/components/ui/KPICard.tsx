'use client'

import { cn, formatCurrency, formatPercent, formatDelta } from '@/lib/utils'
import { TrendingUp, TrendingDown, Minus } from 'lucide-react'

interface KPICardProps {
  title: string
  value: number | string
  format?: 'currency' | 'percent' | 'number' | 'string'
  subtitle?: string
  trend?: {
    value: number
    label?: string
    inverted?: boolean // true if lower is better
  }
  comparison?: {
    value: number | string
    label: string
  }
  progress?: {
    current: number
    target: number
    color?: string
  }
  className?: string
}

export function KPICard({
  title,
  value,
  format = 'string',
  subtitle,
  trend,
  comparison,
  progress,
  className,
}: KPICardProps) {
  const formatValue = (val: number | string): string => {
    if (typeof val === 'string') return val
    switch (format) {
      case 'currency':
        return formatCurrency(val)
      case 'percent':
        return formatPercent(val)
      case 'number':
        return val.toLocaleString()
      default:
        return String(val)
    }
  }

  const getTrendIcon = () => {
    if (!trend) return null
    const isPositive = trend.inverted ? trend.value < 0 : trend.value > 0
    const isNegative = trend.inverted ? trend.value > 0 : trend.value < 0

    if (trend.value === 0) {
      return <Minus className="w-4 h-4 text-text-muted" />
    }
    if (isPositive) {
      return <TrendingUp className="w-4 h-4 text-status-healthy" />
    }
    if (isNegative) {
      return <TrendingDown className="w-4 h-4 text-status-critical" />
    }
  }

  const getTrendColor = () => {
    if (!trend) return ''
    const isPositive = trend.inverted ? trend.value < 0 : trend.value > 0
    const isNegative = trend.inverted ? trend.value > 0 : trend.value < 0

    if (isPositive) return 'text-status-healthy'
    if (isNegative) return 'text-status-critical'
    return 'text-text-muted'
  }

  const progressPercent = progress
    ? Math.min(Math.max((progress.current / progress.target) * 100, 0), 100)
    : 0

  return (
    <div className={cn('kpi-card group', className)}>
      {/* Header with title and trend */}
      <div className="flex items-center justify-between mb-3">
        <span className="kpi-label">{title}</span>
        {trend && (
          <div className={cn('flex items-center gap-1 text-sm', getTrendColor())}>
            {getTrendIcon()}
            <span className="font-medium">
              {formatDelta(Math.round(trend.value * 100), '%')}
            </span>
          </div>
        )}
      </div>

      {/* Main value */}
      <div className="mb-2">
        <span className="kpi-value tabular-nums">{formatValue(value)}</span>
      </div>

      {/* Progress bar */}
      {progress && (
        <div className="mb-3">
          <div className="progress-bar">
            <div
              className="progress-bar-fill"
              style={{
                width: `${progressPercent}%`,
                backgroundColor: progress.color || '#3b82f6',
              }}
            />
          </div>
        </div>
      )}

      {/* Subtitle or comparison */}
      <div className="flex items-center justify-between text-sm">
        {subtitle && <span className="text-text-muted">{subtitle}</span>}
        {comparison && (
          <span className="text-text-secondary">
            {comparison.label}: {typeof comparison.value === 'number' ? formatValue(comparison.value) : comparison.value}
          </span>
        )}
        {trend?.label && !subtitle && (
          <span className="text-text-muted">{trend.label}</span>
        )}
      </div>
    </div>
  )
}
