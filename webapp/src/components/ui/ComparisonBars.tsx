'use client'

import { cn } from '@/lib/utils'
import type { DriverComparison } from '@/types'

interface ComparisonBarsProps {
  data: DriverComparison[]
  className?: string
}

export function ComparisonBars({ data, className }: ComparisonBarsProps) {
  const formatValue = (value: number, format: string): string => {
    switch (format) {
      case 'percent':
        return `${Math.round(value * 100)}%`
      case 'days':
        return `${value.toFixed(1)}`
      default:
        return value.toFixed(1)
    }
  }

  const calculateBarWidth = (value: number, max: number): number => {
    // Normalize based on the max value in the data
    return Math.min((value / max) * 100, 100)
  }

  return (
    <div className={cn('space-y-6', className)}>
      {/* Legend */}
      <div className="flex items-center gap-6 text-sm">
        <div className="flex items-center gap-2">
          <div className="w-3 h-3 rounded bg-accent-blue" />
          <span className="text-text-secondary">You</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-3 h-3 rounded bg-status-healthy" />
          <span className="text-text-secondary">Top 25%</span>
        </div>
      </div>

      {/* Metrics */}
      <div className="space-y-5">
        {data.map((metric) => {
          const maxValue = Math.max(metric.repValue, metric.topPerformerValue) * 1.2
          const repWidth = calculateBarWidth(metric.repValue, maxValue)
          const topWidth = calculateBarWidth(metric.topPerformerValue, maxValue)

          const isGood = metric.goodDirection === 'higher'
            ? metric.repValue >= metric.topPerformerValue
            : metric.repValue <= metric.topPerformerValue

          const gapPercent = Math.abs(
            ((metric.topPerformerValue - metric.repValue) / metric.topPerformerValue) * 100
          )

          return (
            <div key={metric.metricName} className="space-y-2">
              {/* Label row */}
              <div className="flex items-center justify-between">
                <span className="text-sm font-medium text-text-primary">
                  {metric.metricLabel}
                </span>
                {!isGood && gapPercent > 15 && (
                  <span className="text-xs text-status-at-risk">
                    {metric.gapImpact}
                  </span>
                )}
              </div>

              {/* Your value bar */}
              <div className="flex items-center gap-3">
                <div className="w-12 text-sm text-text-secondary">You</div>
                <div className="flex-1 relative">
                  <div className="h-3 bg-bg-elevated rounded-full overflow-hidden">
                    <div
                      className="h-full bg-accent-blue rounded-full transition-all duration-500"
                      style={{ width: `${repWidth}%` }}
                    />
                  </div>
                </div>
                <div className="w-12 text-right font-mono text-sm tabular-nums">
                  {formatValue(metric.repValue, metric.format)}
                </div>
              </div>

              {/* Top performer bar */}
              <div className="flex items-center gap-3">
                <div className="w-12 text-sm text-text-muted">Top</div>
                <div className="flex-1 relative">
                  <div className="h-3 bg-bg-elevated rounded-full overflow-hidden">
                    <div
                      className="h-full bg-status-healthy rounded-full transition-all duration-500"
                      style={{ width: `${topWidth}%` }}
                    />
                  </div>
                </div>
                <div className="w-12 text-right font-mono text-sm tabular-nums text-text-muted">
                  {formatValue(metric.topPerformerValue, metric.format)}
                </div>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
