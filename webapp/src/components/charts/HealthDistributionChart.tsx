'use client'

import { cn, formatCurrency } from '@/lib/utils'
import type { HealthDistribution } from '@/types'

interface HealthDistributionChartProps {
  data: HealthDistribution
  className?: string
}

export function HealthDistributionChart({
  data,
  className,
}: HealthDistributionChartProps) {
  const categories = [
    { key: 'healthy', label: 'Healthy', color: '#22c55e', ...data.healthy },
    { key: 'monitor', label: 'Monitor', color: '#eab308', ...data.monitor },
    { key: 'atRisk', label: 'At Risk', color: '#f97316', ...data.atRisk },
    { key: 'critical', label: 'Critical', color: '#ef4444', ...data.critical },
  ]

  const total = categories.reduce((sum, cat) => sum + cat.amount, 0)

  return (
    <div className={cn('space-y-4', className)}>
      {/* Stacked bar */}
      <div className="h-6 flex rounded-lg overflow-hidden">
        {categories.map((cat) => (
          <div
            key={cat.key}
            className="h-full transition-all duration-500"
            style={{
              width: `${cat.pct}%`,
              backgroundColor: cat.color,
            }}
            title={`${cat.label}: ${formatCurrency(cat.amount)} (${cat.pct}%)`}
          />
        ))}
      </div>

      {/* Legend */}
      <div className="space-y-2">
        {categories.map((cat) => (
          <div key={cat.key} className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <div
                className="w-3 h-3 rounded"
                style={{ backgroundColor: cat.color }}
              />
              <span className="text-sm text-text-secondary">{cat.label}</span>
            </div>
            <div className="flex items-center gap-4">
              <span className="text-sm font-mono tabular-nums text-text-primary">
                {cat.pct}%
              </span>
              <span className="text-sm font-mono tabular-nums text-text-muted w-20 text-right">
                {formatCurrency(cat.amount)}
              </span>
            </div>
          </div>
        ))}
      </div>

      {/* Trend */}
      {data.trend !== 0 && (
        <div className="pt-2 border-t border-border-subtle">
          <span className={cn(
            'text-sm',
            data.trend > 0 ? 'text-status-at-risk' : 'text-status-healthy'
          )}>
            {data.trend > 0 ? '⚠️' : '✓'} {Math.abs(data.trend)}% {data.trend > 0 ? 'more' : 'less'} at-risk vs last week
          </span>
        </div>
      )}
    </div>
  )
}
