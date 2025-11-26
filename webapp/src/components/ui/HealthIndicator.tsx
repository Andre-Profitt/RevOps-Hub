'use client'

import { cn, getHealthCategory, getHealthColor, getHealthLabel, formatDelta } from '@/lib/utils'

interface HealthIndicatorProps {
  score: number
  showBar?: boolean
  showLabel?: boolean
  showTrend?: boolean
  trend?: number
  size?: 'sm' | 'md' | 'lg'
  className?: string
}

export function HealthIndicator({
  score,
  showBar = false,
  showLabel = false,
  showTrend = false,
  trend,
  size = 'md',
  className,
}: HealthIndicatorProps) {
  const category = getHealthCategory(score)
  const color = getHealthColor(score)
  const label = getHealthLabel(score)

  const sizeClasses = {
    sm: {
      dot: 'w-2 h-2',
      score: 'text-sm',
      label: 'text-xs',
    },
    md: {
      dot: 'w-3 h-3',
      score: 'text-base',
      label: 'text-sm',
    },
    lg: {
      dot: 'w-4 h-4',
      score: 'text-lg',
      label: 'text-base',
    },
  }

  const sizes = sizeClasses[size]

  return (
    <div className={cn('flex flex-col gap-1', className)}>
      {/* Score with dot */}
      <div className="flex items-center gap-2">
        <span
          className={cn('rounded-full', sizes.dot)}
          style={{ backgroundColor: color }}
        />
        <span className={cn('font-mono font-semibold tabular-nums', sizes.score)}>
          {score}
        </span>
      </div>

      {/* Progress bar */}
      {showBar && (
        <div className="w-full h-1.5 bg-bg-elevated rounded-full overflow-hidden">
          <div
            className="h-full rounded-full transition-all duration-500"
            style={{
              width: `${score}%`,
              backgroundColor: color,
            }}
          />
        </div>
      )}

      {/* Label and trend */}
      {(showLabel || showTrend) && (
        <div className={cn('flex items-center gap-2', sizes.label, 'text-text-muted')}>
          {showLabel && <span>{label}</span>}
          {showLabel && showTrend && trend !== undefined && <span>•</span>}
          {showTrend && trend !== undefined && (
            <span className={trend >= 0 ? 'text-status-healthy' : 'text-status-critical'}>
              {formatDelta(trend)} this week
            </span>
          )}
        </div>
      )}
    </div>
  )
}

// Compact inline version for tables
export function HealthBadge({ score }: { score: number }) {
  const category = getHealthCategory(score)

  const badgeClasses = {
    healthy: 'badge-healthy',
    monitor: 'badge-monitor',
    'at-risk': 'badge-at-risk',
    critical: 'badge-critical',
  }

  return (
    <span className={cn('badge', badgeClasses[category])}>
      {score}
    </span>
  )
}
