'use client'

import { cn, formatCurrency } from '@/lib/utils'
import type { FunnelStage } from '@/types'

interface PipelineFunnelProps {
  data: FunnelStage[]
  className?: string
}

export function PipelineFunnel({ data, className }: PipelineFunnelProps) {
  const maxAmount = Math.max(...data.map((d) => d.amount))

  return (
    <div className={cn('space-y-2', className)}>
      {data.map((stage, index) => {
        const widthPercent = (stage.amount / maxAmount) * 100
        const nextStage = data[index + 1]
        const conversionRate = nextStage
          ? ((nextStage.amount / stage.amount) * 100).toFixed(0)
          : null

        return (
          <div key={stage.name} className="relative">
            {/* Funnel bar */}
            <div
              className="relative h-12 flex items-center justify-between px-4 rounded-lg transition-all duration-500 group cursor-pointer"
              style={{
                width: `${Math.max(widthPercent, 30)}%`,
                background: `linear-gradient(90deg, #3b82f6 0%, #1d4ed8 100%)`,
                marginLeft: `${(100 - Math.max(widthPercent, 30)) / 2}%`,
              }}
            >
              <span className="font-medium text-white text-sm">{stage.name}</span>
              <span className="font-mono text-white text-sm tabular-nums">
                {formatCurrency(stage.amount)}
              </span>

              {/* Hover tooltip */}
              <div className="absolute left-full ml-3 hidden group-hover:block z-10">
                <div className="bg-bg-elevated border border-border-default rounded-lg p-3 shadow-lg whitespace-nowrap">
                  <p className="text-sm font-medium text-text-primary">{stage.name}</p>
                  <p className="text-sm text-text-secondary">
                    {stage.count} deals • {formatCurrency(stage.amount)}
                  </p>
                  {stage.conversionRate && (
                    <p className="text-xs text-text-muted mt-1">
                      Conversion: {Math.round(stage.conversionRate * 100)}%
                    </p>
                  )}
                </div>
              </div>
            </div>

            {/* Conversion arrow */}
            {conversionRate && (
              <div className="flex items-center justify-center h-6 text-xs text-text-muted">
                <span className="px-2 py-0.5 bg-bg-elevated rounded">
                  ↓ {conversionRate}%
                </span>
              </div>
            )}
          </div>
        )
      })}
    </div>
  )
}

// Alternative: Horizontal stacked visualization
export function PipelineStack({ data, className }: PipelineFunnelProps) {
  const total = data.reduce((sum, stage) => sum + stage.amount, 0)

  const colors = [
    '#3b82f6', // blue
    '#6366f1', // indigo
    '#8b5cf6', // purple
    '#a855f7', // violet
    '#d946ef', // fuchsia
  ]

  return (
    <div className={cn('space-y-4', className)}>
      {/* Stacked bar */}
      <div className="h-10 flex rounded-lg overflow-hidden">
        {data.map((stage, i) => (
          <div
            key={stage.name}
            className="h-full flex items-center justify-center text-white text-xs font-medium transition-all duration-500 hover:opacity-90 cursor-pointer"
            style={{
              width: `${(stage.amount / total) * 100}%`,
              backgroundColor: colors[i % colors.length],
            }}
            title={`${stage.name}: ${formatCurrency(stage.amount)}`}
          >
            {(stage.amount / total) * 100 > 10 && formatCurrency(stage.amount)}
          </div>
        ))}
      </div>

      {/* Legend */}
      <div className="grid grid-cols-2 gap-2">
        {data.map((stage, i) => (
          <div key={stage.name} className="flex items-center gap-2">
            <div
              className="w-3 h-3 rounded"
              style={{ backgroundColor: colors[i % colors.length] }}
            />
            <span className="text-sm text-text-secondary truncate">{stage.name}</span>
            <span className="text-sm font-mono tabular-nums text-text-muted ml-auto">
              {formatCurrency(stage.amount)}
            </span>
          </div>
        ))}
      </div>
    </div>
  )
}
