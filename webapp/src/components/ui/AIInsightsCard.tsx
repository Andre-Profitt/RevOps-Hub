'use client'

import { cn, formatCurrency } from '@/lib/utils'
import { Bot, CheckCircle2, Zap, ArrowRight, RefreshCw, MessageSquare } from 'lucide-react'
import type { CoachingInsight } from '@/types'

interface AIInsightsCardProps {
  strengths: CoachingInsight[]
  improvements: CoachingInsight[]
  actions: CoachingInsight[]
  lastUpdated?: string
  onRefresh?: () => void
  onAskQuestion?: () => void
  className?: string
}

export function AIInsightsCard({
  strengths,
  improvements,
  actions,
  lastUpdated = '2 min ago',
  onRefresh,
  onAskQuestion,
  className,
}: AIInsightsCardProps) {
  return (
    <div className={cn('ai-card', className)}>
      {/* Header */}
      <div className="flex items-center justify-between mb-6 pb-4 border-b border-accent-purple/20">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-accent-purple/20 rounded-lg">
            <Bot className="w-5 h-5 text-accent-purple" />
          </div>
          <div>
            <h3 className="font-semibold text-text-primary">AI Analysis</h3>
            <p className="text-xs text-text-muted">Updated {lastUpdated}</p>
          </div>
        </div>
        {onRefresh && (
          <button
            onClick={onRefresh}
            className="p-2 rounded-lg hover:bg-bg-elevated text-text-muted hover:text-text-primary transition-colors"
          >
            <RefreshCw className="w-4 h-4" />
          </button>
        )}
      </div>

      {/* Strengths */}
      <div className="mb-6">
        <h4 className="flex items-center gap-2 text-sm font-medium text-status-healthy mb-3">
          <CheckCircle2 className="w-4 h-4" />
          WHAT'S WORKING
        </h4>
        <div className="space-y-3">
          {strengths.map((insight, i) => (
            <InsightItem
              key={i}
              icon={<CheckCircle2 className="w-4 h-4 text-status-healthy" />}
              insight={insight}
              variant="strength"
            />
          ))}
        </div>
      </div>

      {/* Improvements */}
      <div className="mb-6">
        <h4 className="flex items-center gap-2 text-sm font-medium text-status-monitor mb-3">
          <Zap className="w-4 h-4" />
          FOCUS AREAS
        </h4>
        <div className="space-y-3">
          {improvements.map((insight, i) => (
            <InsightItem
              key={i}
              icon={<Zap className="w-4 h-4 text-status-monitor" />}
              insight={insight}
              variant="improvement"
            />
          ))}
        </div>
      </div>

      {/* Priority Actions */}
      <div className="mb-6">
        <h4 className="flex items-center gap-2 text-sm font-medium text-accent-blue mb-3">
          <ArrowRight className="w-4 h-4" />
          THIS WEEK'S PRIORITIES
        </h4>
        <div className="space-y-2">
          {actions.map((insight, i) => (
            <ActionItem key={i} insight={insight} index={i + 1} />
          ))}
        </div>
      </div>

      {/* Ask AI button */}
      {onAskQuestion && (
        <button
          onClick={onAskQuestion}
          className="w-full flex items-center justify-center gap-2 py-3 bg-bg-elevated hover:bg-bg-interactive rounded-lg text-text-secondary hover:text-text-primary transition-colors"
        >
          <MessageSquare className="w-4 h-4" />
          Ask AI a question...
        </button>
      )}
    </div>
  )
}

function InsightItem({
  icon,
  insight,
  variant,
}: {
  icon: React.ReactNode
  insight: CoachingInsight
  variant: 'strength' | 'improvement'
}) {
  return (
    <div className="p-3 bg-bg-elevated/50 rounded-lg">
      <div className="flex items-start gap-3">
        <div className="mt-0.5">{icon}</div>
        <div className="flex-1 min-w-0">
          <p className="font-medium text-text-primary">{insight.title}</p>
          <p className="text-sm text-text-secondary mt-1">{insight.detail}</p>
          {insight.metric && (
            <p className="text-xs text-text-muted mt-1">
              {insight.metric}
              {insight.comparison && ` • ${insight.comparison}`}
            </p>
          )}
          {variant === 'improvement' && insight.action && (
            <p className="text-sm text-accent-blue mt-2">
              → {insight.action}
            </p>
          )}
        </div>
      </div>
    </div>
  )
}

function ActionItem({ insight, index }: { insight: CoachingInsight; index: number }) {
  return (
    <div className="flex items-center gap-3 p-3 bg-bg-elevated/50 rounded-lg hover:bg-bg-elevated transition-colors cursor-pointer group">
      <div className="w-6 h-6 flex items-center justify-center bg-accent-blue/20 rounded-full text-accent-blue text-sm font-medium">
        {index}
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className="font-medium text-text-primary">{insight.dealName}</span>
          {insight.dealAmount && (
            <span className="text-sm text-text-muted">
              ({formatCurrency(insight.dealAmount)})
            </span>
          )}
        </div>
        <p className="text-sm text-text-secondary truncate">{insight.detail}</p>
      </div>
      <ArrowRight className="w-4 h-4 text-text-muted group-hover:text-accent-blue transition-colors" />
    </div>
  )
}
