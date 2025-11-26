'use client'

import { cn, formatCurrency } from '@/lib/utils'
import { HealthBadge } from './HealthIndicator'
import { Phone, Mail, Calendar, AlertCircle } from 'lucide-react'
import type { StuckDeal } from '@/types'

interface DataTableProps {
  data: StuckDeal[]
  onRowClick?: (deal: StuckDeal) => void
  selectedId?: string
  maxRows?: number
  className?: string
}

const urgencyIcons = {
  CRITICAL: <span className="text-status-critical">●</span>,
  HIGH: <span className="text-status-at-risk">●</span>,
  MEDIUM: <span className="text-status-monitor">●</span>,
  LOW: <span className="text-status-healthy">●</span>,
}

const actionIcons = {
  call: <Phone className="w-4 h-4" />,
  email: <Mail className="w-4 h-4" />,
  meeting: <Calendar className="w-4 h-4" />,
  escalate: <AlertCircle className="w-4 h-4" />,
}

export function StuckDealsTable({
  data,
  onRowClick,
  selectedId,
  maxRows = 5,
  className,
}: DataTableProps) {
  const displayData = data.slice(0, maxRows)

  return (
    <div className={cn('overflow-hidden', className)}>
      <table className="data-table">
        <thead>
          <tr>
            <th className="w-8"></th>
            <th>Deal</th>
            <th className="text-right">Amount</th>
            <th>Stage</th>
            <th className="text-center">Days</th>
            <th className="text-center">Health</th>
            <th>Risk Flags</th>
            <th className="w-12"></th>
          </tr>
        </thead>
        <tbody>
          {displayData.map((deal) => (
            <tr
              key={deal.id}
              onClick={() => onRowClick?.(deal)}
              className={cn(
                'cursor-pointer transition-colors',
                selectedId === deal.id && 'bg-bg-elevated border-l-2 border-l-accent-blue'
              )}
            >
              <td className="text-center">{urgencyIcons[deal.urgency]}</td>
              <td>
                <div className="flex flex-col">
                  <span className="font-medium text-text-primary">
                    {deal.accountName}
                  </span>
                  <span className="text-xs text-text-muted">{deal.ownerName}</span>
                </div>
              </td>
              <td className="text-right font-mono tabular-nums">
                {formatCurrency(deal.amount)}
              </td>
              <td className="text-text-secondary italic">{deal.stageName}</td>
              <td className="text-center font-mono tabular-nums">{deal.daysInStage}</td>
              <td className="text-center">
                <HealthBadge score={deal.healthScore} />
              </td>
              <td>
                <div className="flex flex-wrap gap-1">
                  {deal.riskFlags.slice(0, 2).map((flag, i) => (
                    <span
                      key={i}
                      className="text-xs px-2 py-0.5 bg-status-at-risk/10 text-status-at-risk rounded"
                    >
                      {flag}
                    </span>
                  ))}
                  {deal.riskFlags.length > 2 && (
                    <span className="text-xs text-text-muted">
                      +{deal.riskFlags.length - 2}
                    </span>
                  )}
                </div>
              </td>
              <td>
                <button
                  className="p-2 rounded-lg hover:bg-bg-interactive text-text-secondary hover:text-accent-blue transition-colors"
                  onClick={(e) => {
                    e.stopPropagation()
                    // Handle action
                  }}
                >
                  {actionIcons[deal.primaryAction]}
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {data.length > maxRows && (
        <div className="py-3 text-center">
          <button className="text-sm text-accent-blue hover:underline">
            View all {data.length} deals →
          </button>
        </div>
      )}
    </div>
  )
}
