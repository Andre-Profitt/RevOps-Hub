import { clsx, type ClassValue } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

// Format currency with appropriate suffix
export function formatCurrency(value: number, compact = true): string {
  if (compact) {
    if (value >= 1000000) {
      return `$${(value / 1000000).toFixed(1)}M`
    }
    if (value >= 1000) {
      return `$${(value / 1000).toFixed(0)}K`
    }
  }
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value)
}

// Format percentage
export function formatPercent(value: number, showSign = false): string {
  const formatted = `${Math.round(value * 100)}%`
  if (showSign && value > 0) {
    return `+${formatted}`
  }
  return formatted
}

// Format delta with sign
export function formatDelta(value: number, suffix = ''): string {
  const sign = value >= 0 ? '+' : ''
  return `${sign}${value}${suffix}`
}

// Get health category from score
export function getHealthCategory(score: number): 'healthy' | 'monitor' | 'at-risk' | 'critical' {
  if (score >= 80) return 'healthy'
  if (score >= 60) return 'monitor'
  if (score >= 40) return 'at-risk'
  return 'critical'
}

// Get health color
export function getHealthColor(score: number): string {
  const category = getHealthCategory(score)
  const colors = {
    healthy: '#22c55e',
    monitor: '#eab308',
    'at-risk': '#f97316',
    critical: '#ef4444',
  }
  return colors[category]
}

// Get health label
export function getHealthLabel(score: number): string {
  const category = getHealthCategory(score)
  const labels = {
    healthy: 'Healthy',
    monitor: 'Monitor',
    'at-risk': 'At Risk',
    critical: 'Critical',
  }
  return labels[category]
}

// Format days with suffix
export function formatDays(days: number): string {
  return `${days}d`
}

// Format rank
export function formatRank(rank: number, total: number): string {
  return `#${rank}/${total}`
}

// Truncate text with ellipsis
export function truncate(str: string, length: number): string {
  if (str.length <= length) return str
  return str.slice(0, length) + '...'
}

// Calculate percentage of progress
export function calculateProgress(current: number, target: number): number {
  if (target === 0) return 0
  return Math.min(Math.max((current / target) * 100, 0), 100)
}
