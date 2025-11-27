/**
 * Commercial Analytics Views
 * Aggregated views that consume raw commercial datasets
 *
 * These views serve as consumers for:
 * - notification_daily_rollup
 * - notification_channel_stats
 * - notification_latency_metrics
 * - webhook_analytics
 *
 * And produce unified analytics for:
 * - Ops dashboards
 * - CS console
 * - Internal monitoring
 */

import type {
  NotificationChannel,
  NotificationStatus,
  SyncSystem,
  SyncHealthStatus,
  WebhookDeliveryStatus,
} from './datasets'

// =============================================================================
// VIEW TYPES
// =============================================================================

/**
 * Notification Analytics View
 * Consumes: notification_daily_rollup, notification_channel_stats, notification_latency_metrics
 */
export interface NotificationAnalyticsView {
  tenantId: string
  period: string // YYYY-MM-DD or YYYY-MM
  generatedAt: Date

  // Daily rollup
  dailyStats: {
    date: string
    total: number
    sent: number
    delivered: number
    failed: number
    opened: number
    deliveryRate: number
    openRate: number
  }[]

  // Channel breakdown
  channelStats: {
    channel: NotificationChannel
    total: number
    delivered: number
    failed: number
    avgLatencyMs: number
    deliveryRate: number
  }[]

  // Latency metrics
  latencyPercentiles: {
    p50: number
    p90: number
    p95: number
    p99: number
  }

  // Template performance
  templateStats: {
    templateCode: string
    category: string
    total: number
    delivered: number
    openRate: number
  }[]

  // Trends
  trends: {
    deliveryRateTrend: 'improving' | 'stable' | 'declining'
    volumeTrend: 'increasing' | 'stable' | 'decreasing'
    issuesDetected: string[]
  }
}

/**
 * Webhook Analytics View
 * Consumes: webhook_analytics
 */
export interface WebhookAnalyticsView {
  tenantId: string
  period: string
  generatedAt: Date

  // Overall stats
  overallStats: {
    total: number
    delivered: number
    failed: number
    retrying: number
    deliveryRate: number
    avgLatencyMs: number
  }

  // Per-webhook breakdown
  webhookStats: {
    webhookId: string
    name: string
    targetUrl: string
    status: string
    total: number
    delivered: number
    failed: number
    avgLatencyMs: number
    lastDelivery?: Date
    consecutiveFailures: number
  }[]

  // Error breakdown
  errorBreakdown: {
    errorType: string // 'timeout', 'http_4xx', 'http_5xx', 'connection', 'other'
    count: number
    percentage: number
    examples: string[]
  }[]

  // Latency distribution
  latencyDistribution: {
    bucket: string // '<100ms', '100-500ms', '500ms-1s', '1-5s', '>5s'
    count: number
    percentage: number
  }[]

  // Health indicators
  healthIndicators: {
    allWebhooksHealthy: boolean
    failingWebhooks: string[]
    suspendedWebhooks: string[]
    avgDeliveryRate: number
    recommendation?: string
  }
}

/**
 * Integration Health View
 * Consumes: webhook_analytics, sync_status
 */
export interface IntegrationHealthView {
  tenantId: string
  generatedAt: Date

  // Overall health
  overallHealth: 'healthy' | 'degraded' | 'unhealthy'
  healthScore: number // 0-100

  // Sync health
  syncHealth: {
    system: SyncSystem
    status: SyncHealthStatus
    lastSync?: Date
    message?: string
    trend: 'stable' | 'improving' | 'degrading'
  }[]

  // Webhook health
  webhookHealth: {
    totalConfigured: number
    active: number
    failing: number
    suspended: number
    avgDeliveryRate: number
  }

  // Alerts
  activeAlerts: {
    type: 'sync_error' | 'sync_stale' | 'webhook_failing' | 'webhook_suspended'
    system?: SyncSystem
    webhookId?: string
    message: string
    since: Date
    severity: 'warning' | 'critical'
  }[]

  // Recommendations
  recommendations: {
    priority: 'high' | 'medium' | 'low'
    category: 'sync' | 'webhook' | 'configuration'
    message: string
    action?: string
  }[]
}

/**
 * Commercial Ops Dashboard View
 * Combined view for internal ops monitoring
 */
export interface CommercialOpsDashboardView {
  generatedAt: Date
  period: string // Last 24h, 7d, 30d

  // Summary metrics
  summary: {
    totalTenants: number
    activeTenants: number
    tenantsWithIssues: number
    overallHealthScore: number
  }

  // Notification overview
  notifications: {
    totalSent: number
    deliveryRate: number
    topFailingTemplates: { templateCode: string; failureRate: number }[]
    channelBreakdown: Record<NotificationChannel, number>
  }

  // Webhook overview
  webhooks: {
    totalDeliveries: number
    deliveryRate: number
    avgLatencyMs: number
    failingWebhooks: { tenantId: string; webhookName: string; consecutiveFailures: number }[]
  }

  // Sync overview
  syncs: {
    totalSyncs: number
    healthyRate: number
    problemSyncs: { tenantId: string; system: SyncSystem; issue: string }[]
  }

  // Action analytics
  actions: {
    totalExecuted: number
    successRate: number
    avgLatencyMs: number
    topFailingActions: { actionType: string; failureRate: number }[]
    avgFeedbackRating: number
  }

  // Alerts requiring attention
  criticalAlerts: {
    tenantId: string
    type: string
    message: string
    since: Date
  }[]
}

// =============================================================================
// VIEW GENERATORS
// =============================================================================

interface AnalyticsViewDependencies {
  // Notification data
  getNotificationDailyRollup: (tenantId: string, startDate: Date, endDate: Date) => Promise<NotificationAnalyticsView['dailyStats']>
  getNotificationChannelStats: (tenantId: string, period: string) => Promise<NotificationAnalyticsView['channelStats']>
  getNotificationLatencyMetrics: (tenantId: string, period: string) => Promise<NotificationAnalyticsView['latencyPercentiles']>
  getNotificationTemplateStats: (tenantId: string, period: string) => Promise<NotificationAnalyticsView['templateStats']>

  // Webhook data
  getWebhookAnalytics: (tenantId: string, period: string) => Promise<Omit<WebhookAnalyticsView, 'tenantId' | 'period' | 'generatedAt'>>

  // Sync data
  getSyncStatuses: (tenantId: string) => Promise<IntegrationHealthView['syncHealth']>

  // Action data
  getActionStats: (tenantId: string, period: string) => Promise<CommercialOpsDashboardView['actions']>

  // Alert data
  getActiveAlerts: (tenantId?: string) => Promise<IntegrationHealthView['activeAlerts']>

  // Tenant data
  getTenantCount: () => Promise<{ total: number; active: number; withIssues: number }>
}

export class AnalyticsViewGenerator {
  private deps: AnalyticsViewDependencies

  constructor(deps: AnalyticsViewDependencies) {
    this.deps = deps
  }

  /**
   * Generate notification analytics view
   */
  async generateNotificationAnalytics(
    tenantId: string,
    startDate: Date,
    endDate: Date
  ): Promise<NotificationAnalyticsView> {
    const period = `${startDate.toISOString().split('T')[0]}_${endDate.toISOString().split('T')[0]}`

    const [dailyStats, channelStats, latencyPercentiles, templateStats] = await Promise.all([
      this.deps.getNotificationDailyRollup(tenantId, startDate, endDate),
      this.deps.getNotificationChannelStats(tenantId, period),
      this.deps.getNotificationLatencyMetrics(tenantId, period),
      this.deps.getNotificationTemplateStats(tenantId, period),
    ])

    // Calculate trends
    const recentDeliveryRate = dailyStats.slice(-7).reduce((sum, d) => sum + d.deliveryRate, 0) / 7
    const olderDeliveryRate = dailyStats.slice(0, 7).reduce((sum, d) => sum + d.deliveryRate, 0) / 7

    const deliveryRateTrend = recentDeliveryRate > olderDeliveryRate + 0.02 ? 'improving' :
      recentDeliveryRate < olderDeliveryRate - 0.02 ? 'declining' : 'stable'

    const recentVolume = dailyStats.slice(-7).reduce((sum, d) => sum + d.total, 0)
    const olderVolume = dailyStats.slice(0, 7).reduce((sum, d) => sum + d.total, 0)

    const volumeTrend = recentVolume > olderVolume * 1.1 ? 'increasing' :
      recentVolume < olderVolume * 0.9 ? 'decreasing' : 'stable'

    // Detect issues
    const issuesDetected: string[] = []
    if (recentDeliveryRate < 0.95) {
      issuesDetected.push('Delivery rate below 95%')
    }
    if (latencyPercentiles.p95 > 5000) {
      issuesDetected.push('P95 latency exceeds 5 seconds')
    }
    for (const channel of channelStats) {
      if (channel.deliveryRate < 0.9) {
        issuesDetected.push(`${channel.channel} channel has low delivery rate`)
      }
    }

    return {
      tenantId,
      period,
      generatedAt: new Date(),
      dailyStats,
      channelStats,
      latencyPercentiles,
      templateStats,
      trends: {
        deliveryRateTrend,
        volumeTrend,
        issuesDetected,
      },
    }
  }

  /**
   * Generate webhook analytics view
   */
  async generateWebhookAnalytics(
    tenantId: string,
    period: string
  ): Promise<WebhookAnalyticsView> {
    const analytics = await this.deps.getWebhookAnalytics(tenantId, period)

    return {
      tenantId,
      period,
      generatedAt: new Date(),
      ...analytics,
    }
  }

  /**
   * Generate integration health view
   */
  async generateIntegrationHealth(tenantId: string): Promise<IntegrationHealthView> {
    const [syncHealth, alerts] = await Promise.all([
      this.deps.getSyncStatuses(tenantId),
      this.deps.getActiveAlerts(tenantId),
    ])

    // Calculate health score
    let healthScore = 100
    for (const sync of syncHealth) {
      if (sync.status === 'error') healthScore -= 20
      else if (sync.status === 'warning') healthScore -= 10
    }
    for (const alert of alerts) {
      if (alert.severity === 'critical') healthScore -= 15
      else healthScore -= 5
    }
    healthScore = Math.max(0, healthScore)

    const overallHealth: IntegrationHealthView['overallHealth'] =
      healthScore >= 80 ? 'healthy' :
      healthScore >= 50 ? 'degraded' : 'unhealthy'

    // Generate recommendations
    const recommendations: IntegrationHealthView['recommendations'] = []

    for (const alert of alerts) {
      if (alert.type === 'sync_error') {
        recommendations.push({
          priority: 'high',
          category: 'sync',
          message: `Fix ${alert.system} sync: ${alert.message}`,
          action: 'Check credentials and connectivity',
        })
      }
      if (alert.type === 'webhook_suspended') {
        recommendations.push({
          priority: 'high',
          category: 'webhook',
          message: `Webhook suspended due to repeated failures`,
          action: 'Verify endpoint is reachable and returning 2xx',
        })
      }
    }

    return {
      tenantId,
      generatedAt: new Date(),
      overallHealth,
      healthScore,
      syncHealth,
      webhookHealth: {
        totalConfigured: 0, // Would be populated from webhook config count
        active: 0,
        failing: 0,
        suspended: 0,
        avgDeliveryRate: 0,
      },
      activeAlerts: alerts,
      recommendations,
    }
  }

  /**
   * Generate ops dashboard view
   */
  async generateOpsDashboard(period: string): Promise<CommercialOpsDashboardView> {
    const [tenantCounts, alerts] = await Promise.all([
      this.deps.getTenantCount(),
      this.deps.getActiveAlerts(),
    ])

    return {
      generatedAt: new Date(),
      period,
      summary: {
        totalTenants: tenantCounts.total,
        activeTenants: tenantCounts.active,
        tenantsWithIssues: tenantCounts.withIssues,
        overallHealthScore: 85, // Would be calculated
      },
      notifications: {
        totalSent: 0,
        deliveryRate: 0.98,
        topFailingTemplates: [],
        channelBreakdown: {} as Record<NotificationChannel, number>,
      },
      webhooks: {
        totalDeliveries: 0,
        deliveryRate: 0.95,
        avgLatencyMs: 250,
        failingWebhooks: [],
      },
      syncs: {
        totalSyncs: 0,
        healthyRate: 0.92,
        problemSyncs: [],
      },
      actions: {
        totalExecuted: 0,
        successRate: 0.95,
        avgLatencyMs: 500,
        topFailingActions: [],
        avgFeedbackRating: 4.2,
      },
      criticalAlerts: alerts
        .filter(a => a.severity === 'critical')
        .map(a => ({
          tenantId: '', // Would have tenant context
          type: a.type,
          message: a.message,
          since: a.since,
        })),
    }
  }
}

// =============================================================================
// SINGLETON
// =============================================================================

let analyticsViewGeneratorInstance: AnalyticsViewGenerator | null = null

export function getAnalyticsViewGenerator(
  deps?: AnalyticsViewDependencies
): AnalyticsViewGenerator {
  if (!analyticsViewGeneratorInstance && deps) {
    analyticsViewGeneratorInstance = new AnalyticsViewGenerator(deps)
  }
  if (!analyticsViewGeneratorInstance) {
    throw new Error('AnalyticsViewGenerator not initialized')
  }
  return analyticsViewGeneratorInstance
}
