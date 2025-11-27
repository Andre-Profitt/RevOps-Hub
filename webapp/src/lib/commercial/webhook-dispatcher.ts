/**
 * Webhook Dispatcher
 * Handles webhook configuration, delivery, and event logging
 */

import { v4 as uuidv4 } from 'uuid'
import * as crypto from 'crypto'
import type {
  WebhookConfig,
  WebhookEvent,
  WebhookStatus,
  WebhookDeliveryStatus,
} from './datasets'
import { getEventBus } from '../events/bus'

// =============================================================================
// DEPENDENCIES
// =============================================================================

interface WebhookDispatcherDependencies {
  // Config storage
  getWebhookConfig: (id: string) => Promise<WebhookConfig | null>
  getWebhookConfigsByTenant: (tenantId: string) => Promise<WebhookConfig[]>
  getWebhookConfigsByEvent: (tenantId: string, eventType: string) => Promise<WebhookConfig[]>
  saveWebhookConfig: (config: WebhookConfig) => Promise<void>

  // Event storage
  saveWebhookEvent: (event: WebhookEvent) => Promise<void>
  getWebhookEvents: (tenantId: string, options?: {
    webhookConfigId?: string
    status?: WebhookDeliveryStatus
    since?: Date
    limit?: number
  }) => Promise<WebhookEvent[]>
  getPendingRetries: () => Promise<WebhookEvent[]>

  // HTTP client
  httpPost: (url: string, body: string, headers: Record<string, string>) => Promise<{
    status: number
    body: string
    latencyMs: number
  }>

  // Scheduling
  scheduleRetry: (event: WebhookEvent, delayMs: number) => Promise<void>
}

// =============================================================================
// WEBHOOK DISPATCHER
// =============================================================================

export class WebhookDispatcher {
  private deps: WebhookDispatcherDependencies
  private retryInterval: NodeJS.Timeout | null = null

  constructor(deps: WebhookDispatcherDependencies) {
    this.deps = deps
  }

  /**
   * Start the retry processor
   */
  start(intervalMs = 60000): void {
    if (this.retryInterval) return

    this.retryInterval = setInterval(() => {
      this.processRetries().catch(console.error)
    }, intervalMs)
  }

  /**
   * Stop the retry processor
   */
  stop(): void {
    if (this.retryInterval) {
      clearInterval(this.retryInterval)
      this.retryInterval = null
    }
  }

  // ===========================================================================
  // CONFIG MANAGEMENT
  // ===========================================================================

  /**
   * Create a webhook configuration
   */
  async createConfig(params: {
    tenantId: string
    name: string
    description?: string
    targetUrl: string
    subscribedEvents: string[]
    headers?: Record<string, string>
    maxAttempts?: number
    backoffMs?: number
  }): Promise<WebhookConfig> {
    // Generate a secure secret
    const secret = crypto.randomBytes(32).toString('hex')

    const config: WebhookConfig = {
      id: uuidv4(),
      tenantId: params.tenantId,
      name: params.name,
      description: params.description,
      targetUrl: params.targetUrl,
      secret,
      subscribedEvents: params.subscribedEvents,
      headers: params.headers,
      status: 'active',
      retryPolicy: {
        maxAttempts: params.maxAttempts ?? 5,
        backoffMs: params.backoffMs ?? 1000,
        backoffMultiplier: 2,
      },
      successCount: 0,
      failureCount: 0,
      consecutiveFailures: 0,
      createdAt: new Date(),
      updatedAt: new Date(),
    }

    await this.deps.saveWebhookConfig(config)
    return config
  }

  /**
   * Update a webhook configuration
   */
  async updateConfig(
    configId: string,
    updates: Partial<{
      name: string
      description: string
      targetUrl: string
      subscribedEvents: string[]
      headers: Record<string, string>
      status: WebhookStatus
    }>
  ): Promise<WebhookConfig> {
    const config = await this.deps.getWebhookConfig(configId)
    if (!config) {
      throw new Error('Webhook config not found')
    }

    const updated: WebhookConfig = {
      ...config,
      ...updates,
      updatedAt: new Date(),
    }

    await this.deps.saveWebhookConfig(updated)
    return updated
  }

  /**
   * Get webhook configs for a tenant
   */
  async getConfigs(tenantId: string): Promise<WebhookConfig[]> {
    return this.deps.getWebhookConfigsByTenant(tenantId)
  }

  /**
   * Rotate webhook secret
   */
  async rotateSecret(configId: string): Promise<{ newSecret: string }> {
    const config = await this.deps.getWebhookConfig(configId)
    if (!config) {
      throw new Error('Webhook config not found')
    }

    const newSecret = crypto.randomBytes(32).toString('hex')

    const updated: WebhookConfig = {
      ...config,
      secret: newSecret,
      updatedAt: new Date(),
    }

    await this.deps.saveWebhookConfig(updated)
    return { newSecret }
  }

  // ===========================================================================
  // DELIVERY
  // ===========================================================================

  /**
   * Dispatch an event to all subscribed webhooks
   */
  async dispatch(
    tenantId: string,
    eventType: string,
    eventId: string,
    payload: unknown
  ): Promise<WebhookEvent[]> {
    const configs = await this.deps.getWebhookConfigsByEvent(tenantId, eventType)
    const events: WebhookEvent[] = []

    for (const config of configs) {
      if (config.status !== 'active') continue

      const event = await this.deliverToWebhook(config, eventType, eventId, payload)
      events.push(event)
    }

    return events
  }

  /**
   * Deliver to a specific webhook
   */
  private async deliverToWebhook(
    config: WebhookConfig,
    eventType: string,
    eventId: string,
    payload: unknown
  ): Promise<WebhookEvent> {
    const body = JSON.stringify({
      id: eventId,
      type: eventType,
      timestamp: new Date().toISOString(),
      data: payload,
    })

    const signature = this.signPayload(body, config.secret)
    const bodyHash = crypto.createHash('sha256').update(body).digest('hex')

    const event: WebhookEvent = {
      id: uuidv4(),
      tenantId: config.tenantId,
      webhookConfigId: config.id,
      eventType,
      eventId,
      targetUrl: config.targetUrl,
      status: 'pending',
      attempt: 1,
      maxAttempts: config.retryPolicy.maxAttempts,
      requestBodyHash: bodyHash,
      createdAt: new Date(),
    }

    await this.deps.saveWebhookEvent(event)

    try {
      const result = await this.deps.httpPost(config.targetUrl, body, {
        'Content-Type': 'application/json',
        'X-Webhook-Signature': signature,
        'X-Webhook-Id': event.id,
        'X-Event-Type': eventType,
        ...config.headers,
      })

      event.httpStatus = result.status
      event.latencyMs = result.latencyMs

      if (result.status >= 200 && result.status < 300) {
        event.status = 'delivered'
        event.deliveredAt = new Date()
        await this.updateConfigSuccess(config)
      } else {
        event.status = 'failed'
        event.responseBody = result.body.substring(0, 1000)
        event.errorMessage = `HTTP ${result.status}`
        await this.handleFailure(config, event)
      }
    } catch (error) {
      event.status = 'failed'
      event.errorMessage = error instanceof Error ? error.message : String(error)
      await this.handleFailure(config, event)
    }

    await this.deps.saveWebhookEvent(event)

    // Emit analytics event
    const eventBus = getEventBus()
    eventBus.emit({
      eventId: uuidv4(),
      type: 'health.alert_triggered',
      customerId: config.tenantId,
      timestamp: new Date(),
      data: {
        alertType: 'webhook_delivery',
        webhookConfigId: config.id,
        status: event.status,
        httpStatus: event.httpStatus,
        latencyMs: event.latencyMs,
      },
    })

    return event
  }

  /**
   * Sign payload with HMAC
   */
  private signPayload(payload: string, secret: string): string {
    const timestamp = Math.floor(Date.now() / 1000)
    const signedPayload = `${timestamp}.${payload}`
    const signature = crypto
      .createHmac('sha256', secret)
      .update(signedPayload)
      .digest('hex')
    return `t=${timestamp},v1=${signature}`
  }

  /**
   * Update config success metrics
   */
  private async updateConfigSuccess(config: WebhookConfig): Promise<void> {
    const updated: WebhookConfig = {
      ...config,
      successCount: config.successCount + 1,
      consecutiveFailures: 0,
      lastSuccessAt: new Date(),
      status: 'active', // Restore if was failing
      updatedAt: new Date(),
    }
    await this.deps.saveWebhookConfig(updated)
  }

  /**
   * Handle delivery failure
   */
  private async handleFailure(
    config: WebhookConfig,
    event: WebhookEvent
  ): Promise<void> {
    const consecutiveFailures = config.consecutiveFailures + 1

    // Schedule retry if attempts remaining
    if (event.attempt < event.maxAttempts) {
      const delay = config.retryPolicy.backoffMs *
        Math.pow(config.retryPolicy.backoffMultiplier, event.attempt - 1)

      event.status = 'retrying'
      event.scheduledAt = new Date(Date.now() + delay)

      await this.deps.scheduleRetry(event, delay)
    }

    // Update config failure metrics
    const status: WebhookStatus = consecutiveFailures >= 10 ? 'suspended' :
      consecutiveFailures >= 5 ? 'failing' : config.status

    const updated: WebhookConfig = {
      ...config,
      failureCount: config.failureCount + 1,
      consecutiveFailures,
      lastFailureAt: new Date(),
      status,
      updatedAt: new Date(),
    }
    await this.deps.saveWebhookConfig(updated)

    // Emit alert if webhook is now failing/suspended
    if (status !== config.status) {
      const eventBus = getEventBus()
      eventBus.emit({
        eventId: uuidv4(),
        type: 'health.alert_triggered',
        customerId: config.tenantId,
        timestamp: new Date(),
        data: {
          alertType: 'webhook_health',
          webhookConfigId: config.id,
          status,
          consecutiveFailures,
        },
      })
    }
  }

  /**
   * Process pending retries
   */
  async processRetries(): Promise<void> {
    const pendingRetries = await this.deps.getPendingRetries()

    for (const event of pendingRetries) {
      const config = await this.deps.getWebhookConfig(event.webhookConfigId)
      if (!config || config.status === 'suspended') continue

      // Increment attempt
      event.attempt++

      try {
        // Re-fetch the original payload would be needed in real impl
        // For now, we'll skip the actual retry logic
        console.log(`Retrying webhook event ${event.id}, attempt ${event.attempt}`)
      } catch (error) {
        console.error(`Retry failed for event ${event.id}:`, error)
      }
    }
  }

  // ===========================================================================
  // ANALYTICS
  // ===========================================================================

  /**
   * Get webhook delivery events
   */
  async getEvents(
    tenantId: string,
    options?: {
      webhookConfigId?: string
      status?: WebhookDeliveryStatus
      since?: Date
      limit?: number
    }
  ): Promise<WebhookEvent[]> {
    return this.deps.getWebhookEvents(tenantId, options)
  }

  /**
   * Get webhook analytics
   */
  async getAnalytics(
    tenantId: string,
    since?: Date
  ): Promise<{
    total: number
    delivered: number
    failed: number
    retrying: number
    deliveryRate: number
    avgLatencyMs: number
    byWebhook: Map<string, {
      configId: string
      total: number
      delivered: number
      failed: number
      avgLatencyMs: number
    }>
  }> {
    const events = await this.getEvents(tenantId, { since })

    let delivered = 0
    let failed = 0
    let retrying = 0
    let totalLatency = 0
    let latencyCount = 0

    const byWebhook = new Map<string, {
      configId: string
      total: number
      delivered: number
      failed: number
      avgLatencyMs: number
      totalLatency: number
      latencyCount: number
    }>()

    for (const event of events) {
      // Overall stats
      if (event.status === 'delivered') delivered++
      else if (event.status === 'failed') failed++
      else if (event.status === 'retrying') retrying++

      if (event.latencyMs) {
        totalLatency += event.latencyMs
        latencyCount++
      }

      // Per-webhook stats
      let webhookStats = byWebhook.get(event.webhookConfigId)
      if (!webhookStats) {
        webhookStats = {
          configId: event.webhookConfigId,
          total: 0,
          delivered: 0,
          failed: 0,
          avgLatencyMs: 0,
          totalLatency: 0,
          latencyCount: 0,
        }
        byWebhook.set(event.webhookConfigId, webhookStats)
      }

      webhookStats.total++
      if (event.status === 'delivered') webhookStats.delivered++
      else if (event.status === 'failed') webhookStats.failed++

      if (event.latencyMs) {
        webhookStats.totalLatency += event.latencyMs
        webhookStats.latencyCount++
      }
    }

    // Calculate averages
    for (const stats of byWebhook.values()) {
      stats.avgLatencyMs = stats.latencyCount > 0
        ? stats.totalLatency / stats.latencyCount
        : 0
    }

    return {
      total: events.length,
      delivered,
      failed,
      retrying,
      deliveryRate: events.length > 0 ? delivered / events.length : 0,
      avgLatencyMs: latencyCount > 0 ? totalLatency / latencyCount : 0,
      byWebhook,
    }
  }

  /**
   * Get integration health status
   */
  async getIntegrationHealth(tenantId: string): Promise<{
    webhooks: {
      id: string
      name: string
      status: WebhookStatus
      successRate: number
      lastSuccess?: Date
      lastFailure?: Date
    }[]
  }> {
    const configs = await this.getConfigs(tenantId)

    return {
      webhooks: configs.map(config => ({
        id: config.id,
        name: config.name,
        status: config.status,
        successRate: (config.successCount + config.failureCount) > 0
          ? config.successCount / (config.successCount + config.failureCount)
          : 1,
        lastSuccess: config.lastSuccessAt,
        lastFailure: config.lastFailureAt,
      })),
    }
  }
}

// =============================================================================
// SINGLETON
// =============================================================================

let webhookDispatcherInstance: WebhookDispatcher | null = null

export function getWebhookDispatcher(
  deps?: WebhookDispatcherDependencies
): WebhookDispatcher {
  if (!webhookDispatcherInstance && deps) {
    webhookDispatcherInstance = new WebhookDispatcher(deps)
  }
  if (!webhookDispatcherInstance) {
    throw new Error('WebhookDispatcher not initialized')
  }
  return webhookDispatcherInstance
}
