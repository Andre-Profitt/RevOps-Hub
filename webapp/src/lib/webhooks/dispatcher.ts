/**
 * Webhook Dispatcher
 * Handles webhook delivery with retries and DLQ
 */

import { v4 as uuidv4 } from 'uuid'
import type {
  WebhookSubscription,
  WebhookDelivery,
  WebhookPayload,
  DeliveryStatus,
  DeliveryAttempt,
  WebhookRetryConfig,
} from './types'
import {
  WEBHOOK_SIGNATURE_HEADER,
  WEBHOOK_TIMESTAMP_HEADER,
  WEBHOOK_DELIVERY_ID_HEADER,
} from './types'
import { generateSignatureHeader } from './signature'
import type { BaseEvent, EventType } from '../events/types'

// =============================================================================
// DISPATCHER CONFIG
// =============================================================================

const DEFAULT_RETRY_CONFIG: WebhookRetryConfig = {
  maxRetries: 5,
  initialDelaySeconds: 60,
  maxDelaySeconds: 3600,
  backoffMultiplier: 2,
}

const DELIVERY_TIMEOUT_MS = 30000 // 30 seconds

// =============================================================================
// WEBHOOK DISPATCHER
// =============================================================================

interface DispatcherDependencies {
  getActiveSubscriptions: (customerId: string, eventType: EventType) => Promise<WebhookSubscription[]>
  saveDelivery: (delivery: WebhookDelivery) => Promise<void>
  updateDelivery: (deliveryId: string, updates: Partial<WebhookDelivery>) => Promise<void>
  incrementFailureCount: (webhookId: string) => Promise<void>
  resetFailureCount: (webhookId: string) => Promise<void>
  scheduleRetry: (delivery: WebhookDelivery, retryAt: Date) => Promise<void>
}

export class WebhookDispatcher {
  private deps: DispatcherDependencies
  private inFlight: Map<string, AbortController> = new Map()

  constructor(deps: DispatcherDependencies) {
    this.deps = deps
  }

  /**
   * Dispatch event to all subscribed webhooks
   */
  async dispatch(event: BaseEvent): Promise<string[]> {
    // Get active subscriptions for this event
    const subscriptions = await this.deps.getActiveSubscriptions(
      event.customerId,
      event.type
    )

    if (subscriptions.length === 0) {
      return []
    }

    // Create deliveries for each subscription
    const deliveryIds: string[] = []

    for (const subscription of subscriptions) {
      const deliveryId = await this.createAndSendDelivery(subscription, event)
      deliveryIds.push(deliveryId)
    }

    return deliveryIds
  }

  /**
   * Create delivery record and attempt delivery
   */
  private async createAndSendDelivery(
    subscription: WebhookSubscription,
    event: BaseEvent
  ): Promise<string> {
    const deliveryId = uuidv4()

    // Build payload
    const payload: WebhookPayload = {
      deliveryId,
      eventId: event.eventId,
      eventType: event.type,
      customerId: event.customerId,
      timestamp: event.timestamp.toISOString(),
      data: event,
      meta: {
        webhookId: subscription.id,
        webhookName: subscription.name,
        attemptNumber: 1,
        deliveredAt: new Date().toISOString(),
      },
    }

    const payloadString = JSON.stringify(payload)

    // Create delivery record
    const delivery: WebhookDelivery = {
      id: deliveryId,
      webhookId: subscription.id,
      customerId: event.customerId,
      eventId: event.eventId,
      eventType: event.type,
      payload,
      payloadSizeBytes: Buffer.byteLength(payloadString, 'utf-8'),
      status: 'pending',
      attemptCount: 0,
      responseCode: null,
      responseTimeMs: null,
      errorMessage: null,
      createdAt: new Date(),
      completedAt: null,
      nextRetryAt: null,
    }

    await this.deps.saveDelivery(delivery)

    // Attempt delivery (fire and forget for async)
    this.attemptDelivery(subscription, delivery, payloadString).catch((err) => {
      console.error(`[WebhookDispatcher] Delivery failed: ${err.message}`)
    })

    return deliveryId
  }

  /**
   * Attempt webhook delivery
   */
  async attemptDelivery(
    subscription: WebhookSubscription,
    delivery: WebhookDelivery,
    payloadString: string
  ): Promise<void> {
    const retryConfig = { ...DEFAULT_RETRY_CONFIG, ...subscription.retryConfig }
    const attemptNumber = delivery.attemptCount + 1

    // Update delivery status
    await this.deps.updateDelivery(delivery.id, {
      status: 'pending',
      attemptCount: attemptNumber,
    })

    // Generate signature
    const timestamp = Math.floor(Date.now() / 1000)
    const signatureHeader = generateSignatureHeader(payloadString, subscription.secret, timestamp)

    // Build headers
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      'User-Agent': 'RevOps-Webhook/1.0',
      [WEBHOOK_SIGNATURE_HEADER]: signatureHeader,
      [WEBHOOK_TIMESTAMP_HEADER]: timestamp.toString(),
      [WEBHOOK_DELIVERY_ID_HEADER]: delivery.id,
      ...subscription.headers,
    }

    // Create abort controller for timeout
    const controller = new AbortController()
    this.inFlight.set(delivery.id, controller)

    const timeoutId = setTimeout(() => controller.abort(), DELIVERY_TIMEOUT_MS)

    const startTime = Date.now()
    let responseCode: number | null = null
    let errorMessage: string | null = null

    try {
      const response = await fetch(subscription.url, {
        method: 'POST',
        headers,
        body: payloadString,
        signal: controller.signal,
      })

      responseCode = response.status
      const responseTimeMs = Date.now() - startTime

      if (response.ok) {
        // Success!
        await this.deps.updateDelivery(delivery.id, {
          status: 'delivered',
          responseCode,
          responseTimeMs,
          completedAt: new Date(),
        })
        await this.deps.resetFailureCount(subscription.id)
        return
      }

      // Non-2xx response
      errorMessage = `HTTP ${response.status}: ${response.statusText}`
    } catch (err) {
      const responseTimeMs = Date.now() - startTime
      errorMessage = err instanceof Error ? err.message : 'Unknown error'

      if (err instanceof Error && err.name === 'AbortError') {
        errorMessage = 'Request timeout'
      }

      await this.deps.updateDelivery(delivery.id, {
        responseTimeMs,
      })
    } finally {
      clearTimeout(timeoutId)
      this.inFlight.delete(delivery.id)
    }

    // Handle failure
    await this.handleFailure(subscription, delivery, retryConfig, attemptNumber, responseCode, errorMessage)
  }

  /**
   * Handle delivery failure
   */
  private async handleFailure(
    subscription: WebhookSubscription,
    delivery: WebhookDelivery,
    retryConfig: WebhookRetryConfig,
    attemptNumber: number,
    responseCode: number | null,
    errorMessage: string | null
  ): Promise<void> {
    // Increment failure count
    await this.deps.incrementFailureCount(subscription.id)

    // Check if we should retry
    if (attemptNumber < retryConfig.maxRetries) {
      // Calculate next retry time with exponential backoff
      const delaySeconds = Math.min(
        retryConfig.initialDelaySeconds * Math.pow(retryConfig.backoffMultiplier, attemptNumber - 1),
        retryConfig.maxDelaySeconds
      )
      const nextRetryAt = new Date(Date.now() + delaySeconds * 1000)

      await this.deps.updateDelivery(delivery.id, {
        status: 'retrying',
        responseCode,
        errorMessage,
        nextRetryAt,
      })

      // Schedule retry
      await this.deps.scheduleRetry({ ...delivery, attemptCount: attemptNumber }, nextRetryAt)
    } else {
      // Final failure
      await this.deps.updateDelivery(delivery.id, {
        status: 'failed',
        responseCode,
        errorMessage,
        completedAt: new Date(),
      })
    }
  }

  /**
   * Retry a failed delivery
   */
  async retryDelivery(delivery: WebhookDelivery, subscription: WebhookSubscription): Promise<void> {
    const payloadString = JSON.stringify(delivery.payload)
    await this.attemptDelivery(subscription, delivery, payloadString)
  }

  /**
   * Cancel an in-flight delivery
   */
  cancelDelivery(deliveryId: string): boolean {
    const controller = this.inFlight.get(deliveryId)
    if (controller) {
      controller.abort()
      this.inFlight.delete(deliveryId)
      return true
    }
    return false
  }

  /**
   * Get number of in-flight deliveries
   */
  getInFlightCount(): number {
    return this.inFlight.size
  }
}

// =============================================================================
// SINGLETON INSTANCE
// =============================================================================

let dispatcherInstance: WebhookDispatcher | null = null

export function getWebhookDispatcher(deps?: DispatcherDependencies): WebhookDispatcher {
  if (!dispatcherInstance && deps) {
    dispatcherInstance = new WebhookDispatcher(deps)
  }
  if (!dispatcherInstance) {
    throw new Error('WebhookDispatcher not initialized. Call with dependencies first.')
  }
  return dispatcherInstance
}

// =============================================================================
// EXPORTS
// =============================================================================

export { DEFAULT_RETRY_CONFIG }
