/**
 * Webhook Types
 * Type definitions for webhook subscriptions and deliveries
 */

import type { EventType } from '../events/types'

// =============================================================================
// WEBHOOK SUBSCRIPTION
// =============================================================================

export interface WebhookSubscription {
  /** Unique webhook ID */
  id: string
  /** Customer/tenant ID */
  customerId: string
  /** Human-readable name */
  name: string
  /** Target URL for webhook delivery */
  url: string
  /** Secret for HMAC signature */
  secret: string
  /** Event types to subscribe to */
  events: EventType[]
  /** Is webhook active */
  isActive: boolean
  /** Creation timestamp */
  createdAt: Date
  /** Last update timestamp */
  updatedAt: Date
  /** Last successful delivery */
  lastTriggeredAt: Date | null
  /** Consecutive failure count */
  failureCount: number
  /** Auto-disabled due to failures */
  isDisabledAuto: boolean
  /** Optional description */
  description?: string
  /** Optional headers to include */
  headers?: Record<string, string>
  /** Retry configuration */
  retryConfig?: WebhookRetryConfig
}

export interface WebhookRetryConfig {
  /** Max retry attempts (default: 5) */
  maxRetries: number
  /** Initial retry delay in seconds (default: 60) */
  initialDelaySeconds: number
  /** Max retry delay in seconds (default: 3600) */
  maxDelaySeconds: number
  /** Backoff multiplier (default: 2) */
  backoffMultiplier: number
}

export interface CreateWebhookRequest {
  customerId: string
  name: string
  url: string
  events: EventType[]
  description?: string
  headers?: Record<string, string>
  retryConfig?: Partial<WebhookRetryConfig>
}

export interface UpdateWebhookRequest {
  name?: string
  url?: string
  events?: EventType[]
  isActive?: boolean
  description?: string
  headers?: Record<string, string>
  retryConfig?: Partial<WebhookRetryConfig>
}

// =============================================================================
// WEBHOOK DELIVERY
// =============================================================================

export type DeliveryStatus = 'pending' | 'delivered' | 'failed' | 'retrying'

export interface WebhookDelivery {
  /** Unique delivery ID */
  id: string
  /** Webhook subscription ID */
  webhookId: string
  /** Customer ID */
  customerId: string
  /** Event ID that triggered this delivery */
  eventId: string
  /** Event type */
  eventType: EventType
  /** Request payload */
  payload: unknown
  /** Payload size in bytes */
  payloadSizeBytes: number
  /** Delivery status */
  status: DeliveryStatus
  /** Number of delivery attempts */
  attemptCount: number
  /** HTTP response code from target */
  responseCode: number | null
  /** Response time in milliseconds */
  responseTimeMs: number | null
  /** Error message if failed */
  errorMessage: string | null
  /** When delivery was created */
  createdAt: Date
  /** When delivery was completed (success or final failure) */
  completedAt: Date | null
  /** Next retry time if retrying */
  nextRetryAt: Date | null
}

export interface DeliveryAttempt {
  attemptNumber: number
  timestamp: Date
  responseCode: number | null
  responseTimeMs: number | null
  errorMessage: string | null
  success: boolean
}

// =============================================================================
// WEBHOOK PAYLOAD
// =============================================================================

export interface WebhookPayload<T = unknown> {
  /** Webhook delivery ID */
  deliveryId: string
  /** Event ID */
  eventId: string
  /** Event type */
  eventType: EventType
  /** Tenant/customer ID */
  customerId: string
  /** Event timestamp */
  timestamp: string
  /** Event payload */
  data: T
  /** Webhook metadata */
  meta: {
    webhookId: string
    webhookName: string
    attemptNumber: number
    deliveredAt: string
  }
}

// =============================================================================
// SIGNATURE
// =============================================================================

export interface WebhookSignature {
  /** Signature algorithm */
  algorithm: 'sha256'
  /** Timestamp used for signature */
  timestamp: number
  /** HMAC signature */
  signature: string
}

export const WEBHOOK_SIGNATURE_HEADER = 'X-RevOps-Signature'
export const WEBHOOK_TIMESTAMP_HEADER = 'X-RevOps-Timestamp'
export const WEBHOOK_DELIVERY_ID_HEADER = 'X-RevOps-Delivery-Id'
