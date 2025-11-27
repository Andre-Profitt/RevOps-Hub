/**
 * RevOps SDK Type Definitions
 */

import { z } from 'zod'

// =============================================================================
// EVENT TYPES
// =============================================================================

export const EventTypeSchema = z.enum([
  'tenant.created',
  'tenant.updated',
  'tenant.suspended',
  'tenant.deleted',
  'implementation.stage_changed',
  'implementation.blocked',
  'implementation.completed',
  'implementation.checklist_updated',
  'health.score_changed',
  'health.alert_triggered',
  'health.threshold_breached',
  'data.sync_completed',
  'data.sync_failed',
  'data.quality_alert',
  'ai.prediction_generated',
  'ai.insight_created',
  'ai.model_retrained',
  'billing.plan_changed',
  'billing.usage_threshold',
  'billing.invoice_generated',
  'user.invited',
  'user.activated',
  'user.role_changed',
  'integration.connected',
  'integration.disconnected',
])

export type EventType = z.infer<typeof EventTypeSchema>

// =============================================================================
// WEBHOOK PAYLOAD
// =============================================================================

export const WebhookPayloadSchema = z.object({
  deliveryId: z.string().uuid(),
  eventId: z.string().uuid(),
  eventType: EventTypeSchema,
  customerId: z.string(),
  timestamp: z.string().datetime(),
  data: z.record(z.unknown()),
  meta: z.object({
    webhookId: z.string(),
    webhookName: z.string(),
    attemptNumber: z.number().int().positive(),
    deliveredAt: z.string().datetime(),
  }),
})

export type WebhookPayload<T = Record<string, unknown>> = {
  deliveryId: string
  eventId: string
  eventType: EventType
  customerId: string
  timestamp: string
  data: T
  meta: {
    webhookId: string
    webhookName: string
    attemptNumber: number
    deliveredAt: string
  }
}

// =============================================================================
// API TYPES
// =============================================================================

export interface RevOpsConfig {
  /** API base URL */
  baseUrl: string
  /** API key for authentication */
  apiKey: string
  /** Webhook signing secret (for webhook verification) */
  webhookSecret?: string
  /** Request timeout in milliseconds */
  timeout?: number
  /** Enable debug logging */
  debug?: boolean
}

export interface Tenant {
  id: string
  name: string
  tier: 'starter' | 'growth' | 'enterprise'
  status: 'active' | 'suspended' | 'pending'
  createdAt: string
  config: TenantConfig
}

export interface TenantConfig {
  crmType: 'salesforce' | 'hubspot' | 'dynamics'
  timezone: string
  fiscalYearStart: number
  currency: string
  features: string[]
}

export interface ImplementationStatus {
  customerId: string
  currentStage: 'onboarding' | 'connector_setup' | 'dashboard_config' | 'agent_training' | 'live'
  completionPercentage: number
  blockedItems: string[]
  startedAt: string
  estimatedCompletionDate: string | null
  checklistProgress: Record<string, boolean>
}

export interface HealthScore {
  customerId: string
  overallScore: number
  dimensions: {
    dataFreshness: number
    adoption: number
    aiAccuracy: number
    buildHealth: number
  }
  trend: 'improving' | 'stable' | 'declining'
  lastUpdated: string
}

export interface ROIMetrics {
  customerId: string
  winRateImprovement: number
  cycleTimeReduction: number
  forecastAccuracyGain: number
  dataHygieneImprovement: number
  estimatedAnnualValue: number
  periodStart: string
  periodEnd: string
}

// =============================================================================
// API RESPONSE TYPES
// =============================================================================

export interface ApiResponse<T> {
  success: boolean
  data?: T
  error?: {
    code: string
    message: string
    details?: Record<string, unknown>
  }
  meta?: {
    requestId: string
    timestamp: string
  }
}

export interface PaginatedResponse<T> extends ApiResponse<T[]> {
  pagination?: {
    page: number
    pageSize: number
    totalItems: number
    totalPages: number
    hasNext: boolean
    hasPrev: boolean
  }
}

// =============================================================================
// WEBHOOK SUBSCRIPTION
// =============================================================================

export interface WebhookSubscription {
  id: string
  name: string
  url: string
  events: EventType[]
  isActive: boolean
  createdAt: string
  lastTriggeredAt: string | null
}

export interface CreateWebhookRequest {
  name: string
  url: string
  events: EventType[]
  description?: string
  headers?: Record<string, string>
}

export interface WebhookDelivery {
  id: string
  webhookId: string
  eventType: EventType
  status: 'pending' | 'delivered' | 'failed' | 'retrying'
  attemptCount: number
  responseCode: number | null
  createdAt: string
  completedAt: string | null
}
