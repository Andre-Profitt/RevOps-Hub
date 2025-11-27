/**
 * RevOps SDK for TypeScript
 * Official SDK for the RevOps Command Center
 *
 * @example
 * ```typescript
 * import { createClient, verifyWebhook, createWebhookRouter } from '@revops/sdk'
 *
 * // Create API client
 * const client = createClient({
 *   baseUrl: 'https://api.revops.io',
 *   apiKey: process.env.REVOPS_API_KEY,
 * })
 *
 * // Get tenant health
 * const health = await client.getHealthScore('tenant-123')
 *
 * // Verify incoming webhooks
 * const result = verifyWebhook(body, signature, secret)
 * if (result.valid) {
 *   console.log('Event:', result.payload.eventType)
 * }
 * ```
 *
 * @packageDocumentation
 */

// Client
export { RevOpsClient, createClient } from './client'

// Webhooks
export {
  verifyWebhook,
  createWebhookRouter,
  webhookMiddleware,
  SIGNATURE_HEADER,
  TIMESTAMP_HEADER,
  DELIVERY_ID_HEADER,
  type WebhookRouter,
  type WebhookHandler,
  type VerificationResult,
} from './webhooks'

// Types
export type {
  RevOpsConfig,
  Tenant,
  TenantConfig,
  ImplementationStatus,
  HealthScore,
  ROIMetrics,
  WebhookSubscription,
  WebhookDelivery,
  WebhookPayload,
  CreateWebhookRequest,
  ApiResponse,
  PaginatedResponse,
  EventType,
} from './types'

export { EventTypeSchema, WebhookPayloadSchema } from './types'
