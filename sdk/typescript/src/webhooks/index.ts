/**
 * Webhook Utilities
 * Verify and parse incoming webhooks from RevOps
 */

import { createHmac, timingSafeEqual } from 'crypto'
import { WebhookPayloadSchema, type WebhookPayload, type EventType } from '../types'

// =============================================================================
// CONSTANTS
// =============================================================================

export const SIGNATURE_HEADER = 'x-revops-signature'
export const TIMESTAMP_HEADER = 'x-revops-timestamp'
export const DELIVERY_ID_HEADER = 'x-revops-delivery-id'

const DEFAULT_TOLERANCE_SECONDS = 300 // 5 minutes

// =============================================================================
// VERIFICATION
// =============================================================================

export interface VerificationResult {
  valid: boolean
  reason?: string
  payload?: WebhookPayload
}

/**
 * Verify webhook signature and parse payload
 *
 * @example
 * ```typescript
 * import { verifyWebhook } from '@revops/sdk/webhooks'
 *
 * app.post('/webhooks/revops', (req, res) => {
 *   const result = verifyWebhook(
 *     req.body,
 *     req.headers['x-revops-signature'],
 *     process.env.REVOPS_WEBHOOK_SECRET
 *   )
 *
 *   if (!result.valid) {
 *     return res.status(401).json({ error: result.reason })
 *   }
 *
 *   // Process the verified payload
 *   console.log('Event:', result.payload.eventType)
 * })
 * ```
 */
export function verifyWebhook(
  rawBody: string | Buffer,
  signatureHeader: string | undefined,
  secret: string,
  options: { toleranceSeconds?: number } = {}
): VerificationResult {
  const toleranceSeconds = options.toleranceSeconds ?? DEFAULT_TOLERANCE_SECONDS

  // Validate inputs
  if (!signatureHeader) {
    return { valid: false, reason: 'Missing signature header' }
  }

  if (!secret) {
    return { valid: false, reason: 'Webhook secret not configured' }
  }

  // Parse signature header (format: t=<timestamp>,v1=<signature>)
  const parsed = parseSignatureHeader(signatureHeader)
  if (!parsed) {
    return { valid: false, reason: 'Invalid signature header format' }
  }

  const { timestamp, signature } = parsed

  // Check timestamp tolerance
  const now = Math.floor(Date.now() / 1000)
  const timeDiff = Math.abs(now - timestamp)
  if (timeDiff > toleranceSeconds) {
    return {
      valid: false,
      reason: `Timestamp outside tolerance (${timeDiff}s > ${toleranceSeconds}s)`
    }
  }

  // Generate expected signature
  const body = typeof rawBody === 'string' ? rawBody : rawBody.toString('utf-8')
  const signatureMessage = `${timestamp}.${body}`
  const expectedSignature = createHmac('sha256', secret)
    .update(signatureMessage)
    .digest('hex')

  // Timing-safe comparison
  try {
    const sigBuffer = Buffer.from(signature, 'hex')
    const expectedBuffer = Buffer.from(expectedSignature, 'hex')

    if (sigBuffer.length !== expectedBuffer.length) {
      return { valid: false, reason: 'Signature length mismatch' }
    }

    if (!timingSafeEqual(sigBuffer, expectedBuffer)) {
      return { valid: false, reason: 'Signature mismatch' }
    }
  } catch {
    return { valid: false, reason: 'Signature comparison error' }
  }

  // Parse and validate payload
  try {
    const json = JSON.parse(body)
    const result = WebhookPayloadSchema.safeParse(json)

    if (!result.success) {
      return { valid: false, reason: 'Invalid payload schema' }
    }

    return { valid: true, payload: json as WebhookPayload }
  } catch {
    return { valid: false, reason: 'Invalid JSON payload' }
  }
}

/**
 * Parse signature header into components
 */
function parseSignatureHeader(header: string): { timestamp: number; signature: string } | null {
  try {
    const parts = header.split(',')
    const values: Record<string, string> = {}

    for (const part of parts) {
      const [key, value] = part.split('=')
      if (key && value) {
        values[key.trim()] = value.trim()
      }
    }

    if (!values['t'] || !values['v1']) {
      return null
    }

    return {
      timestamp: parseInt(values['t'], 10),
      signature: values['v1'],
    }
  } catch {
    return null
  }
}

// =============================================================================
// WEBHOOK HANDLER
// =============================================================================

export type WebhookHandler<T = unknown> = (payload: WebhookPayload<T>) => void | Promise<void>

export interface WebhookRouter {
  on<T = unknown>(eventType: EventType, handler: WebhookHandler<T>): void
  onAny(handler: WebhookHandler): void
  handle(payload: WebhookPayload): Promise<void>
}

/**
 * Create a webhook router for handling different event types
 *
 * @example
 * ```typescript
 * import { createWebhookRouter, verifyWebhook } from '@revops/sdk/webhooks'
 *
 * const router = createWebhookRouter()
 *
 * router.on('tenant.created', async (payload) => {
 *   console.log('New tenant:', payload.data)
 * })
 *
 * router.on('health.alert_triggered', async (payload) => {
 *   await sendSlackAlert(payload.data)
 * })
 *
 * app.post('/webhooks/revops', async (req, res) => {
 *   const result = verifyWebhook(req.body, req.headers['x-revops-signature'], secret)
 *   if (!result.valid) return res.status(401).send()
 *
 *   await router.handle(result.payload)
 *   res.status(200).send()
 * })
 * ```
 */
export function createWebhookRouter(): WebhookRouter {
  const handlers = new Map<EventType | '*', WebhookHandler[]>()

  return {
    on<T = unknown>(eventType: EventType, handler: WebhookHandler<T>) {
      const existing = handlers.get(eventType) || []
      existing.push(handler as WebhookHandler)
      handlers.set(eventType, existing)
    },

    onAny(handler: WebhookHandler) {
      const existing = handlers.get('*' as EventType) || []
      existing.push(handler)
      handlers.set('*' as EventType, existing)
    },

    async handle(payload: WebhookPayload) {
      // Run specific handlers
      const specificHandlers = handlers.get(payload.eventType) || []
      for (const handler of specificHandlers) {
        await handler(payload)
      }

      // Run catch-all handlers
      const anyHandlers = handlers.get('*' as EventType) || []
      for (const handler of anyHandlers) {
        await handler(payload)
      }
    },
  }
}

// =============================================================================
// EXPRESS MIDDLEWARE
// =============================================================================

/**
 * Express middleware for webhook verification
 *
 * @example
 * ```typescript
 * import express from 'express'
 * import { webhookMiddleware } from '@revops/sdk/webhooks'
 *
 * const app = express()
 *
 * app.post('/webhooks/revops',
 *   express.raw({ type: 'application/json' }),
 *   webhookMiddleware({ secret: process.env.REVOPS_WEBHOOK_SECRET }),
 *   (req, res) => {
 *     // req.webhook contains the verified payload
 *     console.log('Event:', req.webhook.eventType)
 *     res.status(200).send()
 *   }
 * )
 * ```
 */
export function webhookMiddleware(options: { secret: string; toleranceSeconds?: number }) {
  return (req: any, res: any, next: any) => {
    const body = typeof req.body === 'string' ? req.body :
                 Buffer.isBuffer(req.body) ? req.body.toString('utf-8') :
                 JSON.stringify(req.body)

    const result = verifyWebhook(
      body,
      req.headers[SIGNATURE_HEADER],
      options.secret,
      { toleranceSeconds: options.toleranceSeconds }
    )

    if (!result.valid) {
      return res.status(401).json({ error: result.reason })
    }

    req.webhook = result.payload
    next()
  }
}
