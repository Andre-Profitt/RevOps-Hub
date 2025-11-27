/**
 * Webhook Signature
 * HMAC signature generation and verification
 */

import { createHmac, timingSafeEqual } from 'crypto'
import type { WebhookSignature } from './types'

// =============================================================================
// SIGNATURE GENERATION
// =============================================================================

/**
 * Generate HMAC signature for webhook payload
 */
export function generateSignature(
  payload: string,
  secret: string,
  timestamp?: number
): WebhookSignature {
  const ts = timestamp || Math.floor(Date.now() / 1000)

  // Create signature message: timestamp.payload
  const signatureMessage = `${ts}.${payload}`

  // Generate HMAC-SHA256
  const hmac = createHmac('sha256', secret)
  hmac.update(signatureMessage)
  const signature = hmac.digest('hex')

  return {
    algorithm: 'sha256',
    timestamp: ts,
    signature,
  }
}

/**
 * Generate signature header value
 * Format: t=<timestamp>,v1=<signature>
 */
export function generateSignatureHeader(
  payload: string,
  secret: string,
  timestamp?: number
): string {
  const sig = generateSignature(payload, secret, timestamp)
  return `t=${sig.timestamp},v1=${sig.signature}`
}

// =============================================================================
// SIGNATURE VERIFICATION
// =============================================================================

/**
 * Parse signature header
 */
export function parseSignatureHeader(header: string): WebhookSignature | null {
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
      algorithm: 'sha256',
      timestamp: parseInt(values['t'], 10),
      signature: values['v1'],
    }
  } catch {
    return null
  }
}

/**
 * Verify webhook signature
 */
export function verifySignature(
  payload: string,
  secret: string,
  signatureHeader: string,
  toleranceSeconds = 300
): { valid: boolean; reason?: string } {
  // Parse header
  const parsedSig = parseSignatureHeader(signatureHeader)
  if (!parsedSig) {
    return { valid: false, reason: 'Invalid signature header format' }
  }

  // Check timestamp tolerance
  const now = Math.floor(Date.now() / 1000)
  const timeDiff = Math.abs(now - parsedSig.timestamp)
  if (timeDiff > toleranceSeconds) {
    return { valid: false, reason: `Timestamp outside tolerance (${timeDiff}s > ${toleranceSeconds}s)` }
  }

  // Generate expected signature
  const expected = generateSignature(payload, secret, parsedSig.timestamp)

  // Timing-safe comparison
  try {
    const sigBuffer = Buffer.from(parsedSig.signature, 'hex')
    const expectedBuffer = Buffer.from(expected.signature, 'hex')

    if (sigBuffer.length !== expectedBuffer.length) {
      return { valid: false, reason: 'Signature length mismatch' }
    }

    const isValid = timingSafeEqual(sigBuffer, expectedBuffer)
    if (!isValid) {
      return { valid: false, reason: 'Signature mismatch' }
    }

    return { valid: true }
  } catch {
    return { valid: false, reason: 'Signature comparison error' }
  }
}

// =============================================================================
// SECRET GENERATION
// =============================================================================

/**
 * Generate a secure webhook secret
 */
export function generateWebhookSecret(): string {
  const { randomBytes } = require('crypto')
  return `whsec_${randomBytes(32).toString('hex')}`
}

/**
 * Validate webhook secret format
 */
export function isValidSecret(secret: string): boolean {
  return /^whsec_[a-f0-9]{64}$/.test(secret)
}
