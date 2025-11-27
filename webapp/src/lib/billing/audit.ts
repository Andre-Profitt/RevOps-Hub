/**
 * Subscription Audit Logging
 * Track all changes to subscriptions for compliance and debugging
 */

import { v4 as uuidv4 } from 'uuid'
import type { SubscriptionEvent, SubscriptionEventType } from './types'

// =============================================================================
// DEPENDENCIES
// =============================================================================

interface AuditDependencies {
  saveEvent: (event: SubscriptionEvent) => Promise<void>
  getEvents: (subscriptionId: string, limit?: number) => Promise<SubscriptionEvent[]>
  getEventsByTenant: (tenantId: string, limit?: number) => Promise<SubscriptionEvent[]>
  getEventsByType: (type: SubscriptionEventType, since?: Date) => Promise<SubscriptionEvent[]>
}

// =============================================================================
// AUDIT LOGGER
// =============================================================================

export class AuditLogger {
  private deps: AuditDependencies
  private buffer: SubscriptionEvent[] = []
  private flushInterval: NodeJS.Timeout | null = null

  constructor(deps: AuditDependencies) {
    this.deps = deps
  }

  /**
   * Start background flushing
   */
  start(flushIntervalMs = 5000): void {
    if (this.flushInterval) return

    this.flushInterval = setInterval(() => {
      this.flush().catch(console.error)
    }, flushIntervalMs)
  }

  /**
   * Stop background flushing
   */
  stop(): void {
    if (this.flushInterval) {
      clearInterval(this.flushInterval)
      this.flushInterval = null
    }
  }

  /**
   * Log an audit event
   */
  async log(
    subscriptionId: string,
    tenantId: string,
    type: SubscriptionEventType,
    actor: 'system' | string,
    options: {
      previousValue?: unknown
      newValue?: unknown
      metadata?: Record<string, unknown>
    } = {}
  ): Promise<void> {
    const event: SubscriptionEvent = {
      id: uuidv4(),
      subscriptionId,
      tenantId,
      type,
      actor,
      previousValue: options.previousValue,
      newValue: options.newValue,
      metadata: options.metadata,
      occurredAt: new Date(),
    }

    this.buffer.push(event)

    // Flush critical events immediately
    if (this.isCriticalEvent(type)) {
      await this.flush()
    }
  }

  /**
   * Check if event type is critical (should be flushed immediately)
   */
  private isCriticalEvent(type: SubscriptionEventType): boolean {
    const criticalTypes: SubscriptionEventType[] = [
      'payment.failed',
      'subscription.canceled',
      'usage.hard_cap_hit',
    ]
    return criticalTypes.includes(type)
  }

  /**
   * Flush buffered events to storage
   */
  async flush(): Promise<void> {
    if (this.buffer.length === 0) return

    const events = [...this.buffer]
    this.buffer = []

    for (const event of events) {
      await this.deps.saveEvent(event)
    }
  }

  /**
   * Get audit trail for a subscription
   */
  async getSubscriptionAuditTrail(
    subscriptionId: string,
    limit = 100
  ): Promise<SubscriptionEvent[]> {
    return this.deps.getEvents(subscriptionId, limit)
  }

  /**
   * Get audit trail for a tenant
   */
  async getTenantAuditTrail(
    tenantId: string,
    limit = 100
  ): Promise<SubscriptionEvent[]> {
    return this.deps.getEventsByTenant(tenantId, limit)
  }

  /**
   * Get events by type (for analytics)
   */
  async getEventsByType(
    type: SubscriptionEventType,
    since?: Date
  ): Promise<SubscriptionEvent[]> {
    return this.deps.getEventsByType(type, since)
  }
}

// =============================================================================
// CONVENIENCE LOGGING FUNCTIONS
// =============================================================================

let auditLoggerInstance: AuditLogger | null = null

export function getAuditLogger(deps?: AuditDependencies): AuditLogger {
  if (!auditLoggerInstance && deps) {
    auditLoggerInstance = new AuditLogger(deps)
  }
  if (!auditLoggerInstance) {
    throw new Error('AuditLogger not initialized')
  }
  return auditLoggerInstance
}

// Subscription lifecycle events
export async function logSubscriptionCreated(
  subscriptionId: string,
  tenantId: string,
  actor: string,
  subscriptionData: unknown
): Promise<void> {
  const logger = getAuditLogger()
  await logger.log(subscriptionId, tenantId, 'subscription.created', actor, {
    newValue: subscriptionData,
  })
}

export async function logSubscriptionActivated(
  subscriptionId: string,
  tenantId: string,
  actor: string = 'system'
): Promise<void> {
  const logger = getAuditLogger()
  await logger.log(subscriptionId, tenantId, 'subscription.activated', actor)
}

export async function logPlanChanged(
  subscriptionId: string,
  tenantId: string,
  actor: string,
  previousPlan: string,
  newPlan: string,
  reason?: string
): Promise<void> {
  const logger = getAuditLogger()
  await logger.log(subscriptionId, tenantId, 'subscription.plan_changed', actor, {
    previousValue: previousPlan,
    newValue: newPlan,
    metadata: { reason },
  })
}

export async function logDiscountApplied(
  subscriptionId: string,
  tenantId: string,
  actor: string,
  discountPercent: number,
  reason?: string
): Promise<void> {
  const logger = getAuditLogger()
  await logger.log(subscriptionId, tenantId, 'subscription.discount_applied', actor, {
    newValue: discountPercent,
    metadata: { reason },
  })
}

export async function logDiscountRemoved(
  subscriptionId: string,
  tenantId: string,
  actor: string,
  previousDiscount: number
): Promise<void> {
  const logger = getAuditLogger()
  await logger.log(subscriptionId, tenantId, 'subscription.discount_removed', actor, {
    previousValue: previousDiscount,
  })
}

export async function logAddOnAdded(
  subscriptionId: string,
  tenantId: string,
  actor: string,
  addOnCode: string,
  quantity: number
): Promise<void> {
  const logger = getAuditLogger()
  await logger.log(subscriptionId, tenantId, 'subscription.addon_added', actor, {
    newValue: { addOnCode, quantity },
  })
}

export async function logAddOnRemoved(
  subscriptionId: string,
  tenantId: string,
  actor: string,
  addOnCode: string
): Promise<void> {
  const logger = getAuditLogger()
  await logger.log(subscriptionId, tenantId, 'subscription.addon_removed', actor, {
    previousValue: addOnCode,
  })
}

export async function logSubscriptionCanceled(
  subscriptionId: string,
  tenantId: string,
  actor: string,
  reason?: string,
  immediately?: boolean
): Promise<void> {
  const logger = getAuditLogger()
  await logger.log(subscriptionId, tenantId, 'subscription.canceled', actor, {
    metadata: { reason, immediately },
  })
}

export async function logSubscriptionReactivated(
  subscriptionId: string,
  tenantId: string,
  actor: string
): Promise<void> {
  const logger = getAuditLogger()
  await logger.log(subscriptionId, tenantId, 'subscription.reactivated', actor)
}

export async function logSubscriptionRenewed(
  subscriptionId: string,
  tenantId: string,
  newEndDate: Date
): Promise<void> {
  const logger = getAuditLogger()
  await logger.log(subscriptionId, tenantId, 'subscription.renewed', 'system', {
    newValue: newEndDate.toISOString(),
  })
}

// Payment events
export async function logPaymentMethodAdded(
  subscriptionId: string,
  tenantId: string,
  actor: string,
  paymentMethodType: string,
  last4?: string
): Promise<void> {
  const logger = getAuditLogger()
  await logger.log(subscriptionId, tenantId, 'payment.method_added', actor, {
    newValue: { type: paymentMethodType, last4 },
  })
}

export async function logPaymentMethodRemoved(
  subscriptionId: string,
  tenantId: string,
  actor: string,
  paymentMethodId: string
): Promise<void> {
  const logger = getAuditLogger()
  await logger.log(subscriptionId, tenantId, 'payment.method_removed', actor, {
    previousValue: paymentMethodId,
  })
}

export async function logPaymentSucceeded(
  subscriptionId: string,
  tenantId: string,
  invoiceId: string,
  amount: number,
  currency: string
): Promise<void> {
  const logger = getAuditLogger()
  await logger.log(subscriptionId, tenantId, 'payment.succeeded', 'system', {
    metadata: { invoiceId, amount, currency },
  })
}

export async function logPaymentFailed(
  subscriptionId: string,
  tenantId: string,
  invoiceId: string,
  amount: number,
  errorMessage: string
): Promise<void> {
  const logger = getAuditLogger()
  await logger.log(subscriptionId, tenantId, 'payment.failed', 'system', {
    metadata: { invoiceId, amount, errorMessage },
  })
}

// Invoice events
export async function logInvoiceCreated(
  subscriptionId: string,
  tenantId: string,
  invoiceId: string,
  total: number
): Promise<void> {
  const logger = getAuditLogger()
  await logger.log(subscriptionId, tenantId, 'invoice.created', 'system', {
    newValue: { invoiceId, total },
  })
}

export async function logInvoicePaid(
  subscriptionId: string,
  tenantId: string,
  invoiceId: string,
  amount: number
): Promise<void> {
  const logger = getAuditLogger()
  await logger.log(subscriptionId, tenantId, 'invoice.paid', 'system', {
    metadata: { invoiceId, amount },
  })
}

export async function logInvoiceVoided(
  subscriptionId: string,
  tenantId: string,
  actor: string,
  invoiceId: string,
  reason: string
): Promise<void> {
  const logger = getAuditLogger()
  await logger.log(subscriptionId, tenantId, 'invoice.voided', actor, {
    metadata: { invoiceId, reason },
  })
}

// Usage events
export async function logUsageThresholdReached(
  subscriptionId: string,
  tenantId: string,
  meter: string,
  threshold: number,
  used: number,
  included: number
): Promise<void> {
  const logger = getAuditLogger()
  await logger.log(subscriptionId, tenantId, 'usage.threshold_reached', 'system', {
    metadata: { meter, threshold, used, included },
  })
}

export async function logHardCapHit(
  subscriptionId: string,
  tenantId: string,
  meter: string,
  limit: number
): Promise<void> {
  const logger = getAuditLogger()
  await logger.log(subscriptionId, tenantId, 'usage.hard_cap_hit', 'system', {
    metadata: { meter, limit },
  })
}
