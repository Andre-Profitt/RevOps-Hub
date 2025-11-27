/**
 * Usage Tracking v2
 * Meter-based usage tracking with aggregation and enforcement
 */

import { v4 as uuidv4 } from 'uuid'
import type {
  MeterName,
  UsageRecord,
  UsageAggregate,
  EnforcementResult,
  LimitDefinition,
  Subscription,
  SubscriptionAddOn,
  PlanTier,
} from './types'
import { getPlanByCode, getAddOnByCode, getMeterDisplayName } from './types'
import { getEventBus } from '../events/bus'

// =============================================================================
// DEPENDENCIES
// =============================================================================

interface UsageDependencies {
  // Storage
  saveUsageRecord: (record: UsageRecord) => Promise<void>
  getUsageAggregate: (tenantId: string, meter: MeterName, period: string) => Promise<UsageAggregate | null>
  upsertUsageAggregate: (aggregate: UsageAggregate) => Promise<void>
  getUsageAggregatesForPeriod: (tenantId: string, period: string) => Promise<UsageAggregate[]>

  // Subscription context
  getSubscription: (tenantId: string) => Promise<Subscription | null>
  getSubscriptionAddOns: (subscriptionId: string) => Promise<SubscriptionAddOn[]>
}

// =============================================================================
// USAGE TRACKER
// =============================================================================

export class UsageTracker {
  private deps: UsageDependencies
  private buffer: UsageRecord[] = []
  private flushInterval: NodeJS.Timeout | null = null
  private thresholdAlertsSent: Map<string, Set<number>> = new Map() // tenantId:meter -> thresholds

  constructor(deps: UsageDependencies) {
    this.deps = deps
  }

  /**
   * Start background flushing of usage records
   */
  start(flushIntervalMs = 30000): void {
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
   * Record a usage event for a meter
   */
  async record(
    tenantId: string,
    subscriptionId: string,
    meter: MeterName,
    quantity: number = 1,
    metadata?: Record<string, unknown>
  ): Promise<void> {
    const record: UsageRecord = {
      id: uuidv4(),
      tenantId,
      subscriptionId,
      meter,
      quantity,
      occurredAt: new Date(),
      metadata,
    }

    this.buffer.push(record)

    // Immediately update aggregate for real-time tracking
    await this.updateAggregate(tenantId, subscriptionId, meter, quantity)

    // Check thresholds
    await this.checkThresholds(tenantId, meter)
  }

  /**
   * Flush buffered records to storage
   */
  async flush(): Promise<void> {
    if (this.buffer.length === 0) return

    const records = [...this.buffer]
    this.buffer = []

    for (const record of records) {
      await this.deps.saveUsageRecord(record)
    }
  }

  /**
   * Update usage aggregate
   */
  private async updateAggregate(
    tenantId: string,
    subscriptionId: string,
    meter: MeterName,
    quantity: number
  ): Promise<void> {
    const period = this.getCurrentPeriod()
    const existing = await this.deps.getUsageAggregate(tenantId, meter, period)

    const aggregate: UsageAggregate = {
      id: existing?.id ?? uuidv4(),
      tenantId,
      subscriptionId,
      meter,
      period,
      used: (existing?.used ?? 0) + quantity,
      lastUpdatedAt: new Date(),
    }

    await this.deps.upsertUsageAggregate(aggregate)
  }

  /**
   * Get current billing period (YYYY-MM format)
   */
  private getCurrentPeriod(): string {
    const now = new Date()
    return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`
  }

  /**
   * Check usage thresholds and emit alerts
   */
  private async checkThresholds(tenantId: string, meter: MeterName): Promise<void> {
    const key = `${tenantId}:${meter}`
    const sentThresholds = this.thresholdAlertsSent.get(key) ?? new Set()

    const enforcement = await this.checkEnforcement(tenantId, meter)
    if (enforcement.included === 'unlimited') return

    const usagePercent = (enforcement.used / enforcement.included) * 100
    const thresholds = [70, 90, 100]

    for (const threshold of thresholds) {
      if (usagePercent >= threshold && !sentThresholds.has(threshold)) {
        sentThresholds.add(threshold)
        this.thresholdAlertsSent.set(key, sentThresholds)

        // Emit threshold event
        const eventBus = getEventBus()
        eventBus.emit({
          eventId: uuidv4(),
          type: 'billing.usage_threshold',
          customerId: tenantId,
          timestamp: new Date(),
          data: {
            meter,
            meterDisplayName: getMeterDisplayName(meter),
            threshold,
            used: enforcement.used,
            included: enforcement.included,
            remaining: enforcement.remaining,
            overage: enforcement.overage,
          },
        })

        // If hard cap hit, emit special event
        if (threshold === 100 && enforcement.capType === 'hard') {
          eventBus.emit({
            eventId: uuidv4(),
            type: 'health.alert_triggered',
            customerId: tenantId,
            timestamp: new Date(),
            data: {
              alertType: 'usage_hard_cap',
              meter,
              message: `Hard cap reached for ${getMeterDisplayName(meter)}`,
            },
          })
        }
      }
    }
  }

  /**
   * Check enforcement for a specific meter
   */
  async checkEnforcement(tenantId: string, meter: MeterName): Promise<EnforcementResult> {
    const subscription = await this.deps.getSubscription(tenantId)

    if (!subscription) {
      return {
        allowed: false,
        meter,
        included: 0,
        used: 0,
        remaining: 0,
        overage: 0,
        overageCost: 0,
        capType: 'hard',
        message: 'No active subscription',
      }
    }

    // Get plan limits
    const plan = getPlanByCode(subscription.planCode)
    if (!plan) {
      return {
        allowed: false,
        meter,
        included: 0,
        used: 0,
        remaining: 0,
        overage: 0,
        overageCost: 0,
        capType: 'hard',
        message: 'Invalid plan',
      }
    }

    // Find limit for this meter
    let limitDef = plan.limits.find(l => l.meter === meter)

    // Check add-ons for additional limits
    const addOns = await this.deps.getSubscriptionAddOns(subscription.id)
    for (const addOn of addOns) {
      if (addOn.deactivatedAt) continue
      const addOnDef = getAddOnByCode(addOn.addOnCode)
      if (addOnDef?.limits) {
        const addOnLimit = addOnDef.limits.find(l => l.meter === meter)
        if (addOnLimit) {
          // Merge limits (add-on provides additional quota)
          if (limitDef) {
            const existingIncluded = limitDef.included === 'unlimited' ? Infinity : limitDef.included
            const addOnIncluded = addOnLimit.included === 'unlimited' ? Infinity : addOnLimit.included
            limitDef = {
              ...limitDef,
              included: existingIncluded === Infinity || addOnIncluded === Infinity
                ? 'unlimited'
                : existingIncluded + addOnIncluded,
            }
          } else {
            limitDef = addOnLimit
          }
        }
      }
    }

    // If no limit defined, allow unlimited
    if (!limitDef) {
      return {
        allowed: true,
        meter,
        included: 'unlimited',
        used: 0,
        remaining: 'unlimited',
        overage: 0,
        overageCost: 0,
        capType: 'none',
      }
    }

    // Get current usage
    const period = this.getCurrentPeriod()
    const aggregate = await this.deps.getUsageAggregate(tenantId, meter, period)
    const used = aggregate?.used ?? 0

    // Calculate enforcement
    const included = limitDef.included
    if (included === 'unlimited') {
      return {
        allowed: true,
        meter,
        included: 'unlimited',
        used,
        remaining: 'unlimited',
        overage: 0,
        overageCost: 0,
        capType: 'none',
      }
    }

    const remaining = Math.max(0, included - used)
    const overage = Math.max(0, used - included)
    const overageCost = overage * (limitDef.overagePrice ?? 0)

    // Determine if allowed
    let allowed = true
    let message: string | undefined

    if (used >= included) {
      if (limitDef.capType === 'hard') {
        allowed = false
        message = `You've reached your ${getMeterDisplayName(meter)} limit. Upgrade your plan to continue.`
      } else if (limitDef.capType === 'soft') {
        message = `You've exceeded your included ${getMeterDisplayName(meter)}. Overage charges will apply.`
      }
    }

    return {
      allowed,
      meter,
      included,
      used,
      remaining,
      overage,
      overageCost,
      capType: limitDef.capType,
      message,
    }
  }

  /**
   * Check if a usage increment would be allowed
   */
  async canUse(
    tenantId: string,
    meter: MeterName,
    quantity: number = 1
  ): Promise<{ allowed: boolean; message?: string }> {
    const enforcement = await this.checkEnforcement(tenantId, meter)

    if (!enforcement.allowed) {
      return { allowed: false, message: enforcement.message }
    }

    // For hard caps, check if increment would exceed
    if (enforcement.capType === 'hard' && enforcement.included !== 'unlimited') {
      const wouldExceed = enforcement.used + quantity > enforcement.included
      if (wouldExceed) {
        return {
          allowed: false,
          message: `This action would exceed your ${getMeterDisplayName(meter)} limit of ${enforcement.included}.`,
        }
      }
    }

    return { allowed: true }
  }

  /**
   * Get all usage for a tenant in current period
   */
  async getTenantUsage(tenantId: string): Promise<Map<MeterName, EnforcementResult>> {
    const results = new Map<MeterName, EnforcementResult>()
    const meters: MeterName[] = [
      'seats', 'deals', 'ai_insight_credits', 'agent_actions',
      'workspaces', 'data_storage_gb', 'api_calls', 'webhook_deliveries',
      'conversation_minutes', 'custom_model_inferences',
    ]

    for (const meter of meters) {
      results.set(meter, await this.checkEnforcement(tenantId, meter))
    }

    return results
  }

  /**
   * Calculate overage charges for billing period
   */
  async calculateOverages(tenantId: string, period?: string): Promise<{
    meter: MeterName
    used: number
    included: number
    overage: number
    unitPrice: number
    total: number
  }[]> {
    const targetPeriod = period ?? this.getCurrentPeriod()
    const aggregates = await this.deps.getUsageAggregatesForPeriod(tenantId, targetPeriod)
    const subscription = await this.deps.getSubscription(tenantId)

    if (!subscription) return []

    const plan = getPlanByCode(subscription.planCode)
    if (!plan) return []

    const overages: {
      meter: MeterName
      used: number
      included: number
      overage: number
      unitPrice: number
      total: number
    }[] = []

    for (const aggregate of aggregates) {
      const limitDef = plan.limits.find(l => l.meter === aggregate.meter)
      if (!limitDef || limitDef.included === 'unlimited' || !limitDef.overagePrice) continue

      const overage = Math.max(0, aggregate.used - limitDef.included)
      if (overage > 0) {
        overages.push({
          meter: aggregate.meter,
          used: aggregate.used,
          included: limitDef.included,
          overage,
          unitPrice: limitDef.overagePrice,
          total: overage * limitDef.overagePrice,
        })
      }
    }

    return overages
  }

  /**
   * Reset threshold alerts for new billing period
   */
  resetThresholdAlerts(tenantId?: string): void {
    if (tenantId) {
      // Reset for specific tenant
      for (const key of this.thresholdAlertsSent.keys()) {
        if (key.startsWith(tenantId)) {
          this.thresholdAlertsSent.delete(key)
        }
      }
    } else {
      // Reset all
      this.thresholdAlertsSent.clear()
    }
  }
}

// =============================================================================
// SINGLETON
// =============================================================================

let usageTrackerInstance: UsageTracker | null = null

export function getUsageTracker(deps?: UsageDependencies): UsageTracker {
  if (!usageTrackerInstance && deps) {
    usageTrackerInstance = new UsageTracker(deps)
  }
  if (!usageTrackerInstance) {
    throw new Error('UsageTracker not initialized')
  }
  return usageTrackerInstance
}

// =============================================================================
// CONVENIENCE FUNCTIONS
// =============================================================================

export async function trackUsage(
  tenantId: string,
  subscriptionId: string,
  meter: MeterName,
  quantity: number = 1,
  metadata?: Record<string, unknown>
): Promise<void> {
  const tracker = getUsageTracker()
  await tracker.record(tenantId, subscriptionId, meter, quantity, metadata)
}

export async function checkUsageAllowed(
  tenantId: string,
  meter: MeterName,
  quantity: number = 1
): Promise<{ allowed: boolean; message?: string }> {
  const tracker = getUsageTracker()
  return tracker.canUse(tenantId, meter, quantity)
}

export async function getUsageStatus(
  tenantId: string,
  meter: MeterName
): Promise<EnforcementResult> {
  const tracker = getUsageTracker()
  return tracker.checkEnforcement(tenantId, meter)
}

// =============================================================================
// METER-SPECIFIC HELPERS
// =============================================================================

export async function trackAIUsage(
  tenantId: string,
  subscriptionId: string,
  credits: number = 1,
  model?: string
): Promise<void> {
  await trackUsage(tenantId, subscriptionId, 'ai_insight_credits', credits, { model })
}

export async function trackAgentAction(
  tenantId: string,
  subscriptionId: string,
  agentType: string
): Promise<void> {
  await trackUsage(tenantId, subscriptionId, 'agent_actions', 1, { agentType })
}

export async function trackAPICall(
  tenantId: string,
  subscriptionId: string,
  endpoint: string
): Promise<void> {
  await trackUsage(tenantId, subscriptionId, 'api_calls', 1, { endpoint })
}

export async function trackWebhookDelivery(
  tenantId: string,
  subscriptionId: string,
  webhookId: string
): Promise<void> {
  await trackUsage(tenantId, subscriptionId, 'webhook_deliveries', 1, { webhookId })
}

export async function trackConversationMinutes(
  tenantId: string,
  subscriptionId: string,
  minutes: number,
  callId?: string
): Promise<void> {
  await trackUsage(tenantId, subscriptionId, 'conversation_minutes', minutes, { callId })
}

export async function trackCustomModelInference(
  tenantId: string,
  subscriptionId: string,
  modelId: string,
  inferences: number = 1
): Promise<void> {
  await trackUsage(tenantId, subscriptionId, 'custom_model_inferences', inferences, { modelId })
}

export async function setDealCount(
  tenantId: string,
  subscriptionId: string,
  totalDeals: number
): Promise<void> {
  // For absolute counts like deals, we need to calculate delta
  const tracker = getUsageTracker()
  const current = await tracker.checkEnforcement(tenantId, 'deals')
  const delta = totalDeals - current.used
  if (delta !== 0) {
    await trackUsage(tenantId, subscriptionId, 'deals', delta)
  }
}

export async function setSeatCount(
  tenantId: string,
  subscriptionId: string,
  totalSeats: number
): Promise<void> {
  const tracker = getUsageTracker()
  const current = await tracker.checkEnforcement(tenantId, 'seats')
  const delta = totalSeats - current.used
  if (delta !== 0) {
    await trackUsage(tenantId, subscriptionId, 'seats', delta)
  }
}
