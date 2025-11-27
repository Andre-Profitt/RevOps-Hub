/**
 * Billing Service
 * Subscription management and plan enforcement
 */

import type {
  Subscription,
  PlanTier,
  PlanStatus,
  BillingCycle,
  PlanChangeRequest,
  PlanChangeResult,
  PlanLimits,
} from './types'
import { PLAN_DEFINITIONS, getPlanById, getPlanByTier } from './types'
import { getEventBus } from '../events/bus'

// =============================================================================
// SERVICE INTERFACE
// =============================================================================

interface BillingDependencies {
  getSubscription: (customerId: string) => Promise<Subscription | null>
  saveSubscription: (subscription: Subscription) => Promise<void>
  createInvoice: (customerId: string, amount: number, description: string) => Promise<string>
  processPayment: (customerId: string, amount: number) => Promise<boolean>
}

// =============================================================================
// BILLING SERVICE
// =============================================================================

export class BillingService {
  private deps: BillingDependencies

  constructor(deps: BillingDependencies) {
    this.deps = deps
  }

  /**
   * Get current subscription for customer
   */
  async getSubscription(customerId: string): Promise<Subscription | null> {
    return this.deps.getSubscription(customerId)
  }

  /**
   * Get plan limits for customer
   */
  async getPlanLimits(customerId: string): Promise<PlanLimits | null> {
    const subscription = await this.getSubscription(customerId)
    if (!subscription) return null

    const plan = getPlanById(subscription.planId)
    return plan?.limits ?? null
  }

  /**
   * Check if customer has access to a feature
   */
  async hasFeature(customerId: string, feature: keyof PlanLimits): Promise<boolean> {
    const limits = await this.getPlanLimits(customerId)
    if (!limits) return false

    const value = limits[feature]
    if (typeof value === 'boolean') return value
    if (value === 'unlimited') return true
    if (typeof value === 'number') return value > 0

    return false
  }

  /**
   * Check if customer is within usage limits
   */
  async checkLimit(
    customerId: string,
    limitType: 'maxUsers' | 'maxDeals' | 'maxDashboards',
    currentUsage: number
  ): Promise<{ allowed: boolean; limit: number | 'unlimited'; usage: number; remaining: number | 'unlimited' }> {
    const limits = await this.getPlanLimits(customerId)

    if (!limits) {
      return { allowed: false, limit: 0, usage: currentUsage, remaining: 0 }
    }

    const limit = limits[limitType]

    if (limit === 'unlimited') {
      return { allowed: true, limit: 'unlimited', usage: currentUsage, remaining: 'unlimited' }
    }

    const remaining = limit - currentUsage
    return {
      allowed: currentUsage < limit,
      limit,
      usage: currentUsage,
      remaining: Math.max(0, remaining),
    }
  }

  /**
   * Create a new subscription
   */
  async createSubscription(
    customerId: string,
    tier: PlanTier,
    billingCycle: BillingCycle,
    trialDays = 0
  ): Promise<Subscription> {
    const plan = getPlanByTier(tier)
    if (!plan) {
      throw new Error(`Invalid tier: ${tier}`)
    }

    const now = new Date()
    const periodEnd = new Date(now)
    periodEnd.setMonth(periodEnd.getMonth() + (billingCycle === 'annual' ? 12 : 1))

    const trialEnd = trialDays > 0 ? new Date(now.getTime() + trialDays * 24 * 60 * 60 * 1000) : null

    const subscription: Subscription = {
      id: `sub_${Date.now()}_${Math.random().toString(36).slice(2)}`,
      customerId,
      planId: plan.id,
      tier,
      status: trialDays > 0 ? 'trialing' : 'active',
      billingCycle,
      currentPeriodStart: now,
      currentPeriodEnd: periodEnd,
      cancelAtPeriodEnd: false,
      trialEnd,
      createdAt: now,
      updatedAt: now,
    }

    await this.deps.saveSubscription(subscription)

    // Emit event
    const eventBus = getEventBus()
    eventBus.emit({
      eventId: `evt_${Date.now()}`,
      type: 'billing.plan_changed',
      customerId,
      timestamp: now,
      data: {
        subscriptionId: subscription.id,
        tier,
        status: subscription.status,
      },
    })

    return subscription
  }

  /**
   * Change subscription plan (upgrade/downgrade)
   */
  async changePlan(request: PlanChangeRequest): Promise<PlanChangeResult> {
    const subscription = await this.getSubscription(request.customerId)
    if (!subscription) {
      return { success: false, error: 'No active subscription found' }
    }

    const fromPlan = getPlanById(request.fromPlanId)
    const toPlan = getPlanById(request.toPlanId)

    if (!fromPlan || !toPlan) {
      return { success: false, error: 'Invalid plan ID' }
    }

    // Calculate proration if immediate
    let prorationAmount = 0
    if (request.effectiveDate === 'immediate') {
      const daysRemaining = Math.ceil(
        (subscription.currentPeriodEnd.getTime() - Date.now()) / (24 * 60 * 60 * 1000)
      )
      const totalDays = subscription.billingCycle === 'annual' ? 365 : 30

      const fromDailyRate = fromPlan.pricing[subscription.billingCycle === 'annual' ? 'annual' : 'monthly'] / totalDays
      const toDailyRate = toPlan.pricing[subscription.billingCycle === 'annual' ? 'annual' : 'monthly'] / totalDays

      prorationAmount = (toDailyRate - fromDailyRate) * daysRemaining
    }

    // Process payment if upgrade
    if (prorationAmount > 0) {
      const paymentSuccess = await this.deps.processPayment(request.customerId, prorationAmount)
      if (!paymentSuccess) {
        return { success: false, error: 'Payment failed', prorationAmount }
      }
    }

    // Update subscription
    const updatedSubscription: Subscription = {
      ...subscription,
      planId: toPlan.id,
      tier: toPlan.tier,
      updatedAt: new Date(),
    }

    await this.deps.saveSubscription(updatedSubscription)

    // Emit event
    const eventBus = getEventBus()
    eventBus.emit({
      eventId: `evt_${Date.now()}`,
      type: 'billing.plan_changed',
      customerId: request.customerId,
      timestamp: new Date(),
      data: {
        subscriptionId: subscription.id,
        fromTier: fromPlan.tier,
        toTier: toPlan.tier,
        prorationAmount,
        reason: request.reason,
      },
    })

    return {
      success: true,
      subscription: updatedSubscription,
      prorationAmount,
    }
  }

  /**
   * Cancel subscription
   */
  async cancelSubscription(
    customerId: string,
    immediately = false
  ): Promise<{ success: boolean; effectiveDate: Date }> {
    const subscription = await this.getSubscription(customerId)
    if (!subscription) {
      throw new Error('No active subscription found')
    }

    const effectiveDate = immediately ? new Date() : subscription.currentPeriodEnd

    const updatedSubscription: Subscription = {
      ...subscription,
      status: immediately ? 'canceled' : subscription.status,
      cancelAtPeriodEnd: !immediately,
      updatedAt: new Date(),
    }

    await this.deps.saveSubscription(updatedSubscription)

    return { success: true, effectiveDate }
  }

  /**
   * Reactivate canceled subscription
   */
  async reactivateSubscription(customerId: string): Promise<Subscription> {
    const subscription = await this.getSubscription(customerId)
    if (!subscription) {
      throw new Error('No subscription found')
    }

    if (subscription.status === 'canceled') {
      throw new Error('Cannot reactivate fully canceled subscription')
    }

    const updatedSubscription: Subscription = {
      ...subscription,
      cancelAtPeriodEnd: false,
      updatedAt: new Date(),
    }

    await this.deps.saveSubscription(updatedSubscription)

    return updatedSubscription
  }

  /**
   * Get upgrade options for customer
   */
  async getUpgradeOptions(customerId: string): Promise<{
    currentPlan: PlanTier
    availableUpgrades: Array<{
      tier: PlanTier
      name: string
      monthlyPrice: number
      annualPrice: number
      additionalFeatures: string[]
    }>
  }> {
    const subscription = await this.getSubscription(customerId)
    if (!subscription) {
      throw new Error('No subscription found')
    }

    const currentPlan = getPlanByTier(subscription.tier)
    if (!currentPlan) {
      throw new Error('Invalid current plan')
    }

    const tierOrder: PlanTier[] = ['starter', 'growth', 'enterprise']
    const currentIndex = tierOrder.indexOf(subscription.tier)

    const availableUpgrades = PLAN_DEFINITIONS
      .filter(plan => tierOrder.indexOf(plan.tier) > currentIndex)
      .map(plan => ({
        tier: plan.tier,
        name: plan.name,
        monthlyPrice: plan.pricing.monthly,
        annualPrice: plan.pricing.annual,
        additionalFeatures: plan.features.filter(f => !currentPlan.features.includes(f)),
      }))

    return {
      currentPlan: subscription.tier,
      availableUpgrades,
    }
  }
}

// =============================================================================
// SINGLETON
// =============================================================================

let billingServiceInstance: BillingService | null = null

export function getBillingService(deps?: BillingDependencies): BillingService {
  if (!billingServiceInstance && deps) {
    billingServiceInstance = new BillingService(deps)
  }
  if (!billingServiceInstance) {
    throw new Error('BillingService not initialized')
  }
  return billingServiceInstance
}
