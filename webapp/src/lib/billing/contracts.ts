/**
 * Contract & Subscription Management
 * Handles ramp schedules, renewals, approvals, and contract workflows
 */

import { v4 as uuidv4 } from 'uuid'
import type {
  Subscription,
  RampSchedule,
  RampStep,
  PlanTier,
  BillingInterval,
  ApprovalRequest,
  ApprovalStatus,
  ApprovalType,
  SubscriptionAddOn,
  AddOnCode,
} from './types'
import { getPlanByCode, getAddOnByCode } from './types'
import { getAuditLogger, logSubscriptionCreated, logPlanChanged, logDiscountApplied, logSubscriptionCanceled, logSubscriptionRenewed, logAddOnAdded, logAddOnRemoved } from './audit'
import { getEventBus } from '../events/bus'

// =============================================================================
// DEPENDENCIES
// =============================================================================

interface ContractDependencies {
  // Subscriptions
  getSubscription: (id: string) => Promise<Subscription | null>
  getSubscriptionByTenant: (tenantId: string) => Promise<Subscription | null>
  saveSubscription: (subscription: Subscription) => Promise<void>

  // Ramp schedules
  getRampSchedule: (id: string) => Promise<RampSchedule | null>
  saveRampSchedule: (schedule: RampSchedule) => Promise<void>

  // Add-ons
  getSubscriptionAddOns: (subscriptionId: string) => Promise<SubscriptionAddOn[]>
  saveSubscriptionAddOn: (addOn: SubscriptionAddOn) => Promise<void>

  // Approvals
  getApprovalRequest: (id: string) => Promise<ApprovalRequest | null>
  saveApprovalRequest: (request: ApprovalRequest) => Promise<void>
  getPendingApprovals: (subscriptionId: string) => Promise<ApprovalRequest[]>

  // Notifications
  sendEmail: (to: string, subject: string, body: string) => Promise<void>
  sendSlackNotification: (channel: string, message: string) => Promise<void>
}

// =============================================================================
// CONTRACT SERVICE
// =============================================================================

export class ContractService {
  private deps: ContractDependencies

  constructor(deps: ContractDependencies) {
    this.deps = deps
  }

  // ===========================================================================
  // SUBSCRIPTION CREATION
  // ===========================================================================

  /**
   * Create a new subscription
   */
  async createSubscription(params: {
    tenantId: string
    planCode: PlanTier
    billingInterval: BillingInterval
    termMonths?: number
    discountPercent?: number
    trialDays?: number
    rampSchedule?: Omit<RampStep, 'id' | 'rampScheduleId'>[]
    createdBy: string
  }): Promise<Subscription> {
    const plan = getPlanByCode(params.planCode)
    if (!plan) {
      throw new Error(`Invalid plan: ${params.planCode}`)
    }

    // Check if discount requires approval
    if (params.discountPercent && params.discountPercent > 20) {
      throw new Error('Discounts over 20% require approval. Use requestDiscountApproval instead.')
    }

    const now = new Date()
    const termMonths = params.termMonths ?? (params.billingInterval === 'annual' ? 12 : 1)

    const contractEndDate = new Date(now)
    contractEndDate.setMonth(contractEndDate.getMonth() + termMonths)

    const trialEnd = params.trialDays && params.trialDays > 0
      ? new Date(now.getTime() + params.trialDays * 24 * 60 * 60 * 1000)
      : undefined

    // Calculate contracted MRR
    const basePrice = params.billingInterval === 'annual'
      ? plan.pricing.annual / 12
      : plan.pricing.monthly
    const discountAmount = params.discountPercent
      ? basePrice * (params.discountPercent / 100)
      : 0
    const contractedMrr = basePrice - discountAmount

    const subscriptionId = uuidv4()

    const subscription: Subscription = {
      id: subscriptionId,
      tenantId: params.tenantId,
      planCode: params.planCode,
      status: trialEnd ? 'trialing' : 'active',
      basePricePerMonth: basePrice,
      discountPercent: params.discountPercent,
      contractedMrr,
      currency: plan.pricing.currency,
      billingInterval: params.billingInterval,
      contractStartDate: now,
      contractEndDate,
      termMonths,
      renewalDate: contractEndDate,
      renewalType: 'auto',
      trialEnd,
      createdAt: now,
      updatedAt: now,
      cancelAtPeriodEnd: false,
    }

    // Create ramp schedule if provided
    if (params.rampSchedule && params.rampSchedule.length > 0) {
      const rampScheduleId = uuidv4()
      const rampSchedule: RampSchedule = {
        id: rampScheduleId,
        subscriptionId,
        name: `Ramp for ${params.tenantId}`,
        steps: params.rampSchedule.map((step, i) => ({
          ...step,
          id: uuidv4(),
          rampScheduleId,
          stepNumber: i + 1,
        })),
        createdAt: now,
      }

      await this.deps.saveRampSchedule(rampSchedule)
      subscription.rampScheduleId = rampScheduleId
    }

    await this.deps.saveSubscription(subscription)

    // Audit log
    await logSubscriptionCreated(subscriptionId, params.tenantId, params.createdBy, subscription)

    // Emit event
    const eventBus = getEventBus()
    eventBus.emit({
      eventId: uuidv4(),
      type: 'billing.plan_changed',
      customerId: params.tenantId,
      timestamp: now,
      data: {
        subscriptionId,
        planCode: params.planCode,
        status: subscription.status,
      },
    })

    return subscription
  }

  // ===========================================================================
  // PLAN CHANGES
  // ===========================================================================

  /**
   * Change subscription plan (upgrade/downgrade)
   */
  async changePlan(
    subscriptionId: string,
    newPlanCode: PlanTier,
    options: {
      effectiveDate?: 'immediate' | 'end_of_period'
      reason?: string
      changedBy: string
    }
  ): Promise<Subscription> {
    const subscription = await this.deps.getSubscription(subscriptionId)
    if (!subscription) {
      throw new Error('Subscription not found')
    }

    const oldPlan = getPlanByCode(subscription.planCode)
    const newPlan = getPlanByCode(newPlanCode)

    if (!oldPlan || !newPlan) {
      throw new Error('Invalid plan')
    }

    // For downgrades, apply at end of period by default
    const isDowngrade = newPlan.sortOrder < oldPlan.sortOrder
    const effectiveDate = options.effectiveDate ?? (isDowngrade ? 'end_of_period' : 'immediate')

    // Update subscription
    const newBasePrice = subscription.billingInterval === 'annual'
      ? newPlan.pricing.annual / 12
      : newPlan.pricing.monthly

    const discountAmount = subscription.discountPercent
      ? newBasePrice * (subscription.discountPercent / 100)
      : 0

    const updatedSubscription: Subscription = {
      ...subscription,
      planCode: newPlanCode,
      basePricePerMonth: newBasePrice,
      contractedMrr: newBasePrice - discountAmount,
      updatedAt: new Date(),
    }

    await this.deps.saveSubscription(updatedSubscription)

    // Audit log
    await logPlanChanged(
      subscriptionId,
      subscription.tenantId,
      options.changedBy,
      subscription.planCode,
      newPlanCode,
      options.reason
    )

    return updatedSubscription
  }

  // ===========================================================================
  // DISCOUNTS
  // ===========================================================================

  /**
   * Apply a discount (up to 20%, higher requires approval)
   */
  async applyDiscount(
    subscriptionId: string,
    discountPercent: number,
    appliedBy: string,
    reason?: string
  ): Promise<Subscription> {
    if (discountPercent > 20) {
      throw new Error('Discounts over 20% require approval')
    }

    const subscription = await this.deps.getSubscription(subscriptionId)
    if (!subscription) {
      throw new Error('Subscription not found')
    }

    const discountAmount = subscription.basePricePerMonth * (discountPercent / 100)

    const updatedSubscription: Subscription = {
      ...subscription,
      discountPercent,
      contractedMrr: subscription.basePricePerMonth - discountAmount,
      updatedAt: new Date(),
    }

    await this.deps.saveSubscription(updatedSubscription)

    // Audit log
    await logDiscountApplied(subscriptionId, subscription.tenantId, appliedBy, discountPercent, reason)

    return updatedSubscription
  }

  /**
   * Request approval for a large discount
   */
  async requestDiscountApproval(
    subscriptionId: string,
    discountPercent: number,
    requestedBy: string,
    justification: string
  ): Promise<ApprovalRequest> {
    const subscription = await this.deps.getSubscription(subscriptionId)
    if (!subscription) {
      throw new Error('Subscription not found')
    }

    const request: ApprovalRequest = {
      id: uuidv4(),
      subscriptionId,
      tenantId: subscription.tenantId,
      type: 'discount',
      requestedBy,
      requestedAt: new Date(),
      status: 'pending',
      details: {
        field: 'discountPercent',
        currentValue: subscription.discountPercent ?? 0,
        requestedValue: discountPercent,
        justification,
      },
    }

    await this.deps.saveApprovalRequest(request)

    // Notify approvers
    await this.deps.sendSlackNotification(
      '#sales-approvals',
      `Discount approval requested: ${discountPercent}% off for ${subscription.tenantId}\nJustification: ${justification}`
    )

    return request
  }

  /**
   * Approve a pending request
   */
  async approveRequest(
    requestId: string,
    approvedBy: string,
    notes?: string
  ): Promise<ApprovalRequest> {
    const request = await this.deps.getApprovalRequest(requestId)
    if (!request) {
      throw new Error('Approval request not found')
    }

    if (request.status !== 'pending') {
      throw new Error('Request is not pending')
    }

    const updatedRequest: ApprovalRequest = {
      ...request,
      status: 'approved',
      reviewedBy: approvedBy,
      reviewedAt: new Date(),
      notes,
    }

    await this.deps.saveApprovalRequest(updatedRequest)

    // Apply the approved change
    if (request.type === 'discount') {
      await this.applyDiscount(
        request.subscriptionId,
        request.details.requestedValue as number,
        approvedBy,
        `Approved: ${notes ?? request.details.justification}`
      )
    }

    return updatedRequest
  }

  /**
   * Reject a pending request
   */
  async rejectRequest(
    requestId: string,
    rejectedBy: string,
    reason: string
  ): Promise<ApprovalRequest> {
    const request = await this.deps.getApprovalRequest(requestId)
    if (!request) {
      throw new Error('Approval request not found')
    }

    const updatedRequest: ApprovalRequest = {
      ...request,
      status: 'rejected',
      reviewedBy: rejectedBy,
      reviewedAt: new Date(),
      notes: reason,
    }

    await this.deps.saveApprovalRequest(updatedRequest)

    return updatedRequest
  }

  // ===========================================================================
  // ADD-ONS
  // ===========================================================================

  /**
   * Add an add-on to subscription
   */
  async addAddOn(
    subscriptionId: string,
    addOnCode: AddOnCode,
    quantity: number = 1,
    addedBy: string
  ): Promise<SubscriptionAddOn> {
    const subscription = await this.deps.getSubscription(subscriptionId)
    if (!subscription) {
      throw new Error('Subscription not found')
    }

    const addOnDef = getAddOnByCode(addOnCode)
    if (!addOnDef) {
      throw new Error('Invalid add-on')
    }

    // Check if add-on is available for this plan
    const plan = getPlanByCode(subscription.planCode)
    if (!plan?.availableAddOns.includes(addOnCode)) {
      throw new Error(`Add-on ${addOnCode} is not available for ${subscription.planCode} plan`)
    }

    // Check if already added
    const existingAddOns = await this.deps.getSubscriptionAddOns(subscriptionId)
    const existing = existingAddOns.find(a => a.addOnCode === addOnCode && !a.deactivatedAt)
    if (existing) {
      throw new Error('Add-on already active on subscription')
    }

    const addOn: SubscriptionAddOn = {
      id: uuidv4(),
      subscriptionId,
      addOnCode,
      quantity,
      activatedAt: new Date(),
    }

    await this.deps.saveSubscriptionAddOn(addOn)

    // Audit log
    await logAddOnAdded(subscriptionId, subscription.tenantId, addedBy, addOnCode, quantity)

    return addOn
  }

  /**
   * Remove an add-on from subscription
   */
  async removeAddOn(
    subscriptionId: string,
    addOnCode: AddOnCode,
    removedBy: string
  ): Promise<void> {
    const subscription = await this.deps.getSubscription(subscriptionId)
    if (!subscription) {
      throw new Error('Subscription not found')
    }

    const addOns = await this.deps.getSubscriptionAddOns(subscriptionId)
    const addOn = addOns.find(a => a.addOnCode === addOnCode && !a.deactivatedAt)

    if (!addOn) {
      throw new Error('Add-on not found on subscription')
    }

    const updatedAddOn: SubscriptionAddOn = {
      ...addOn,
      deactivatedAt: new Date(),
    }

    await this.deps.saveSubscriptionAddOn(updatedAddOn)

    // Audit log
    await logAddOnRemoved(subscriptionId, subscription.tenantId, removedBy, addOnCode)
  }

  // ===========================================================================
  // RENEWALS
  // ===========================================================================

  /**
   * Check for subscriptions needing renewal reminders
   */
  async checkRenewalReminders(): Promise<void> {
    // This would be called by a scheduled job
    // Implementation would:
    // 1. Find subscriptions with renewalDate within N days
    // 2. Send reminders at 90, 60, 30, 14, 7 days
    // 3. Auto-renew if renewalType === 'auto'
    // 4. Create renewal quote if renewalType === 'manual'
  }

  /**
   * Process auto-renewal for a subscription
   */
  async processRenewal(subscriptionId: string): Promise<Subscription> {
    const subscription = await this.deps.getSubscription(subscriptionId)
    if (!subscription) {
      throw new Error('Subscription not found')
    }

    if (subscription.cancelAtPeriodEnd) {
      // Don't renew if marked for cancellation
      const canceledSubscription: Subscription = {
        ...subscription,
        status: 'canceled',
        canceledAt: new Date(),
        updatedAt: new Date(),
      }
      await this.deps.saveSubscription(canceledSubscription)
      return canceledSubscription
    }

    // Extend contract period
    const newEndDate = new Date(subscription.contractEndDate)
    newEndDate.setMonth(newEndDate.getMonth() + subscription.termMonths)

    const renewedSubscription: Subscription = {
      ...subscription,
      contractEndDate: newEndDate,
      renewalDate: newEndDate,
      updatedAt: new Date(),
    }

    await this.deps.saveSubscription(renewedSubscription)

    // Audit log
    await logSubscriptionRenewed(subscriptionId, subscription.tenantId, newEndDate)

    // Emit event
    const eventBus = getEventBus()
    eventBus.emit({
      eventId: uuidv4(),
      type: 'billing.plan_changed',
      customerId: subscription.tenantId,
      timestamp: new Date(),
      data: {
        subscriptionId,
        event: 'renewed',
        newEndDate: newEndDate.toISOString(),
      },
    })

    return renewedSubscription
  }

  // ===========================================================================
  // CANCELLATION
  // ===========================================================================

  /**
   * Cancel a subscription
   */
  async cancelSubscription(
    subscriptionId: string,
    options: {
      immediately?: boolean
      reason?: string
      canceledBy: string
    }
  ): Promise<Subscription> {
    const subscription = await this.deps.getSubscription(subscriptionId)
    if (!subscription) {
      throw new Error('Subscription not found')
    }

    const updatedSubscription: Subscription = {
      ...subscription,
      status: options.immediately ? 'canceled' : subscription.status,
      cancelAtPeriodEnd: !options.immediately,
      canceledAt: options.immediately ? new Date() : undefined,
      updatedAt: new Date(),
    }

    await this.deps.saveSubscription(updatedSubscription)

    // Audit log
    await logSubscriptionCanceled(
      subscriptionId,
      subscription.tenantId,
      options.canceledBy,
      options.reason,
      options.immediately
    )

    return updatedSubscription
  }

  // ===========================================================================
  // RAMP SCHEDULES
  // ===========================================================================

  /**
   * Get current ramp step for subscription
   */
  async getCurrentRampStep(subscriptionId: string): Promise<RampStep | null> {
    const subscription = await this.deps.getSubscription(subscriptionId)
    if (!subscription?.rampScheduleId) return null

    const rampSchedule = await this.deps.getRampSchedule(subscription.rampScheduleId)
    if (!rampSchedule) return null

    const now = new Date()
    return rampSchedule.steps.find(step =>
      step.startDate <= now && step.endDate > now
    ) ?? null
  }

  /**
   * Apply ramp step changes
   */
  async applyRampStep(subscriptionId: string, step: RampStep): Promise<Subscription> {
    const subscription = await this.deps.getSubscription(subscriptionId)
    if (!subscription) {
      throw new Error('Subscription not found')
    }

    const plan = getPlanByCode(step.planCode)
    if (!plan) {
      throw new Error('Invalid plan in ramp step')
    }

    const updatedSubscription: Subscription = {
      ...subscription,
      planCode: step.planCode,
      basePricePerMonth: step.priceOverride ?? (
        subscription.billingInterval === 'annual'
          ? plan.pricing.annual / 12
          : plan.pricing.monthly
      ),
      updatedAt: new Date(),
    }

    // Recalculate contracted MRR
    const discountAmount = updatedSubscription.discountPercent
      ? updatedSubscription.basePricePerMonth * (updatedSubscription.discountPercent / 100)
      : 0
    updatedSubscription.contractedMrr = updatedSubscription.basePricePerMonth - discountAmount

    await this.deps.saveSubscription(updatedSubscription)

    return updatedSubscription
  }
}

// =============================================================================
// SINGLETON
// =============================================================================

let contractServiceInstance: ContractService | null = null

export function getContractService(deps?: ContractDependencies): ContractService {
  if (!contractServiceInstance && deps) {
    contractServiceInstance = new ContractService(deps)
  }
  if (!contractServiceInstance) {
    throw new Error('ContractService not initialized')
  }
  return contractServiceInstance
}
