/**
 * Renewal Scheduler
 * Handles renewal reminders, auto-renewals, and renewal quote generation
 */

import { v4 as uuidv4 } from 'uuid'
import type { Subscription, RenewalQuote, PlanTier } from './types'
import { getPlanByCode } from './types'
import { getContractService } from './contracts'
import { getAuditLogger } from './audit'
import { getEventBus } from '../events/bus'

// =============================================================================
// TYPES
// =============================================================================

export interface RenewalReminderConfig {
  daysBeforeReminders: number[] // e.g., [90, 60, 30, 14, 7]
  autoRenewLeadDays: number // days before renewal to process auto-renewals
}

interface RenewalSchedulerDependencies {
  // Subscriptions
  getSubscriptionsNearingRenewal: (withinDays: number) => Promise<Subscription[]>
  getSubscription: (id: string) => Promise<Subscription | null>
  saveSubscription: (subscription: Subscription) => Promise<void>

  // Renewal quotes
  saveRenewalQuote: (quote: RenewalQuote) => Promise<void>
  getRenewalQuote: (subscriptionId: string) => Promise<RenewalQuote | null>

  // Tracking
  getRemindersSent: (subscriptionId: string) => Promise<number[]> // days before
  saveReminderSent: (subscriptionId: string, daysBeforeRenewal: number) => Promise<void>

  // Notifications
  sendEmail: (to: string, subject: string, body: string, template?: string) => Promise<void>
  sendSlackNotification: (channel: string, message: string) => Promise<void>
  getTenantEmail: (tenantId: string) => Promise<string>
  getAccountOwnerEmail: (tenantId: string) => Promise<string | null>
}

// =============================================================================
// RENEWAL SCHEDULER
// =============================================================================

export class RenewalScheduler {
  private deps: RenewalSchedulerDependencies
  private config: RenewalReminderConfig
  private schedulerInterval: NodeJS.Timeout | null = null

  constructor(
    deps: RenewalSchedulerDependencies,
    config: RenewalReminderConfig = {
      daysBeforeReminders: [90, 60, 30, 14, 7],
      autoRenewLeadDays: 3,
    }
  ) {
    this.deps = deps
    this.config = config
  }

  /**
   * Start the renewal scheduler
   */
  start(intervalMs = 86400000): void { // Default: daily
    if (this.schedulerInterval) return

    // Run immediately on start
    this.runScheduledTasks().catch(console.error)

    // Then run on interval
    this.schedulerInterval = setInterval(() => {
      this.runScheduledTasks().catch(console.error)
    }, intervalMs)
  }

  /**
   * Stop the renewal scheduler
   */
  stop(): void {
    if (this.schedulerInterval) {
      clearInterval(this.schedulerInterval)
      this.schedulerInterval = null
    }
  }

  /**
   * Run all scheduled renewal tasks
   */
  async runScheduledTasks(): Promise<void> {
    await this.checkRenewalReminders()
    await this.processAutoRenewals()
    await this.generateRenewalQuotes()
  }

  /**
   * Check and send renewal reminders
   */
  async checkRenewalReminders(): Promise<void> {
    const maxDays = Math.max(...this.config.daysBeforeReminders)
    const subscriptions = await this.deps.getSubscriptionsNearingRenewal(maxDays)

    for (const subscription of subscriptions) {
      await this.processRenewalReminders(subscription)
    }
  }

  /**
   * Process reminders for a single subscription
   */
  private async processRenewalReminders(subscription: Subscription): Promise<void> {
    if (subscription.status !== 'active' && subscription.status !== 'trialing') return
    if (subscription.cancelAtPeriodEnd) return

    const now = new Date()
    const renewalDate = new Date(subscription.renewalDate)
    const daysUntilRenewal = Math.ceil(
      (renewalDate.getTime() - now.getTime()) / (24 * 60 * 60 * 1000)
    )

    // Check which reminders to send
    const sentReminders = await this.deps.getRemindersSent(subscription.id)

    for (const reminderDays of this.config.daysBeforeReminders) {
      if (daysUntilRenewal <= reminderDays && !sentReminders.includes(reminderDays)) {
        await this.sendRenewalReminder(subscription, reminderDays, daysUntilRenewal)
        await this.deps.saveReminderSent(subscription.id, reminderDays)
      }
    }
  }

  /**
   * Send a renewal reminder
   */
  private async sendRenewalReminder(
    subscription: Subscription,
    reminderDays: number,
    actualDaysRemaining: number
  ): Promise<void> {
    const plan = getPlanByCode(subscription.planCode)
    const tenantEmail = await this.deps.getTenantEmail(subscription.tenantId)
    const accountOwner = await this.deps.getAccountOwnerEmail(subscription.tenantId)

    const renewalDate = new Date(subscription.renewalDate).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'long',
      day: 'numeric',
    })

    const subject = subscription.renewalType === 'auto'
      ? `Your ${plan?.name ?? subscription.planCode} subscription will auto-renew in ${actualDaysRemaining} days`
      : `Action Required: Renew your ${plan?.name ?? subscription.planCode} subscription`

    const body = subscription.renewalType === 'auto'
      ? this.buildAutoRenewalReminderEmail(subscription, plan?.name ?? subscription.planCode, renewalDate, actualDaysRemaining)
      : this.buildManualRenewalReminderEmail(subscription, plan?.name ?? subscription.planCode, renewalDate, actualDaysRemaining)

    await this.deps.sendEmail(tenantEmail, subject, body, 'renewal-reminder')

    // Also notify account owner internally
    if (accountOwner) {
      await this.deps.sendSlackNotification(
        '#renewals',
        `Renewal reminder sent to ${subscription.tenantId} (${actualDaysRemaining} days remaining, ${subscription.renewalType} renewal)`
      )
    }

    // Emit event
    const eventBus = getEventBus()
    eventBus.emit({
      eventId: uuidv4(),
      type: 'health.alert_triggered',
      customerId: subscription.tenantId,
      timestamp: new Date(),
      data: {
        alertType: 'renewal_reminder',
        subscriptionId: subscription.id,
        daysUntilRenewal: actualDaysRemaining,
        renewalType: subscription.renewalType,
      },
    })
  }

  /**
   * Build auto-renewal reminder email
   */
  private buildAutoRenewalReminderEmail(
    subscription: Subscription,
    planName: string,
    renewalDate: string,
    daysRemaining: number
  ): string {
    return `
Your ${planName} subscription will automatically renew on ${renewalDate}.

Renewal Details:
- Plan: ${planName}
- Term: ${subscription.termMonths} month(s)
- Amount: ${subscription.currency} ${subscription.contractedMrr.toFixed(2)}/month

No action is needed. Your subscription will renew automatically using your saved payment method.

If you wish to make changes to your subscription or cancel before renewal, please visit your billing settings or contact our support team.

${daysRemaining <= 7 ? 'This is your final reminder before automatic renewal.' : ''}
`.trim()
  }

  /**
   * Build manual renewal reminder email
   */
  private buildManualRenewalReminderEmail(
    subscription: Subscription,
    planName: string,
    renewalDate: string,
    daysRemaining: number
  ): string {
    return `
Action required: Your ${planName} subscription expires on ${renewalDate}.

Current Subscription:
- Plan: ${planName}
- Monthly Rate: ${subscription.currency} ${subscription.contractedMrr.toFixed(2)}

To continue your service without interruption, please review and accept your renewal quote in your account settings.

${daysRemaining <= 7 ? '⚠️ Your subscription expires in less than a week. Please take action to avoid service interruption.' : ''}
${daysRemaining <= 3 ? '🚨 URGENT: Your subscription expires in ${daysRemaining} days!' : ''}
`.trim()
  }

  /**
   * Process auto-renewals
   */
  async processAutoRenewals(): Promise<void> {
    const subscriptions = await this.deps.getSubscriptionsNearingRenewal(
      this.config.autoRenewLeadDays
    )

    const contractService = getContractService()

    for (const subscription of subscriptions) {
      if (subscription.renewalType !== 'auto') continue
      if (subscription.cancelAtPeriodEnd) continue
      if (subscription.status !== 'active') continue

      // Check if already past renewal date
      const now = new Date()
      const renewalDate = new Date(subscription.renewalDate)

      if (renewalDate <= now) {
        try {
          await contractService.processRenewal(subscription.id)
          console.log(`Auto-renewed subscription ${subscription.id}`)
        } catch (error) {
          console.error(`Failed to auto-renew ${subscription.id}:`, error)

          // Emit failure event
          const eventBus = getEventBus()
          eventBus.emit({
            eventId: uuidv4(),
            type: 'health.alert_triggered',
            customerId: subscription.tenantId,
            timestamp: new Date(),
            data: {
              alertType: 'renewal_failure',
              subscriptionId: subscription.id,
              error: error instanceof Error ? error.message : 'Unknown error',
            },
          })
        }
      }
    }
  }

  /**
   * Generate renewal quotes for manual renewals
   */
  async generateRenewalQuotes(): Promise<void> {
    const subscriptions = await this.deps.getSubscriptionsNearingRenewal(60) // 60 days before

    for (const subscription of subscriptions) {
      if (subscription.renewalType !== 'manual') continue

      // Check if quote already exists
      const existingQuote = await this.deps.getRenewalQuote(subscription.id)
      if (existingQuote && existingQuote.status !== 'expired') continue

      await this.createRenewalQuote(subscription)
    }
  }

  /**
   * Create a renewal quote
   */
  async createRenewalQuote(subscription: Subscription): Promise<RenewalQuote> {
    const plan = getPlanByCode(subscription.planCode)
    if (!plan) {
      throw new Error('Invalid plan')
    }

    const now = new Date()
    const expiresAt = new Date(subscription.renewalDate)
    expiresAt.setDate(expiresAt.getDate() + 14) // Quote valid 14 days after renewal date

    const newTermEnd = new Date(subscription.renewalDate)
    newTermEnd.setMonth(newTermEnd.getMonth() + subscription.termMonths)

    const quote: RenewalQuote = {
      id: uuidv4(),
      subscriptionId: subscription.id,
      tenantId: subscription.tenantId,
      currentPlanCode: subscription.planCode,
      proposedPlanCode: subscription.planCode, // Same plan by default
      currentMrr: subscription.contractedMrr,
      proposedMrr: subscription.contractedMrr,
      termMonths: subscription.termMonths,
      proposedTermStart: subscription.renewalDate,
      proposedTermEnd: newTermEnd,
      status: 'pending',
      createdAt: now,
      expiresAt,
    }

    await this.deps.saveRenewalQuote(quote)

    // Emit event
    const eventBus = getEventBus()
    eventBus.emit({
      eventId: uuidv4(),
      type: 'billing.plan_changed',
      customerId: subscription.tenantId,
      timestamp: now,
      data: {
        event: 'renewal_quote_created',
        quoteId: quote.id,
        subscriptionId: subscription.id,
      },
    })

    return quote
  }

  /**
   * Accept a renewal quote
   */
  async acceptRenewalQuote(
    quoteId: string,
    acceptedBy: string
  ): Promise<Subscription> {
    const quote = await this.deps.getRenewalQuote(quoteId)
    if (!quote) {
      throw new Error('Quote not found')
    }

    if (quote.status !== 'pending') {
      throw new Error('Quote is not pending')
    }

    const subscription = await this.deps.getSubscription(quote.subscriptionId)
    if (!subscription) {
      throw new Error('Subscription not found')
    }

    // Update quote status
    const updatedQuote: RenewalQuote = {
      ...quote,
      status: 'accepted',
      acceptedAt: new Date(),
      acceptedBy,
    }
    await this.deps.saveRenewalQuote(updatedQuote)

    // Process renewal with potentially new terms
    const contractService = getContractService()

    // If plan changed, apply that first
    if (quote.proposedPlanCode !== subscription.planCode) {
      await contractService.changePlan(subscription.id, quote.proposedPlanCode, {
        effectiveDate: 'end_of_period',
        reason: 'Renewal with plan change',
        changedBy: acceptedBy,
      })
    }

    // Process the renewal
    return contractService.processRenewal(subscription.id)
  }

  /**
   * Decline a renewal quote
   */
  async declineRenewalQuote(quoteId: string, reason?: string): Promise<void> {
    const quote = await this.deps.getRenewalQuote(quoteId)
    if (!quote) {
      throw new Error('Quote not found')
    }

    const updatedQuote: RenewalQuote = {
      ...quote,
      status: 'declined',
    }
    await this.deps.saveRenewalQuote(updatedQuote)

    // Notify internal team
    await this.deps.sendSlackNotification(
      '#renewals',
      `Renewal quote declined for ${quote.tenantId}. Reason: ${reason ?? 'Not specified'}`
    )
  }

  /**
   * Modify a renewal quote (e.g., offer different pricing)
   */
  async modifyRenewalQuote(
    quoteId: string,
    modifications: {
      proposedPlanCode?: PlanTier
      proposedMrr?: number
      proposedDiscountPercent?: number
      termMonths?: number
    },
    modifiedBy: string
  ): Promise<RenewalQuote> {
    const quote = await this.deps.getRenewalQuote(quoteId)
    if (!quote) {
      throw new Error('Quote not found')
    }

    const plan = modifications.proposedPlanCode
      ? getPlanByCode(modifications.proposedPlanCode)
      : getPlanByCode(quote.proposedPlanCode)

    if (!plan) {
      throw new Error('Invalid plan')
    }

    const subscription = await this.deps.getSubscription(quote.subscriptionId)
    if (!subscription) {
      throw new Error('Subscription not found')
    }

    // Calculate new MRR if not explicitly set
    let proposedMrr = modifications.proposedMrr ?? quote.proposedMrr
    if (modifications.proposedPlanCode || modifications.proposedDiscountPercent !== undefined) {
      const basePrice = subscription.billingInterval === 'annual'
        ? plan.pricing.annual / 12
        : plan.pricing.monthly
      const discount = modifications.proposedDiscountPercent ?? subscription.discountPercent ?? 0
      proposedMrr = basePrice * (1 - discount / 100)
    }

    const updatedQuote: RenewalQuote = {
      ...quote,
      proposedPlanCode: modifications.proposedPlanCode ?? quote.proposedPlanCode,
      proposedMrr,
      proposedDiscountPercent: modifications.proposedDiscountPercent ?? quote.proposedDiscountPercent,
      termMonths: modifications.termMonths ?? quote.termMonths,
      modifiedBy,
      modifiedAt: new Date(),
    }

    // Recalculate term end if months changed
    if (modifications.termMonths) {
      const newTermEnd = new Date(quote.proposedTermStart)
      newTermEnd.setMonth(newTermEnd.getMonth() + modifications.termMonths)
      updatedQuote.proposedTermEnd = newTermEnd
    }

    await this.deps.saveRenewalQuote(updatedQuote)

    return updatedQuote
  }
}

// =============================================================================
// SINGLETON
// =============================================================================

let renewalSchedulerInstance: RenewalScheduler | null = null

export function getRenewalScheduler(
  deps?: RenewalSchedulerDependencies,
  config?: RenewalReminderConfig
): RenewalScheduler {
  if (!renewalSchedulerInstance && deps) {
    renewalSchedulerInstance = new RenewalScheduler(deps, config)
  }
  if (!renewalSchedulerInstance) {
    throw new Error('RenewalScheduler not initialized')
  }
  return renewalSchedulerInstance
}
