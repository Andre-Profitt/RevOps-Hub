/**
 * Upgrade Nudges
 * Smart notifications to encourage plan upgrades
 */

import type { PlanTier, Subscription, SubscriptionUsage, PlanLimits } from './types'
import { getPlanByTier, PLAN_DEFINITIONS } from './types'
import { getEventBus } from '../events/bus'

// =============================================================================
// NUDGE TYPES
// =============================================================================

export type NudgeType =
  | 'usage_limit_approaching'
  | 'usage_limit_reached'
  | 'feature_locked'
  | 'value_proposition'
  | 'trial_ending'
  | 'seasonal_offer'

export type NudgeChannel = 'in_app' | 'email' | 'slack'
export type NudgePriority = 'low' | 'medium' | 'high' | 'urgent'

export interface UpgradeNudge {
  id: string
  customerId: string
  type: NudgeType
  priority: NudgePriority
  title: string
  message: string
  ctaText: string
  ctaUrl: string
  targetTier: PlanTier
  channels: NudgeChannel[]
  expiresAt: Date | null
  metadata?: Record<string, unknown>
  createdAt: Date
  dismissedAt: Date | null
  convertedAt: Date | null
}

export interface NudgeTemplate {
  type: NudgeType
  priority: NudgePriority
  titleTemplate: string
  messageTemplate: string
  ctaText: string
  channels: NudgeChannel[]
  expiresInHours: number | null
}

// =============================================================================
// NUDGE TEMPLATES
// =============================================================================

const NUDGE_TEMPLATES: Record<NudgeType, NudgeTemplate> = {
  usage_limit_approaching: {
    type: 'usage_limit_approaching',
    priority: 'medium',
    titleTemplate: "You're approaching your {{limitType}} limit",
    messageTemplate: "You've used {{currentUsage}} of your {{limit}} {{limitType}}. Upgrade to {{targetTier}} for {{newLimit}}.",
    ctaText: 'View Plans',
    channels: ['in_app'],
    expiresInHours: 72,
  },
  usage_limit_reached: {
    type: 'usage_limit_reached',
    priority: 'high',
    titleTemplate: "You've reached your {{limitType}} limit",
    messageTemplate: "You've hit the {{limit}} {{limitType}} limit on your {{currentTier}} plan. Upgrade now to continue using this feature.",
    ctaText: 'Upgrade Now',
    channels: ['in_app', 'email'],
    expiresInHours: null,
  },
  feature_locked: {
    type: 'feature_locked',
    priority: 'medium',
    titleTemplate: 'Unlock {{featureName}}',
    messageTemplate: '{{featureName}} is available on {{targetTier}} and above. Upgrade to access this feature.',
    ctaText: 'See What You Get',
    channels: ['in_app'],
    expiresInHours: 168, // 1 week
  },
  value_proposition: {
    type: 'value_proposition',
    priority: 'low',
    titleTemplate: 'Get more from RevOps',
    messageTemplate: "Customers on {{targetTier}} see an average {{improvement}}% improvement in {{metric}}. Ready to level up?",
    ctaText: 'Learn More',
    channels: ['in_app', 'email'],
    expiresInHours: 336, // 2 weeks
  },
  trial_ending: {
    type: 'trial_ending',
    priority: 'urgent',
    titleTemplate: 'Your trial ends in {{daysRemaining}} days',
    messageTemplate: "Don't lose access to {{featuresCount}} features. Subscribe to {{targetTier}} to keep your data and insights.",
    ctaText: 'Subscribe Now',
    channels: ['in_app', 'email'],
    expiresInHours: null,
  },
  seasonal_offer: {
    type: 'seasonal_offer',
    priority: 'low',
    titleTemplate: '{{offerTitle}}',
    messageTemplate: '{{offerMessage}}',
    ctaText: 'Claim Offer',
    channels: ['in_app', 'email'],
    expiresInHours: null,
  },
}

// =============================================================================
// NUDGE ENGINE
// =============================================================================

interface NudgeDependencies {
  getSubscription: (customerId: string) => Promise<Subscription | null>
  getUsage: (customerId: string) => Promise<SubscriptionUsage | null>
  saveNudge: (nudge: UpgradeNudge) => Promise<void>
  getActiveNudges: (customerId: string) => Promise<UpgradeNudge[]>
  sendEmail: (to: string, subject: string, body: string) => Promise<void>
  sendSlack: (webhookUrl: string, message: string) => Promise<void>
}

export class NudgeEngine {
  private deps: NudgeDependencies

  constructor(deps: NudgeDependencies) {
    this.deps = deps
  }

  /**
   * Evaluate and create nudges for a customer
   */
  async evaluateNudges(customerId: string): Promise<UpgradeNudge[]> {
    const subscription = await this.deps.getSubscription(customerId)
    if (!subscription) return []

    const usage = await this.deps.getUsage(customerId)
    const currentPlan = getPlanByTier(subscription.tier)
    if (!currentPlan) return []

    const nudges: UpgradeNudge[] = []

    // Check usage limits
    if (usage) {
      const usageNudges = await this.evaluateUsageLimits(customerId, subscription, usage, currentPlan.limits)
      nudges.push(...usageNudges)
    }

    // Check trial ending
    if (subscription.status === 'trialing' && subscription.trialEnd) {
      const trialNudge = this.evaluateTrialEnding(customerId, subscription)
      if (trialNudge) nudges.push(trialNudge)
    }

    // Save nudges
    for (const nudge of nudges) {
      await this.deps.saveNudge(nudge)
    }

    return nudges
  }

  /**
   * Evaluate usage limits and create nudges
   */
  private async evaluateUsageLimits(
    customerId: string,
    subscription: Subscription,
    usage: SubscriptionUsage,
    limits: PlanLimits
  ): Promise<UpgradeNudge[]> {
    const nudges: UpgradeNudge[] = []
    const nextTier = this.getNextTier(subscription.tier)
    if (!nextTier) return nudges

    const nextPlan = getPlanByTier(nextTier)
    if (!nextPlan) return nudges

    // Check each numeric limit
    const limitChecks: Array<{
      limitType: string
      current: number
      limit: number | 'unlimited'
      nextLimit: number | 'unlimited'
    }> = [
      { limitType: 'users', current: usage.users, limit: limits.maxUsers, nextLimit: nextPlan.limits.maxUsers },
      { limitType: 'deals', current: usage.deals, limit: limits.maxDeals, nextLimit: nextPlan.limits.maxDeals },
      { limitType: 'dashboards', current: usage.dashboards, limit: limits.maxDashboards, nextLimit: nextPlan.limits.maxDashboards },
    ]

    for (const check of limitChecks) {
      if (check.limit === 'unlimited') continue

      const percentage = check.current / check.limit
      const nextLimitDisplay = check.nextLimit === 'unlimited' ? 'unlimited' : check.nextLimit.toLocaleString()

      if (percentage >= 1) {
        // Limit reached
        nudges.push(this.createNudge(customerId, 'usage_limit_reached', nextTier, {
          limitType: check.limitType,
          currentUsage: check.current.toLocaleString(),
          limit: check.limit.toLocaleString(),
          currentTier: subscription.tier,
          newLimit: nextLimitDisplay,
        }))
      } else if (percentage >= 0.8) {
        // Approaching limit
        nudges.push(this.createNudge(customerId, 'usage_limit_approaching', nextTier, {
          limitType: check.limitType,
          currentUsage: check.current.toLocaleString(),
          limit: check.limit.toLocaleString(),
          targetTier: nextTier,
          newLimit: nextLimitDisplay,
        }))
      }
    }

    return nudges
  }

  /**
   * Evaluate trial ending
   */
  private evaluateTrialEnding(customerId: string, subscription: Subscription): UpgradeNudge | null {
    if (!subscription.trialEnd) return null

    const daysRemaining = Math.ceil(
      (subscription.trialEnd.getTime() - Date.now()) / (24 * 60 * 60 * 1000)
    )

    if (daysRemaining <= 7) {
      const plan = getPlanByTier(subscription.tier)
      return this.createNudge(customerId, 'trial_ending', subscription.tier, {
        daysRemaining: daysRemaining.toString(),
        featuresCount: plan?.features.length.toString() ?? '0',
        targetTier: subscription.tier,
      })
    }

    return null
  }

  /**
   * Create a nudge from template
   */
  private createNudge(
    customerId: string,
    type: NudgeType,
    targetTier: PlanTier,
    variables: Record<string, string>
  ): UpgradeNudge {
    const template = NUDGE_TEMPLATES[type]
    const now = new Date()

    const title = this.interpolate(template.titleTemplate, variables)
    const message = this.interpolate(template.messageTemplate, variables)

    return {
      id: `nudge_${Date.now()}_${Math.random().toString(36).slice(2)}`,
      customerId,
      type,
      priority: template.priority,
      title,
      message,
      ctaText: template.ctaText,
      ctaUrl: `/upgrade?tier=${targetTier}&source=nudge_${type}`,
      targetTier,
      channels: template.channels,
      expiresAt: template.expiresInHours
        ? new Date(now.getTime() + template.expiresInHours * 60 * 60 * 1000)
        : null,
      metadata: variables,
      createdAt: now,
      dismissedAt: null,
      convertedAt: null,
    }
  }

  /**
   * Interpolate template variables
   */
  private interpolate(template: string, variables: Record<string, string>): string {
    return template.replace(/\{\{(\w+)\}\}/g, (match, key) => {
      return variables[key] ?? match
    })
  }

  /**
   * Get next tier for upgrade
   */
  private getNextTier(currentTier: PlanTier): PlanTier | null {
    const tierOrder: PlanTier[] = ['starter', 'growth', 'enterprise']
    const currentIndex = tierOrder.indexOf(currentTier)
    return currentIndex < tierOrder.length - 1 ? tierOrder[currentIndex + 1] : null
  }

  /**
   * Create a feature locked nudge
   */
  async createFeatureNudge(
    customerId: string,
    featureName: string,
    requiredTier: PlanTier
  ): Promise<UpgradeNudge> {
    const nudge = this.createNudge(customerId, 'feature_locked', requiredTier, {
      featureName,
      targetTier: requiredTier,
    })

    await this.deps.saveNudge(nudge)

    // Emit event
    const eventBus = getEventBus()
    eventBus.emit({
      eventId: `evt_${Date.now()}`,
      type: 'billing.usage_threshold',
      customerId,
      timestamp: new Date(),
      data: {
        nudgeType: 'feature_locked',
        featureName,
        requiredTier,
      },
    })

    return nudge
  }

  /**
   * Deliver nudge through configured channels
   */
  async deliverNudge(nudge: UpgradeNudge, customerEmail?: string, slackWebhook?: string): Promise<void> {
    for (const channel of nudge.channels) {
      switch (channel) {
        case 'email':
          if (customerEmail) {
            await this.deps.sendEmail(
              customerEmail,
              nudge.title,
              `${nudge.message}\n\n${nudge.ctaText}: ${nudge.ctaUrl}`
            )
          }
          break
        case 'slack':
          if (slackWebhook) {
            await this.deps.sendSlack(slackWebhook, `*${nudge.title}*\n${nudge.message}`)
          }
          break
        case 'in_app':
          // In-app nudges are stored and shown via getActiveNudges
          break
      }
    }
  }

  /**
   * Get active nudges for customer
   */
  async getActiveNudges(customerId: string): Promise<UpgradeNudge[]> {
    const nudges = await this.deps.getActiveNudges(customerId)

    // Filter expired
    const now = new Date()
    return nudges.filter(nudge => {
      if (nudge.dismissedAt || nudge.convertedAt) return false
      if (nudge.expiresAt && nudge.expiresAt < now) return false
      return true
    })
  }
}

// =============================================================================
// SINGLETON
// =============================================================================

let nudgeEngineInstance: NudgeEngine | null = null

export function getNudgeEngine(deps?: NudgeDependencies): NudgeEngine {
  if (!nudgeEngineInstance && deps) {
    nudgeEngineInstance = new NudgeEngine(deps)
  }
  if (!nudgeEngineInstance) {
    throw new Error('NudgeEngine not initialized')
  }
  return nudgeEngineInstance
}
