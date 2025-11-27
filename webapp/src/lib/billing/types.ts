/**
 * Billing Types v2
 * Extended type definitions for consumption-based billing with add-ons
 */

// =============================================================================
// METERS & USAGE
// =============================================================================

export type MeterName =
  | 'seats'
  | 'deals'
  | 'ai_insight_credits'
  | 'agent_actions'
  | 'workspaces'
  | 'data_storage_gb'
  | 'api_calls'
  | 'webhook_deliveries'
  | 'conversation_minutes'
  | 'custom_model_inferences'

export interface UsageRecord {
  id: string
  tenantId: string
  subscriptionId: string
  meter: MeterName
  quantity: number
  occurredAt: Date
  metadata?: Record<string, unknown>
}

export interface UsageAggregate {
  id: string
  tenantId: string
  subscriptionId: string
  meter: MeterName
  period: string // "2025-11" or contract period identifier
  used: number
  lastUpdatedAt: Date
}

// =============================================================================
// LIMITS & ENFORCEMENT
// =============================================================================

export type CapType = 'soft' | 'hard' | 'none'

export interface LimitDefinition {
  meter: MeterName
  included: number | 'unlimited'
  capType: CapType
  overagePrice?: number // price per unit over included
  overageCurrency?: string
}

export interface EnforcementResult {
  allowed: boolean
  meter: MeterName
  included: number | 'unlimited'
  used: number
  remaining: number | 'unlimited'
  overage: number
  overageCost: number
  capType: CapType
  message?: string
}

// =============================================================================
// ADD-ONS
// =============================================================================

export type AddOnCode =
  | 'coaching_ai'
  | 'conversation_intelligence'
  | 'custom_models'
  | 'extra_workspace'
  | 'priority_support'
  | 'sso_saml'
  | 'advanced_analytics'
  | 'data_export'

export interface AddOnDefinition {
  code: AddOnCode
  name: string
  description: string
  category: 'ai' | 'integration' | 'support' | 'security' | 'data'
  pricingModel: 'flat' | 'per_unit' | 'tiered'
  pricePerMonth?: number // for flat pricing
  pricePerUnit?: number // for per-unit pricing
  unitName?: string // e.g., "workspace", "model", "hour"
  tiers?: AddOnTier[] // for tiered pricing
  meters?: MeterName[] // new meters this add-on unlocks
  limits?: LimitDefinition[] // additional limits from this add-on
  requiredPlan?: PlanTier[] // which plans can purchase this add-on
}

export interface AddOnTier {
  upTo: number | 'unlimited'
  pricePerUnit: number
}

export interface SubscriptionAddOn {
  id: string
  subscriptionId: string
  addOnCode: AddOnCode
  quantity: number // for per-unit add-ons
  priceOverride?: number // custom negotiated price
  activatedAt: Date
  deactivatedAt?: Date
}

// =============================================================================
// PLANS
// =============================================================================

export type PlanTier = 'starter' | 'growth' | 'enterprise'
export type BillingInterval = 'monthly' | 'annual'
export type PlanStatus = 'active' | 'trialing' | 'past_due' | 'canceled' | 'suspended'

export interface PlanPricing {
  monthly: number
  annual: number // typically discounted (e.g., 2 months free)
  currency: string
}

export interface PlanDefinition {
  id: string
  code: PlanTier
  name: string
  description: string
  tagline: string
  pricing: PlanPricing
  limits: LimitDefinition[]
  features: string[]
  availableAddOns: AddOnCode[]
  defaultTemplatePacks: string[]
  trialDays: number
  sortOrder: number
}

// =============================================================================
// SUBSCRIPTIONS & CONTRACTS
// =============================================================================

export type RenewalType = 'auto' | 'manual'

export interface Subscription {
  id: string
  tenantId: string
  planCode: PlanTier
  status: PlanStatus

  // Pricing
  basePricePerMonth: number // list price at contract time
  discountPercent?: number // e.g., 20 = 20% off
  contractedMrr: number // after discount, for reporting
  currency: string

  // Contract terms
  billingInterval: BillingInterval
  contractStartDate: Date
  contractEndDate: Date
  termMonths: number
  renewalDate?: Date
  renewalType: RenewalType

  // Ramp (for enterprise)
  rampScheduleId?: string

  // Trial
  trialEnd?: Date

  // Metadata
  createdAt: Date
  updatedAt: Date
  canceledAt?: Date
  cancelAtPeriodEnd: boolean

  // Payment
  paymentGatewayCustomerId?: string
  paymentGatewaySubscriptionId?: string
}

export interface RampSchedule {
  id: string
  subscriptionId: string
  name: string
  steps: RampStep[]
  createdAt: Date
}

export interface RampStep {
  id: string
  rampScheduleId: string
  stepNumber: number
  startDate: Date
  endDate: Date
  planCode: PlanTier
  priceOverride?: number
  committedUsage?: Partial<Record<MeterName, number>>
  notes?: string
}

// =============================================================================
// RENEWAL QUOTES
// =============================================================================

export type RenewalQuoteStatus = 'pending' | 'accepted' | 'declined' | 'expired' | 'modified'

export interface RenewalQuote {
  id: string
  subscriptionId: string
  tenantId: string

  // Current state
  currentPlanCode: PlanTier
  currentMrr: number

  // Proposed renewal terms
  proposedPlanCode: PlanTier
  proposedMrr: number
  proposedDiscountPercent?: number
  termMonths: number
  proposedTermStart: Date
  proposedTermEnd: Date

  // Status
  status: RenewalQuoteStatus
  acceptedAt?: Date
  acceptedBy?: string
  modifiedAt?: Date
  modifiedBy?: string

  // Metadata
  createdAt: Date
  expiresAt: Date
  notes?: string
}

// =============================================================================
// INVOICES
// =============================================================================

export type InvoiceStatus = 'draft' | 'open' | 'paid' | 'void' | 'uncollectible'

export interface Invoice {
  id: string
  subscriptionId: string
  tenantId: string
  number: string

  // Period
  periodStart: Date
  periodEnd: Date
  dueDate: Date

  // Amounts
  currency: string
  subtotal: number
  discountAmount: number
  taxAmount: number
  total: number
  amountDue: number
  amountPaid: number

  // Status
  status: InvoiceStatus
  paidAt?: Date

  // Line items
  lineItems: InvoiceLineItem[]

  // Metadata
  createdAt: Date
  updatedAt: Date

  // Payment gateway
  paymentGatewayInvoiceId?: string
}

export interface InvoiceLineItem {
  id: string
  invoiceId: string
  type: 'subscription' | 'addon' | 'overage' | 'credit' | 'proration' | 'tax'
  description: string
  meter?: MeterName // for overage items
  quantity: number
  unitPrice: number
  amount: number
  periodStart?: Date
  periodEnd?: Date
}

// =============================================================================
// PRICE BOOKS (LOCALIZATION)
// =============================================================================

export type Region = 'US' | 'EU' | 'UK' | 'APAC' | 'LATAM' | 'GLOBAL'

export interface PriceBookEntry {
  id: string
  planCode: PlanTier
  region: Region
  currency: string
  basePriceMonthly: number
  basePriceAnnual: number
  overagePrices: Partial<Record<MeterName, number>>
  effectiveDate: Date
  expiresAt?: Date
}

export interface TaxConfig {
  region: Region
  taxType: 'vat' | 'gst' | 'sales_tax' | 'none'
  defaultRate: number // percentage, e.g., 20 for 20%
  reverseChargeEnabled: boolean // for B2B in EU
}

// =============================================================================
// AUDIT
// =============================================================================

export type SubscriptionEventType =
  | 'subscription.created'
  | 'subscription.activated'
  | 'subscription.plan_changed'
  | 'subscription.discount_applied'
  | 'subscription.discount_removed'
  | 'subscription.addon_added'
  | 'subscription.addon_removed'
  | 'subscription.canceled'
  | 'subscription.reactivated'
  | 'subscription.renewed'
  | 'payment.method_added'
  | 'payment.method_removed'
  | 'payment.succeeded'
  | 'payment.failed'
  | 'invoice.created'
  | 'invoice.paid'
  | 'invoice.voided'
  | 'usage.threshold_reached'
  | 'usage.hard_cap_hit'

export interface SubscriptionEvent {
  id: string
  subscriptionId: string
  tenantId: string
  type: SubscriptionEventType
  actor: 'system' | string // userId if human
  previousValue?: unknown
  newValue?: unknown
  metadata?: Record<string, unknown>
  occurredAt: Date
}

// =============================================================================
// APPROVALS
// =============================================================================

export type ApprovalStatus = 'pending' | 'approved' | 'rejected'
export type ApprovalType = 'discount' | 'custom_pricing' | 'contract_terms' | 'addon'

export interface ApprovalRequest {
  id: string
  subscriptionId: string
  tenantId: string
  type: ApprovalType
  requestedBy: string // userId
  requestedAt: Date
  status: ApprovalStatus
  reviewedBy?: string // userId
  reviewedAt?: Date
  details: {
    field: string
    currentValue: unknown
    requestedValue: unknown
    justification?: string
  }
  notes?: string
}

// =============================================================================
// PLAN DEFINITIONS (CONFIGURED)
// =============================================================================

export const ADD_ON_DEFINITIONS: AddOnDefinition[] = [
  {
    code: 'coaching_ai',
    name: 'Coaching AI',
    description: 'Advanced AI-powered coaching with personalized playbooks and rep development',
    category: 'ai',
    pricingModel: 'flat',
    pricePerMonth: 2000,
    meters: ['ai_insight_credits'],
    limits: [{ meter: 'ai_insight_credits', included: 10000, capType: 'soft', overagePrice: 0.01 }],
    requiredPlan: ['growth', 'enterprise'],
  },
  {
    code: 'conversation_intelligence',
    name: 'Conversation Intelligence',
    description: 'Deep analysis of calls and emails with sentiment, talk tracks, and competitive mentions',
    category: 'ai',
    pricingModel: 'tiered',
    unitName: 'hour',
    tiers: [
      { upTo: 100, pricePerUnit: 5 },
      { upTo: 500, pricePerUnit: 4 },
      { upTo: 'unlimited', pricePerUnit: 3 },
    ],
    meters: ['conversation_minutes'],
    requiredPlan: ['growth', 'enterprise'],
  },
  {
    code: 'custom_models',
    name: 'Custom AI Models',
    description: 'Train and deploy custom ML models for your specific use cases',
    category: 'ai',
    pricingModel: 'per_unit',
    pricePerUnit: 1000,
    unitName: 'model',
    meters: ['custom_model_inferences'],
    limits: [{ meter: 'custom_model_inferences', included: 50000, capType: 'soft', overagePrice: 0.001 }],
    requiredPlan: ['enterprise'],
  },
  {
    code: 'extra_workspace',
    name: 'Additional Workspace',
    description: 'Add another CRM workspace or environment',
    category: 'integration',
    pricingModel: 'per_unit',
    pricePerUnit: 300,
    unitName: 'workspace',
    meters: ['workspaces'],
  },
  {
    code: 'priority_support',
    name: 'Priority Support',
    description: '24/7 priority support with dedicated success manager',
    category: 'support',
    pricingModel: 'flat',
    pricePerMonth: 500,
    requiredPlan: ['growth', 'enterprise'],
  },
  {
    code: 'sso_saml',
    name: 'SSO/SAML',
    description: 'Enterprise single sign-on with SAML 2.0 and SCIM provisioning',
    category: 'security',
    pricingModel: 'flat',
    pricePerMonth: 200,
    requiredPlan: ['growth', 'enterprise'],
  },
  {
    code: 'advanced_analytics',
    name: 'Advanced Analytics',
    description: 'Custom reports, data warehouse export, and BI tool integration',
    category: 'data',
    pricingModel: 'flat',
    pricePerMonth: 400,
    meters: ['data_storage_gb'],
    limits: [{ meter: 'data_storage_gb', included: 100, capType: 'soft', overagePrice: 1 }],
  },
  {
    code: 'data_export',
    name: 'Data Export API',
    description: 'Programmatic access to export all your data',
    category: 'data',
    pricingModel: 'flat',
    pricePerMonth: 150,
    meters: ['api_calls'],
    limits: [{ meter: 'api_calls', included: 100000, capType: 'soft', overagePrice: 0.0001 }],
  },
]

export const PLAN_DEFINITIONS: PlanDefinition[] = [
  {
    id: 'plan_starter',
    code: 'starter',
    name: 'Starter',
    description: 'Essential RevOps for small teams getting started',
    tagline: 'For teams of 10-20 reps',
    pricing: { monthly: 499, annual: 4990, currency: 'USD' },
    limits: [
      { meter: 'seats', included: 20, capType: 'hard', overagePrice: 20 },
      { meter: 'deals', included: 10000, capType: 'soft', overagePrice: 0.01 },
      { meter: 'ai_insight_credits', included: 2000, capType: 'soft', overagePrice: 0.02 },
      { meter: 'workspaces', included: 1, capType: 'hard' },
      { meter: 'api_calls', included: 0, capType: 'hard' }, // No API access
      { meter: 'agent_actions', included: 500, capType: 'soft', overagePrice: 0.05 },
    ],
    features: [
      'Pipeline dashboard',
      'Forecast summary',
      'CRM sync (daily)',
      'Basic AI summaries',
      '1 template pack',
      'Email support',
    ],
    availableAddOns: ['extra_workspace', 'data_export'],
    defaultTemplatePacks: ['starter'],
    trialDays: 14,
    sortOrder: 1,
  },
  {
    id: 'plan_growth',
    code: 'growth',
    name: 'Growth',
    description: 'AI-powered insights for scaling revenue teams',
    tagline: 'For teams of 20-100 reps',
    pricing: { monthly: 1499, annual: 14990, currency: 'USD' },
    limits: [
      { meter: 'seats', included: 75, capType: 'soft', overagePrice: 15 },
      { meter: 'deals', included: 100000, capType: 'soft', overagePrice: 0.008 },
      { meter: 'ai_insight_credits', included: 10000, capType: 'soft', overagePrice: 0.015 },
      { meter: 'workspaces', included: 2, capType: 'soft', overagePrice: 300 },
      { meter: 'api_calls', included: 50000, capType: 'soft', overagePrice: 0.0005 },
      { meter: 'agent_actions', included: 2500, capType: 'soft', overagePrice: 0.03 },
      { meter: 'webhook_deliveries', included: 10000, capType: 'soft', overagePrice: 0.001 },
    ],
    features: [
      'Everything in Starter',
      'AI deal scoring',
      'Win probability predictions',
      'Activity analytics',
      'Real-time CRM sync',
      'API access',
      'Webhooks',
      '2 template packs',
      'Chat support',
    ],
    availableAddOns: [
      'coaching_ai',
      'conversation_intelligence',
      'extra_workspace',
      'priority_support',
      'sso_saml',
      'advanced_analytics',
      'data_export',
    ],
    defaultTemplatePacks: ['starter', 'growth'],
    trialDays: 14,
    sortOrder: 2,
  },
  {
    id: 'plan_enterprise',
    code: 'enterprise',
    name: 'Enterprise',
    description: 'Full platform for large revenue organizations',
    tagline: 'For 100+ rep organizations',
    pricing: { monthly: 4999, annual: 49990, currency: 'USD' },
    limits: [
      { meter: 'seats', included: 'unlimited', capType: 'none' },
      { meter: 'deals', included: 'unlimited', capType: 'none' },
      { meter: 'ai_insight_credits', included: 50000, capType: 'soft', overagePrice: 0.01 },
      { meter: 'workspaces', included: 5, capType: 'soft', overagePrice: 250 },
      { meter: 'api_calls', included: 500000, capType: 'soft', overagePrice: 0.0002 },
      { meter: 'agent_actions', included: 10000, capType: 'soft', overagePrice: 0.02 },
      { meter: 'webhook_deliveries', included: 100000, capType: 'soft', overagePrice: 0.0005 },
      { meter: 'data_storage_gb', included: 50, capType: 'soft', overagePrice: 0.5 },
      { meter: 'conversation_minutes', included: 1000, capType: 'soft', overagePrice: 3 },
      { meter: 'custom_model_inferences', included: 10000, capType: 'soft', overagePrice: 0.002 },
    ],
    features: [
      'Everything in Growth',
      'Custom AI models',
      'Conversation intelligence',
      'Territory planning',
      'Executive dashboards',
      'SSO/SAML included',
      'Dedicated success manager',
      'SLA guarantee (99.9%)',
      'Custom integrations',
      'Unlimited template packs',
    ],
    availableAddOns: [
      'coaching_ai',
      'conversation_intelligence',
      'custom_models',
      'extra_workspace',
      'priority_support',
      'advanced_analytics',
      'data_export',
    ],
    defaultTemplatePacks: ['starter', 'growth', 'enterprise'],
    trialDays: 30,
    sortOrder: 3,
  },
]

// =============================================================================
// TAX CONFIGURATIONS
// =============================================================================

export const TAX_CONFIGS: TaxConfig[] = [
  { region: 'US', taxType: 'sales_tax', defaultRate: 0, reverseChargeEnabled: false },
  { region: 'EU', taxType: 'vat', defaultRate: 20, reverseChargeEnabled: true },
  { region: 'UK', taxType: 'vat', defaultRate: 20, reverseChargeEnabled: false },
  { region: 'APAC', taxType: 'gst', defaultRate: 10, reverseChargeEnabled: false },
  { region: 'LATAM', taxType: 'vat', defaultRate: 16, reverseChargeEnabled: false },
  { region: 'GLOBAL', taxType: 'none', defaultRate: 0, reverseChargeEnabled: false },
]

// =============================================================================
// HELPERS
// =============================================================================

export function getPlanByCode(code: PlanTier): PlanDefinition | undefined {
  return PLAN_DEFINITIONS.find(p => p.code === code)
}

export function getPlanById(id: string): PlanDefinition | undefined {
  return PLAN_DEFINITIONS.find(p => p.id === id)
}

export function getAddOnByCode(code: AddOnCode): AddOnDefinition | undefined {
  return ADD_ON_DEFINITIONS.find(a => a.code === code)
}

export function getTaxConfig(region: Region): TaxConfig | undefined {
  return TAX_CONFIGS.find(t => t.region === region)
}

export function getMeterDisplayName(meter: MeterName): string {
  const names: Record<MeterName, string> = {
    seats: 'Seats',
    deals: 'Deals',
    ai_insight_credits: 'AI Insight Credits',
    agent_actions: 'Agent Actions',
    workspaces: 'Workspaces',
    data_storage_gb: 'Data Storage (GB)',
    api_calls: 'API Calls',
    webhook_deliveries: 'Webhook Deliveries',
    conversation_minutes: 'Conversation Minutes',
    custom_model_inferences: 'Custom Model Inferences',
  }
  return names[meter]
}

export function calculateAddOnPrice(addOn: AddOnDefinition, quantity: number): number {
  switch (addOn.pricingModel) {
    case 'flat':
      return addOn.pricePerMonth ?? 0

    case 'per_unit':
      return (addOn.pricePerUnit ?? 0) * quantity

    case 'tiered':
      if (!addOn.tiers) return 0
      let remaining = quantity
      let total = 0
      let previousUpTo = 0

      for (const tier of addOn.tiers) {
        const tierUpTo = tier.upTo === 'unlimited' ? Infinity : tier.upTo
        const tierQuantity = Math.min(remaining, tierUpTo - previousUpTo)
        if (tierQuantity > 0) {
          total += tierQuantity * tier.pricePerUnit
          remaining -= tierQuantity
        }
        previousUpTo = tierUpTo === Infinity ? previousUpTo : tierUpTo
        if (remaining <= 0) break
      }
      return total

    default:
      return 0
  }
}
