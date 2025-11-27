/**
 * Billing Module
 * Consumption-based billing with add-ons, usage tracking, and enterprise features
 */

// =============================================================================
// CORE TYPES
// =============================================================================

export type {
  // Meters & Usage
  MeterName,
  UsageRecord,
  UsageAggregate,

  // Limits & Enforcement
  CapType,
  LimitDefinition,
  EnforcementResult,

  // Add-ons
  AddOnCode,
  AddOnDefinition,
  AddOnTier,
  SubscriptionAddOn,

  // Plans
  PlanTier,
  BillingInterval,
  PlanStatus,
  PlanPricing,
  PlanDefinition,

  // Subscriptions & Contracts
  RenewalType,
  Subscription,
  RampSchedule,
  RampStep,

  // Renewal Quotes
  RenewalQuoteStatus,
  RenewalQuote,

  // Invoices
  InvoiceStatus,
  Invoice,
  InvoiceLineItem,

  // Price Books
  Region,
  PriceBookEntry,
  TaxConfig,

  // Audit
  SubscriptionEventType,
  SubscriptionEvent,

  // Approvals
  ApprovalStatus,
  ApprovalType,
  ApprovalRequest,
} from './types'

export {
  // Plan & Add-on definitions
  PLAN_DEFINITIONS,
  ADD_ON_DEFINITIONS,
  TAX_CONFIGS,

  // Helpers
  getPlanByCode,
  getPlanById,
  getAddOnByCode,
  getTaxConfig,
  getMeterDisplayName,
  calculateAddOnPrice,
} from './types'

// =============================================================================
// USAGE TRACKING
// =============================================================================

export {
  UsageTracker,
  getUsageTracker,

  // Convenience functions
  trackUsage,
  checkUsageAllowed,
  getUsageStatus,

  // Meter-specific helpers
  trackAIUsage,
  trackAgentAction,
  trackAPICall,
  trackWebhookDelivery,
  trackConversationMinutes,
  trackCustomModelInference,
  setDealCount,
  setSeatCount,
} from './usage'

// =============================================================================
// INVOICING
// =============================================================================

export {
  InvoicingService,
  getInvoicingService,
} from './invoicing'

// =============================================================================
// PAYMENT GATEWAY
// =============================================================================

export type {
  CustomerParams,
  PaymentMethodParams,
  CreateSubscriptionParams,
  CreateInvoiceParams,
  WebhookEvent,
  PaymentGateway,
  StripeConfig,
} from './payment-gateway'

export {
  StripePaymentGateway,
  MockPaymentGateway,
  initializePaymentGateway,
  getPaymentGateway,
} from './payment-gateway'

// =============================================================================
// AUDIT LOGGING
// =============================================================================

export {
  AuditLogger,
  getAuditLogger,

  // Subscription lifecycle events
  logSubscriptionCreated,
  logSubscriptionActivated,
  logPlanChanged,
  logDiscountApplied,
  logDiscountRemoved,
  logAddOnAdded,
  logAddOnRemoved,
  logSubscriptionCanceled,
  logSubscriptionReactivated,
  logSubscriptionRenewed,

  // Payment events
  logPaymentMethodAdded,
  logPaymentMethodRemoved,
  logPaymentSucceeded,
  logPaymentFailed,

  // Invoice events
  logInvoiceCreated,
  logInvoicePaid,
  logInvoiceVoided,

  // Usage events
  logUsageThresholdReached,
  logHardCapHit,
} from './audit'

// =============================================================================
// CONTRACTS
// =============================================================================

export {
  ContractService,
  getContractService,
} from './contracts'

// =============================================================================
// RENEWAL SCHEDULER
// =============================================================================

export type {
  RenewalReminderConfig,
} from './renewal-scheduler'

export {
  RenewalScheduler,
  getRenewalScheduler,
} from './renewal-scheduler'

// =============================================================================
// PRICE BOOKS
// =============================================================================

export type {
  CurrencyConfig,
} from './price-books'

export {
  SUPPORTED_CURRENCIES,
  REGION_DEFAULT_CURRENCY,
  PriceBookService,
  getPriceBookService,
  generateDefaultPriceBooks,
} from './price-books'

// =============================================================================
// TEMPLATE PROVISIONING
// =============================================================================

export type {
  TemplatePack,
  TemplateComponent,
  TenantTemplateStatus,
} from './template-provisioning'

export {
  TEMPLATE_PACKS,
  TemplateProvisioningService,
  getTemplateProvisioningService,
} from './template-provisioning'

// =============================================================================
// LEGACY EXPORTS (for backward compatibility)
// =============================================================================

export * from './service'
export * from './nudges'
