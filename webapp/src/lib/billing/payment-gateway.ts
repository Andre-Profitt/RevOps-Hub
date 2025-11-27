/**
 * Payment Gateway Abstraction
 * Interface and implementations for payment processing
 */

import type { Invoice, Subscription } from './types'

// =============================================================================
// GATEWAY INTERFACE
// =============================================================================

export interface CustomerParams {
  email: string
  name: string
  metadata?: Record<string, string>
}

export interface PaymentMethodParams {
  type: 'card' | 'bank_account'
  token: string // Tokenized payment method from Stripe.js / etc
}

export interface CreateSubscriptionParams {
  customerId: string
  priceId: string
  quantity?: number
  trialDays?: number
  metadata?: Record<string, string>
}

export interface CreateInvoiceParams {
  customerId: string
  subscriptionId?: string
  items: {
    description: string
    amount: number
    quantity: number
  }[]
  dueDate?: Date
  metadata?: Record<string, string>
}

export interface WebhookEvent {
  id: string
  type: string
  data: unknown
  created: Date
}

export interface PaymentGateway {
  // Customer management
  createCustomer(params: CustomerParams): Promise<string> // returns gatewayCustomerId
  updateCustomer(customerId: string, params: Partial<CustomerParams>): Promise<void>
  deleteCustomer(customerId: string): Promise<void>

  // Payment methods
  attachPaymentMethod(customerId: string, params: PaymentMethodParams): Promise<string> // returns paymentMethodId
  detachPaymentMethod(paymentMethodId: string): Promise<void>
  setDefaultPaymentMethod(customerId: string, paymentMethodId: string): Promise<void>
  listPaymentMethods(customerId: string): Promise<{ id: string; type: string; last4?: string; expMonth?: number; expYear?: number }[]>

  // Subscriptions (for automatic billing)
  createSubscription(params: CreateSubscriptionParams): Promise<string> // returns gatewaySubscriptionId
  updateSubscription(subscriptionId: string, updates: Partial<CreateSubscriptionParams>): Promise<void>
  cancelSubscription(subscriptionId: string, immediately?: boolean): Promise<void>

  // One-time charges / invoices
  createInvoice(params: CreateInvoiceParams): Promise<string> // returns gatewayInvoiceId
  payInvoice(invoiceId: string, paymentMethodId?: string): Promise<boolean>
  voidInvoice(invoiceId: string): Promise<void>

  // Webhooks
  constructWebhookEvent(payload: string | Buffer, signature: string): WebhookEvent
  handleWebhook(event: WebhookEvent): Promise<void>

  // Utilities
  getPublishableKey(): string
}

// =============================================================================
// STRIPE IMPLEMENTATION
// =============================================================================

export interface StripeConfig {
  secretKey: string
  publishableKey: string
  webhookSecret: string
}

export class StripePaymentGateway implements PaymentGateway {
  private config: StripeConfig
  private stripe: any // Would be Stripe instance

  constructor(config: StripeConfig) {
    this.config = config
    // In production: this.stripe = new Stripe(config.secretKey)
  }

  async createCustomer(params: CustomerParams): Promise<string> {
    // const customer = await this.stripe.customers.create({
    //   email: params.email,
    //   name: params.name,
    //   metadata: params.metadata,
    // })
    // return customer.id
    return `cus_${Date.now()}_mock`
  }

  async updateCustomer(customerId: string, params: Partial<CustomerParams>): Promise<void> {
    // await this.stripe.customers.update(customerId, params)
  }

  async deleteCustomer(customerId: string): Promise<void> {
    // await this.stripe.customers.del(customerId)
  }

  async attachPaymentMethod(customerId: string, params: PaymentMethodParams): Promise<string> {
    // const pm = await this.stripe.paymentMethods.attach(params.token, { customer: customerId })
    // return pm.id
    return `pm_${Date.now()}_mock`
  }

  async detachPaymentMethod(paymentMethodId: string): Promise<void> {
    // await this.stripe.paymentMethods.detach(paymentMethodId)
  }

  async setDefaultPaymentMethod(customerId: string, paymentMethodId: string): Promise<void> {
    // await this.stripe.customers.update(customerId, {
    //   invoice_settings: { default_payment_method: paymentMethodId }
    // })
  }

  async listPaymentMethods(customerId: string): Promise<{ id: string; type: string; last4?: string; expMonth?: number; expYear?: number }[]> {
    // const methods = await this.stripe.paymentMethods.list({ customer: customerId, type: 'card' })
    // return methods.data.map(pm => ({ ... }))
    return []
  }

  async createSubscription(params: CreateSubscriptionParams): Promise<string> {
    // const sub = await this.stripe.subscriptions.create({
    //   customer: params.customerId,
    //   items: [{ price: params.priceId, quantity: params.quantity }],
    //   trial_period_days: params.trialDays,
    //   metadata: params.metadata,
    // })
    // return sub.id
    return `sub_${Date.now()}_mock`
  }

  async updateSubscription(subscriptionId: string, updates: Partial<CreateSubscriptionParams>): Promise<void> {
    // await this.stripe.subscriptions.update(subscriptionId, updates)
  }

  async cancelSubscription(subscriptionId: string, immediately = false): Promise<void> {
    // if (immediately) {
    //   await this.stripe.subscriptions.del(subscriptionId)
    // } else {
    //   await this.stripe.subscriptions.update(subscriptionId, { cancel_at_period_end: true })
    // }
  }

  async createInvoice(params: CreateInvoiceParams): Promise<string> {
    // Create invoice items
    // for (const item of params.items) {
    //   await this.stripe.invoiceItems.create({
    //     customer: params.customerId,
    //     description: item.description,
    //     amount: Math.round(item.amount * 100),
    //     quantity: item.quantity,
    //   })
    // }
    // const invoice = await this.stripe.invoices.create({
    //   customer: params.customerId,
    //   due_date: params.dueDate ? Math.floor(params.dueDate.getTime() / 1000) : undefined,
    //   metadata: params.metadata,
    // })
    // return invoice.id
    return `in_${Date.now()}_mock`
  }

  async payInvoice(invoiceId: string, paymentMethodId?: string): Promise<boolean> {
    // const result = await this.stripe.invoices.pay(invoiceId, {
    //   payment_method: paymentMethodId,
    // })
    // return result.paid
    return true
  }

  async voidInvoice(invoiceId: string): Promise<void> {
    // await this.stripe.invoices.voidInvoice(invoiceId)
  }

  constructWebhookEvent(payload: string | Buffer, signature: string): WebhookEvent {
    // const event = this.stripe.webhooks.constructEvent(payload, signature, this.config.webhookSecret)
    // return { id: event.id, type: event.type, data: event.data.object, created: new Date(event.created * 1000) }
    return {
      id: 'evt_mock',
      type: 'invoice.paid',
      data: {},
      created: new Date(),
    }
  }

  async handleWebhook(event: WebhookEvent): Promise<void> {
    // Handle different event types
    switch (event.type) {
      case 'invoice.paid':
        // Update internal invoice status
        break
      case 'invoice.payment_failed':
        // Mark subscription as past_due
        break
      case 'customer.subscription.deleted':
        // Mark subscription as canceled
        break
      case 'customer.subscription.updated':
        // Sync subscription changes
        break
    }
  }

  getPublishableKey(): string {
    return this.config.publishableKey
  }
}

// =============================================================================
// MOCK IMPLEMENTATION (FOR DEVELOPMENT)
// =============================================================================

export class MockPaymentGateway implements PaymentGateway {
  private customers: Map<string, CustomerParams> = new Map()
  private paymentMethods: Map<string, { customerId: string; type: string; last4: string }> = new Map()
  private subscriptions: Map<string, CreateSubscriptionParams> = new Map()
  private invoices: Map<string, { paid: boolean; voided: boolean }> = new Map()

  async createCustomer(params: CustomerParams): Promise<string> {
    const id = `cus_mock_${Date.now()}`
    this.customers.set(id, params)
    return id
  }

  async updateCustomer(customerId: string, params: Partial<CustomerParams>): Promise<void> {
    const existing = this.customers.get(customerId)
    if (existing) {
      this.customers.set(customerId, { ...existing, ...params })
    }
  }

  async deleteCustomer(customerId: string): Promise<void> {
    this.customers.delete(customerId)
  }

  async attachPaymentMethod(customerId: string, params: PaymentMethodParams): Promise<string> {
    const id = `pm_mock_${Date.now()}`
    this.paymentMethods.set(id, {
      customerId,
      type: params.type,
      last4: '4242',
    })
    return id
  }

  async detachPaymentMethod(paymentMethodId: string): Promise<void> {
    this.paymentMethods.delete(paymentMethodId)
  }

  async setDefaultPaymentMethod(customerId: string, paymentMethodId: string): Promise<void> {
    // No-op in mock
  }

  async listPaymentMethods(customerId: string): Promise<{ id: string; type: string; last4?: string }[]> {
    const methods: { id: string; type: string; last4?: string }[] = []
    for (const [id, pm] of this.paymentMethods) {
      if (pm.customerId === customerId) {
        methods.push({ id, type: pm.type, last4: pm.last4 })
      }
    }
    return methods
  }

  async createSubscription(params: CreateSubscriptionParams): Promise<string> {
    const id = `sub_mock_${Date.now()}`
    this.subscriptions.set(id, params)
    return id
  }

  async updateSubscription(subscriptionId: string, updates: Partial<CreateSubscriptionParams>): Promise<void> {
    const existing = this.subscriptions.get(subscriptionId)
    if (existing) {
      this.subscriptions.set(subscriptionId, { ...existing, ...updates })
    }
  }

  async cancelSubscription(subscriptionId: string, immediately?: boolean): Promise<void> {
    if (immediately) {
      this.subscriptions.delete(subscriptionId)
    }
  }

  async createInvoice(params: CreateInvoiceParams): Promise<string> {
    const id = `in_mock_${Date.now()}`
    this.invoices.set(id, { paid: false, voided: false })
    return id
  }

  async payInvoice(invoiceId: string): Promise<boolean> {
    const invoice = this.invoices.get(invoiceId)
    if (invoice && !invoice.voided) {
      invoice.paid = true
      return true
    }
    return false
  }

  async voidInvoice(invoiceId: string): Promise<void> {
    const invoice = this.invoices.get(invoiceId)
    if (invoice) {
      invoice.voided = true
    }
  }

  constructWebhookEvent(payload: string | Buffer, signature: string): WebhookEvent {
    const data = JSON.parse(typeof payload === 'string' ? payload : payload.toString())
    return {
      id: `evt_mock_${Date.now()}`,
      type: data.type || 'unknown',
      data: data.data || {},
      created: new Date(),
    }
  }

  async handleWebhook(event: WebhookEvent): Promise<void> {
    console.log(`[MockPaymentGateway] Webhook received: ${event.type}`)
  }

  getPublishableKey(): string {
    return 'pk_test_mock'
  }
}

// =============================================================================
// FACTORY
// =============================================================================

let paymentGatewayInstance: PaymentGateway | null = null

export function initializePaymentGateway(
  type: 'stripe' | 'mock',
  config?: StripeConfig
): PaymentGateway {
  if (type === 'stripe' && config) {
    paymentGatewayInstance = new StripePaymentGateway(config)
  } else {
    paymentGatewayInstance = new MockPaymentGateway()
  }
  return paymentGatewayInstance
}

export function getPaymentGateway(): PaymentGateway {
  if (!paymentGatewayInstance) {
    // Default to mock in development
    paymentGatewayInstance = new MockPaymentGateway()
  }
  return paymentGatewayInstance
}
