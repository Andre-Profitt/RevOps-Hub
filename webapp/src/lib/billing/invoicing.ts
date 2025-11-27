/**
 * Invoicing Service
 * Invoice generation with overage calculation and proration
 */

import { v4 as uuidv4 } from 'uuid'
import type {
  Invoice,
  InvoiceLineItem,
  InvoiceStatus,
  Subscription,
  SubscriptionAddOn,
  MeterName,
  Region,
} from './types'
import { getPlanByCode, getAddOnByCode, getTaxConfig, calculateAddOnPrice, getMeterDisplayName } from './types'
import { getUsageTracker } from './usage'

// =============================================================================
// DEPENDENCIES
// =============================================================================

interface InvoicingDependencies {
  getSubscription: (subscriptionId: string) => Promise<Subscription | null>
  getSubscriptionAddOns: (subscriptionId: string) => Promise<SubscriptionAddOn[]>
  saveInvoice: (invoice: Invoice) => Promise<void>
  getInvoice: (invoiceId: string) => Promise<Invoice | null>
  getInvoicesBySubscription: (subscriptionId: string) => Promise<Invoice[]>
  generateInvoiceNumber: () => Promise<string>
  getTenantRegion: (tenantId: string) => Promise<Region>
  getTenantTaxId: (tenantId: string) => Promise<string | null>
}

// =============================================================================
// INVOICING SERVICE
// =============================================================================

export class InvoicingService {
  private deps: InvoicingDependencies

  constructor(deps: InvoicingDependencies) {
    this.deps = deps
  }

  /**
   * Generate invoice for a billing period
   */
  async generateInvoice(
    subscriptionId: string,
    periodStart: Date,
    periodEnd: Date
  ): Promise<Invoice> {
    const subscription = await this.deps.getSubscription(subscriptionId)
    if (!subscription) {
      throw new Error('Subscription not found')
    }

    const plan = getPlanByCode(subscription.planCode)
    if (!plan) {
      throw new Error('Plan not found')
    }

    const addOns = await this.deps.getSubscriptionAddOns(subscriptionId)
    const lineItems: InvoiceLineItem[] = []
    const invoiceId = uuidv4()

    // 1. Base subscription charge
    const basePrice = subscription.billingInterval === 'annual'
      ? plan.pricing.annual / 12
      : plan.pricing.monthly

    lineItems.push({
      id: uuidv4(),
      invoiceId,
      type: 'subscription',
      description: `${plan.name} Plan - ${subscription.billingInterval === 'annual' ? 'Annual' : 'Monthly'}`,
      quantity: 1,
      unitPrice: basePrice,
      amount: basePrice,
      periodStart,
      periodEnd,
    })

    // 2. Add-on charges
    for (const addOn of addOns) {
      if (addOn.deactivatedAt && addOn.deactivatedAt < periodStart) continue

      const addOnDef = getAddOnByCode(addOn.addOnCode)
      if (!addOnDef) continue

      const addOnPrice = addOn.priceOverride ?? calculateAddOnPrice(addOnDef, addOn.quantity)

      lineItems.push({
        id: uuidv4(),
        invoiceId,
        type: 'addon',
        description: `${addOnDef.name}${addOn.quantity > 1 ? ` x ${addOn.quantity}` : ''}`,
        quantity: addOn.quantity,
        unitPrice: addOnPrice / addOn.quantity,
        amount: addOnPrice,
        periodStart,
        periodEnd,
      })
    }

    // 3. Overage charges
    const period = `${periodStart.getFullYear()}-${String(periodStart.getMonth() + 1).padStart(2, '0')}`
    const usageTracker = getUsageTracker()
    const overages = await usageTracker.calculateOverages(subscription.tenantId, period)

    for (const overage of overages) {
      lineItems.push({
        id: uuidv4(),
        invoiceId,
        type: 'overage',
        description: `${getMeterDisplayName(overage.meter)} overage (${overage.overage.toLocaleString()} over ${overage.included.toLocaleString()} included)`,
        meter: overage.meter,
        quantity: overage.overage,
        unitPrice: overage.unitPrice,
        amount: overage.total,
        periodStart,
        periodEnd,
      })
    }

    // Calculate subtotal
    const subtotal = lineItems.reduce((sum, item) => sum + item.amount, 0)

    // Apply discount
    const discountAmount = subscription.discountPercent
      ? subtotal * (subscription.discountPercent / 100)
      : 0

    // Calculate tax
    const region = await this.deps.getTenantRegion(subscription.tenantId)
    const taxId = await this.deps.getTenantTaxId(subscription.tenantId)
    const taxConfig = getTaxConfig(region)

    let taxAmount = 0
    if (taxConfig && taxConfig.defaultRate > 0) {
      // Check for reverse charge (B2B in EU with valid VAT)
      const isReverseCharge = taxConfig.reverseChargeEnabled && taxId
      if (!isReverseCharge) {
        taxAmount = (subtotal - discountAmount) * (taxConfig.defaultRate / 100)

        lineItems.push({
          id: uuidv4(),
          invoiceId,
          type: 'tax',
          description: `${taxConfig.taxType.toUpperCase()} (${taxConfig.defaultRate}%)`,
          quantity: 1,
          unitPrice: taxAmount,
          amount: taxAmount,
        })
      }
    }

    // Calculate totals
    const total = subtotal - discountAmount + taxAmount
    const dueDate = new Date(periodEnd)
    dueDate.setDate(dueDate.getDate() + 30) // Net 30

    const invoice: Invoice = {
      id: invoiceId,
      subscriptionId,
      tenantId: subscription.tenantId,
      number: await this.deps.generateInvoiceNumber(),
      periodStart,
      periodEnd,
      dueDate,
      currency: subscription.currency,
      subtotal,
      discountAmount,
      taxAmount,
      total,
      amountDue: total,
      amountPaid: 0,
      status: 'draft',
      lineItems,
      createdAt: new Date(),
      updatedAt: new Date(),
    }

    await this.deps.saveInvoice(invoice)

    return invoice
  }

  /**
   * Calculate proration for mid-cycle plan change
   */
  async calculateProration(
    subscriptionId: string,
    newPlanCode: string,
    effectiveDate: Date
  ): Promise<{
    credit: number
    charge: number
    net: number
    lineItems: Omit<InvoiceLineItem, 'id' | 'invoiceId'>[]
  }> {
    const subscription = await this.deps.getSubscription(subscriptionId)
    if (!subscription) {
      throw new Error('Subscription not found')
    }

    const currentPlan = getPlanByCode(subscription.planCode)
    const newPlan = getPlanByCode(newPlanCode as any)

    if (!currentPlan || !newPlan) {
      throw new Error('Invalid plan')
    }

    // Calculate remaining days in current period
    const totalDays = Math.ceil(
      (subscription.contractEndDate.getTime() - subscription.contractStartDate.getTime()) /
      (24 * 60 * 60 * 1000)
    )
    const remainingDays = Math.ceil(
      (subscription.contractEndDate.getTime() - effectiveDate.getTime()) /
      (24 * 60 * 60 * 1000)
    )

    // Calculate daily rates
    const currentPrice = subscription.billingInterval === 'annual'
      ? currentPlan.pricing.annual
      : currentPlan.pricing.monthly * 12
    const newPrice = subscription.billingInterval === 'annual'
      ? newPlan.pricing.annual
      : newPlan.pricing.monthly * 12

    const currentDailyRate = currentPrice / 365
    const newDailyRate = newPrice / 365

    // Calculate credit and charge
    const credit = currentDailyRate * remainingDays
    const charge = newDailyRate * remainingDays
    const net = charge - credit

    const lineItems: Omit<InvoiceLineItem, 'id' | 'invoiceId'>[] = []

    if (credit > 0) {
      lineItems.push({
        type: 'credit',
        description: `Credit for unused ${currentPlan.name} (${remainingDays} days)`,
        quantity: remainingDays,
        unitPrice: -currentDailyRate,
        amount: -credit,
        periodStart: effectiveDate,
        periodEnd: subscription.contractEndDate,
      })
    }

    if (charge > 0) {
      lineItems.push({
        type: 'proration',
        description: `${newPlan.name} prorated charge (${remainingDays} days)`,
        quantity: remainingDays,
        unitPrice: newDailyRate,
        amount: charge,
        periodStart: effectiveDate,
        periodEnd: subscription.contractEndDate,
      })
    }

    return { credit, charge, net, lineItems }
  }

  /**
   * Finalize and open an invoice for payment
   */
  async finalizeInvoice(invoiceId: string): Promise<Invoice> {
    const invoice = await this.deps.getInvoice(invoiceId)
    if (!invoice) {
      throw new Error('Invoice not found')
    }

    if (invoice.status !== 'draft') {
      throw new Error('Only draft invoices can be finalized')
    }

    const updatedInvoice: Invoice = {
      ...invoice,
      status: 'open',
      updatedAt: new Date(),
    }

    await this.deps.saveInvoice(updatedInvoice)

    return updatedInvoice
  }

  /**
   * Mark invoice as paid
   */
  async markPaid(invoiceId: string, amountPaid: number): Promise<Invoice> {
    const invoice = await this.deps.getInvoice(invoiceId)
    if (!invoice) {
      throw new Error('Invoice not found')
    }

    const newAmountPaid = invoice.amountPaid + amountPaid
    const newAmountDue = Math.max(0, invoice.total - newAmountPaid)

    const updatedInvoice: Invoice = {
      ...invoice,
      amountPaid: newAmountPaid,
      amountDue: newAmountDue,
      status: newAmountDue === 0 ? 'paid' : 'open',
      paidAt: newAmountDue === 0 ? new Date() : undefined,
      updatedAt: new Date(),
    }

    await this.deps.saveInvoice(updatedInvoice)

    return updatedInvoice
  }

  /**
   * Void an invoice
   */
  async voidInvoice(invoiceId: string, reason: string): Promise<Invoice> {
    const invoice = await this.deps.getInvoice(invoiceId)
    if (!invoice) {
      throw new Error('Invoice not found')
    }

    if (invoice.status === 'paid') {
      throw new Error('Cannot void a paid invoice')
    }

    const updatedInvoice: Invoice = {
      ...invoice,
      status: 'void',
      updatedAt: new Date(),
    }

    await this.deps.saveInvoice(updatedInvoice)

    return updatedInvoice
  }

  /**
   * Get invoice summary for tenant
   */
  async getInvoiceSummary(subscriptionId: string): Promise<{
    totalInvoiced: number
    totalPaid: number
    totalOutstanding: number
    invoiceCount: number
  }> {
    const invoices = await this.deps.getInvoicesBySubscription(subscriptionId)

    const activeInvoices = invoices.filter(i => i.status !== 'void')

    return {
      totalInvoiced: activeInvoices.reduce((sum, i) => sum + i.total, 0),
      totalPaid: activeInvoices.reduce((sum, i) => sum + i.amountPaid, 0),
      totalOutstanding: activeInvoices.reduce((sum, i) => sum + i.amountDue, 0),
      invoiceCount: activeInvoices.length,
    }
  }
}

// =============================================================================
// SINGLETON
// =============================================================================

let invoicingServiceInstance: InvoicingService | null = null

export function getInvoicingService(deps?: InvoicingDependencies): InvoicingService {
  if (!invoicingServiceInstance && deps) {
    invoicingServiceInstance = new InvoicingService(deps)
  }
  if (!invoicingServiceInstance) {
    throw new Error('InvoicingService not initialized')
  }
  return invoicingServiceInstance
}
