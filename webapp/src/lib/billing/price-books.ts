/**
 * Price Books & Multi-Currency Support
 * Regional pricing, currency conversion, and localized billing
 */

import { v4 as uuidv4 } from 'uuid'
import type {
  PriceBookEntry,
  Region,
  PlanTier,
  MeterName,
  TaxConfig,
} from './types'
import { PLAN_DEFINITIONS, TAX_CONFIGS, getPlanByCode } from './types'

// =============================================================================
// CURRENCY CONFIGURATION
// =============================================================================

export interface CurrencyConfig {
  code: string
  symbol: string
  name: string
  decimalPlaces: number
  symbolPosition: 'before' | 'after'
}

export const SUPPORTED_CURRENCIES: CurrencyConfig[] = [
  { code: 'USD', symbol: '$', name: 'US Dollar', decimalPlaces: 2, symbolPosition: 'before' },
  { code: 'EUR', symbol: '€', name: 'Euro', decimalPlaces: 2, symbolPosition: 'before' },
  { code: 'GBP', symbol: '£', name: 'British Pound', decimalPlaces: 2, symbolPosition: 'before' },
  { code: 'AUD', symbol: 'A$', name: 'Australian Dollar', decimalPlaces: 2, symbolPosition: 'before' },
  { code: 'JPY', symbol: '¥', name: 'Japanese Yen', decimalPlaces: 0, symbolPosition: 'before' },
  { code: 'CAD', symbol: 'CA$', name: 'Canadian Dollar', decimalPlaces: 2, symbolPosition: 'before' },
  { code: 'CHF', symbol: 'CHF', name: 'Swiss Franc', decimalPlaces: 2, symbolPosition: 'after' },
  { code: 'BRL', symbol: 'R$', name: 'Brazilian Real', decimalPlaces: 2, symbolPosition: 'before' },
  { code: 'MXN', symbol: 'MX$', name: 'Mexican Peso', decimalPlaces: 2, symbolPosition: 'before' },
  { code: 'SGD', symbol: 'S$', name: 'Singapore Dollar', decimalPlaces: 2, symbolPosition: 'before' },
]

export const REGION_DEFAULT_CURRENCY: Record<Region, string> = {
  US: 'USD',
  EU: 'EUR',
  UK: 'GBP',
  APAC: 'SGD',
  LATAM: 'BRL',
  GLOBAL: 'USD',
}

// =============================================================================
// DEPENDENCIES
// =============================================================================

interface PriceBookDependencies {
  // Price book storage
  getPriceBookEntry: (planCode: PlanTier, region: Region, currency: string) => Promise<PriceBookEntry | null>
  getPriceBookEntriesByRegion: (region: Region) => Promise<PriceBookEntry[]>
  savePriceBookEntry: (entry: PriceBookEntry) => Promise<void>

  // Exchange rates
  getExchangeRate: (from: string, to: string) => Promise<number>
}

// =============================================================================
// PRICE BOOK SERVICE
// =============================================================================

export class PriceBookService {
  private deps: PriceBookDependencies
  private rateCache: Map<string, { rate: number; timestamp: number }> = new Map()
  private rateCacheTTL = 3600000 // 1 hour

  constructor(deps: PriceBookDependencies) {
    this.deps = deps
  }

  /**
   * Get pricing for a plan in a specific region/currency
   */
  async getPlanPricing(
    planCode: PlanTier,
    region: Region,
    currency?: string
  ): Promise<{
    currency: string
    monthlyPrice: number
    annualPrice: number
    overagePrices: Partial<Record<MeterName, number>>
    taxConfig: TaxConfig | undefined
  }> {
    const targetCurrency = currency ?? REGION_DEFAULT_CURRENCY[region]

    // Check for explicit price book entry
    const priceBookEntry = await this.deps.getPriceBookEntry(planCode, region, targetCurrency)

    if (priceBookEntry && this.isPriceBookEntryActive(priceBookEntry)) {
      return {
        currency: priceBookEntry.currency,
        monthlyPrice: priceBookEntry.basePriceMonthly,
        annualPrice: priceBookEntry.basePriceAnnual,
        overagePrices: priceBookEntry.overagePrices,
        taxConfig: TAX_CONFIGS.find(t => t.region === region),
      }
    }

    // Fall back to base plan pricing with currency conversion
    const basePlan = getPlanByCode(planCode)
    if (!basePlan) {
      throw new Error(`Plan not found: ${planCode}`)
    }

    const rate = await this.getExchangeRate(basePlan.pricing.currency, targetCurrency)

    // Convert base prices
    const monthlyPrice = this.roundToDecimalPlaces(
      basePlan.pricing.monthly * rate,
      targetCurrency
    )
    const annualPrice = this.roundToDecimalPlaces(
      basePlan.pricing.annual * rate,
      targetCurrency
    )

    // Convert overage prices
    const overagePrices: Partial<Record<MeterName, number>> = {}
    for (const limit of basePlan.limits) {
      if (limit.overagePrice) {
        overagePrices[limit.meter] = this.roundToDecimalPlaces(
          limit.overagePrice * rate,
          targetCurrency,
          4 // More precision for small per-unit prices
        )
      }
    }

    return {
      currency: targetCurrency,
      monthlyPrice,
      annualPrice,
      overagePrices,
      taxConfig: TAX_CONFIGS.find(t => t.region === region),
    }
  }

  /**
   * Check if a price book entry is currently active
   */
  private isPriceBookEntryActive(entry: PriceBookEntry): boolean {
    const now = new Date()
    if (entry.effectiveDate > now) return false
    if (entry.expiresAt && entry.expiresAt < now) return false
    return true
  }

  /**
   * Get exchange rate with caching
   */
  async getExchangeRate(from: string, to: string): Promise<number> {
    if (from === to) return 1

    const cacheKey = `${from}:${to}`
    const cached = this.rateCache.get(cacheKey)

    if (cached && Date.now() - cached.timestamp < this.rateCacheTTL) {
      return cached.rate
    }

    const rate = await this.deps.getExchangeRate(from, to)
    this.rateCache.set(cacheKey, { rate, timestamp: Date.now() })

    return rate
  }

  /**
   * Round to appropriate decimal places for currency
   */
  private roundToDecimalPlaces(
    amount: number,
    currencyCode: string,
    overrideDecimalPlaces?: number
  ): number {
    const currency = SUPPORTED_CURRENCIES.find(c => c.code === currencyCode)
    const decimalPlaces = overrideDecimalPlaces ?? currency?.decimalPlaces ?? 2
    const multiplier = Math.pow(10, decimalPlaces)
    return Math.round(amount * multiplier) / multiplier
  }

  /**
   * Format price for display
   */
  formatPrice(amount: number, currencyCode: string): string {
    const currency = SUPPORTED_CURRENCIES.find(c => c.code === currencyCode)
    if (!currency) return `${amount} ${currencyCode}`

    const formatted = amount.toFixed(currency.decimalPlaces)

    if (currency.symbolPosition === 'before') {
      return `${currency.symbol}${formatted}`
    } else {
      return `${formatted} ${currency.symbol}`
    }
  }

  /**
   * Create a localized price book entry
   */
  async createPriceBookEntry(params: {
    planCode: PlanTier
    region: Region
    currency: string
    basePriceMonthly: number
    basePriceAnnual: number
    overagePrices?: Partial<Record<MeterName, number>>
    effectiveDate?: Date
    expiresAt?: Date
  }): Promise<PriceBookEntry> {
    const entry: PriceBookEntry = {
      id: uuidv4(),
      planCode: params.planCode,
      region: params.region,
      currency: params.currency,
      basePriceMonthly: params.basePriceMonthly,
      basePriceAnnual: params.basePriceAnnual,
      overagePrices: params.overagePrices ?? {},
      effectiveDate: params.effectiveDate ?? new Date(),
      expiresAt: params.expiresAt,
    }

    await this.deps.savePriceBookEntry(entry)

    return entry
  }

  /**
   * Get all pricing for a region
   */
  async getRegionalPricing(region: Region): Promise<{
    currency: string
    plans: {
      planCode: PlanTier
      name: string
      monthlyPrice: number
      annualPrice: number
      formattedMonthly: string
      formattedAnnual: string
    }[]
    taxConfig: TaxConfig | undefined
  }> {
    const currency = REGION_DEFAULT_CURRENCY[region]
    const plans: {
      planCode: PlanTier
      name: string
      monthlyPrice: number
      annualPrice: number
      formattedMonthly: string
      formattedAnnual: string
    }[] = []

    for (const planDef of PLAN_DEFINITIONS) {
      const pricing = await this.getPlanPricing(planDef.code, region, currency)
      plans.push({
        planCode: planDef.code,
        name: planDef.name,
        monthlyPrice: pricing.monthlyPrice,
        annualPrice: pricing.annualPrice,
        formattedMonthly: this.formatPrice(pricing.monthlyPrice, currency),
        formattedAnnual: this.formatPrice(pricing.annualPrice, currency),
      })
    }

    return {
      currency,
      plans,
      taxConfig: TAX_CONFIGS.find(t => t.region === region),
    }
  }

  /**
   * Calculate invoice total with tax for region
   */
  async calculateTotalWithTax(params: {
    subtotal: number
    region: Region
    currency: string
    hasTaxId: boolean
  }): Promise<{
    subtotal: number
    taxRate: number
    taxAmount: number
    taxType: string
    reverseCharge: boolean
    total: number
  }> {
    const taxConfig = TAX_CONFIGS.find(t => t.region === params.region)

    if (!taxConfig || taxConfig.taxType === 'none') {
      return {
        subtotal: params.subtotal,
        taxRate: 0,
        taxAmount: 0,
        taxType: 'none',
        reverseCharge: false,
        total: params.subtotal,
      }
    }

    // Check for reverse charge (B2B in EU with valid VAT ID)
    const reverseCharge = taxConfig.reverseChargeEnabled && params.hasTaxId
    const taxRate = reverseCharge ? 0 : taxConfig.defaultRate
    const taxAmount = this.roundToDecimalPlaces(
      params.subtotal * (taxRate / 100),
      params.currency
    )

    return {
      subtotal: params.subtotal,
      taxRate,
      taxAmount,
      taxType: taxConfig.taxType,
      reverseCharge,
      total: this.roundToDecimalPlaces(params.subtotal + taxAmount, params.currency),
    }
  }

  /**
   * Get currency config by code
   */
  getCurrency(code: string): CurrencyConfig | undefined {
    return SUPPORTED_CURRENCIES.find(c => c.code === code)
  }

  /**
   * Get all supported currencies
   */
  getSupportedCurrencies(): CurrencyConfig[] {
    return [...SUPPORTED_CURRENCIES]
  }

  /**
   * Get default currency for a region
   */
  getDefaultCurrency(region: Region): string {
    return REGION_DEFAULT_CURRENCY[region]
  }

  /**
   * Determine region from country code
   */
  getRegionFromCountry(countryCode: string): Region {
    const regionMap: Record<string, Region> = {
      // US
      US: 'US',
      // EU
      AT: 'EU', BE: 'EU', BG: 'EU', HR: 'EU', CY: 'EU', CZ: 'EU',
      DK: 'EU', EE: 'EU', FI: 'EU', FR: 'EU', DE: 'EU', GR: 'EU',
      HU: 'EU', IE: 'EU', IT: 'EU', LV: 'EU', LT: 'EU', LU: 'EU',
      MT: 'EU', NL: 'EU', PL: 'EU', PT: 'EU', RO: 'EU', SK: 'EU',
      SI: 'EU', ES: 'EU', SE: 'EU',
      // UK
      GB: 'UK', UK: 'UK',
      // APAC
      AU: 'APAC', NZ: 'APAC', SG: 'APAC', HK: 'APAC', JP: 'APAC',
      KR: 'APAC', TW: 'APAC', MY: 'APAC', TH: 'APAC', ID: 'APAC',
      PH: 'APAC', VN: 'APAC', IN: 'APAC',
      // LATAM
      BR: 'LATAM', MX: 'LATAM', AR: 'LATAM', CL: 'LATAM', CO: 'LATAM',
      PE: 'LATAM', VE: 'LATAM', EC: 'LATAM', UY: 'LATAM', PY: 'LATAM',
    }

    return regionMap[countryCode.toUpperCase()] ?? 'GLOBAL'
  }
}

// =============================================================================
// SINGLETON
// =============================================================================

let priceBookServiceInstance: PriceBookService | null = null

export function getPriceBookService(deps?: PriceBookDependencies): PriceBookService {
  if (!priceBookServiceInstance && deps) {
    priceBookServiceInstance = new PriceBookService(deps)
  }
  if (!priceBookServiceInstance) {
    throw new Error('PriceBookService not initialized')
  }
  return priceBookServiceInstance
}

// =============================================================================
// DEFAULT PRICE BOOKS
// =============================================================================

/**
 * Generate default price book entries for all regions
 * These can be loaded into the database as seed data
 */
export function generateDefaultPriceBooks(): PriceBookEntry[] {
  const entries: PriceBookEntry[] = []
  const now = new Date()

  // Exchange rate multipliers (approximate, would be fetched from service in production)
  const rates: Record<string, number> = {
    USD: 1,
    EUR: 0.92,
    GBP: 0.79,
    AUD: 1.53,
    SGD: 1.34,
    BRL: 4.97,
  }

  for (const plan of PLAN_DEFINITIONS) {
    for (const [region, currency] of Object.entries(REGION_DEFAULT_CURRENCY)) {
      const rate = rates[currency] ?? 1

      // Build overage prices
      const overagePrices: Partial<Record<MeterName, number>> = {}
      for (const limit of plan.limits) {
        if (limit.overagePrice) {
          overagePrices[limit.meter] = Math.round(limit.overagePrice * rate * 10000) / 10000
        }
      }

      entries.push({
        id: uuidv4(),
        planCode: plan.code,
        region: region as Region,
        currency,
        basePriceMonthly: Math.round(plan.pricing.monthly * rate),
        basePriceAnnual: Math.round(plan.pricing.annual * rate),
        overagePrices,
        effectiveDate: now,
      })
    }
  }

  return entries
}
