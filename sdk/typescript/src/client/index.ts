/**
 * RevOps API Client
 * TypeScript client for the RevOps Command Center API
 */

import type {
  RevOpsConfig,
  ApiResponse,
  PaginatedResponse,
  Tenant,
  ImplementationStatus,
  HealthScore,
  ROIMetrics,
  WebhookSubscription,
  WebhookDelivery,
  CreateWebhookRequest,
  EventType,
} from '../types'

// =============================================================================
// CLIENT
// =============================================================================

export class RevOpsClient {
  private config: Required<RevOpsConfig>

  constructor(config: RevOpsConfig) {
    this.config = {
      baseUrl: config.baseUrl.replace(/\/$/, ''),
      apiKey: config.apiKey,
      webhookSecret: config.webhookSecret ?? '',
      timeout: config.timeout ?? 30000,
      debug: config.debug ?? false,
    }
  }

  // ===========================================================================
  // HTTP HELPERS
  // ===========================================================================

  private async request<T>(
    method: 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE',
    path: string,
    body?: unknown
  ): Promise<ApiResponse<T>> {
    const url = `${this.config.baseUrl}${path}`

    const headers: Record<string, string> = {
      'Authorization': `Bearer ${this.config.apiKey}`,
      'Content-Type': 'application/json',
      'User-Agent': 'RevOps-SDK-TypeScript/0.1.0',
    }

    const controller = new AbortController()
    const timeoutId = setTimeout(() => controller.abort(), this.config.timeout)

    try {
      if (this.config.debug) {
        console.log(`[RevOps] ${method} ${url}`)
      }

      const response = await fetch(url, {
        method,
        headers,
        body: body ? JSON.stringify(body) : undefined,
        signal: controller.signal,
      })

      const data = await response.json()

      if (!response.ok) {
        return {
          success: false,
          error: {
            code: data.error?.code ?? 'API_ERROR',
            message: data.error?.message ?? response.statusText,
            details: data.error?.details,
          },
          meta: {
            requestId: response.headers.get('x-request-id') ?? '',
            timestamp: new Date().toISOString(),
          },
        }
      }

      return {
        success: true,
        data: data.data ?? data,
        meta: {
          requestId: response.headers.get('x-request-id') ?? '',
          timestamp: new Date().toISOString(),
        },
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Unknown error'
      return {
        success: false,
        error: {
          code: 'REQUEST_FAILED',
          message,
        },
      }
    } finally {
      clearTimeout(timeoutId)
    }
  }

  private get<T>(path: string): Promise<ApiResponse<T>> {
    return this.request<T>('GET', path)
  }

  private post<T>(path: string, body: unknown): Promise<ApiResponse<T>> {
    return this.request<T>('POST', path, body)
  }

  private put<T>(path: string, body: unknown): Promise<ApiResponse<T>> {
    return this.request<T>('PUT', path, body)
  }

  private patch<T>(path: string, body: unknown): Promise<ApiResponse<T>> {
    return this.request<T>('PATCH', path, body)
  }

  private delete<T>(path: string): Promise<ApiResponse<T>> {
    return this.request<T>('DELETE', path)
  }

  // ===========================================================================
  // TENANTS
  // ===========================================================================

  /**
   * Get tenant by ID
   */
  async getTenant(tenantId: string): Promise<ApiResponse<Tenant>> {
    return this.get<Tenant>(`/api/v1/tenants/${tenantId}`)
  }

  /**
   * List all tenants (paginated)
   */
  async listTenants(options?: {
    page?: number
    pageSize?: number
    status?: 'active' | 'suspended' | 'pending'
  }): Promise<PaginatedResponse<Tenant>> {
    const params = new URLSearchParams()
    if (options?.page) params.set('page', String(options.page))
    if (options?.pageSize) params.set('pageSize', String(options.pageSize))
    if (options?.status) params.set('status', options.status)

    const query = params.toString()
    return this.get<Tenant[]>(`/api/v1/tenants${query ? `?${query}` : ''}`)
  }

  /**
   * Update tenant configuration
   */
  async updateTenant(
    tenantId: string,
    updates: Partial<Tenant>
  ): Promise<ApiResponse<Tenant>> {
    return this.patch<Tenant>(`/api/v1/tenants/${tenantId}`, updates)
  }

  // ===========================================================================
  // IMPLEMENTATION STATUS
  // ===========================================================================

  /**
   * Get implementation status for a tenant
   */
  async getImplementationStatus(tenantId: string): Promise<ApiResponse<ImplementationStatus>> {
    return this.get<ImplementationStatus>(`/api/v1/tenants/${tenantId}/implementation`)
  }

  /**
   * Update implementation checklist item
   */
  async updateChecklistItem(
    tenantId: string,
    itemId: string,
    completed: boolean
  ): Promise<ApiResponse<ImplementationStatus>> {
    return this.patch<ImplementationStatus>(
      `/api/v1/tenants/${tenantId}/implementation/checklist/${itemId}`,
      { completed }
    )
  }

  /**
   * Mark implementation stage as complete
   */
  async completeStage(
    tenantId: string,
    stage: string
  ): Promise<ApiResponse<ImplementationStatus>> {
    return this.post<ImplementationStatus>(
      `/api/v1/tenants/${tenantId}/implementation/complete-stage`,
      { stage }
    )
  }

  // ===========================================================================
  // HEALTH SCORES
  // ===========================================================================

  /**
   * Get health score for a tenant
   */
  async getHealthScore(tenantId: string): Promise<ApiResponse<HealthScore>> {
    return this.get<HealthScore>(`/api/v1/tenants/${tenantId}/health`)
  }

  /**
   * Get health score history
   */
  async getHealthHistory(
    tenantId: string,
    options?: { days?: number }
  ): Promise<ApiResponse<HealthScore[]>> {
    const params = new URLSearchParams()
    if (options?.days) params.set('days', String(options.days))

    const query = params.toString()
    return this.get<HealthScore[]>(
      `/api/v1/tenants/${tenantId}/health/history${query ? `?${query}` : ''}`
    )
  }

  // ===========================================================================
  // ROI METRICS
  // ===========================================================================

  /**
   * Get ROI metrics for a tenant
   */
  async getROIMetrics(tenantId: string): Promise<ApiResponse<ROIMetrics>> {
    return this.get<ROIMetrics>(`/api/v1/tenants/${tenantId}/roi`)
  }

  /**
   * Get ROI comparison (before vs after)
   */
  async getROIComparison(tenantId: string): Promise<ApiResponse<{
    baseline: ROIMetrics
    current: ROIMetrics
    improvement: ROIMetrics
  }>> {
    return this.get(`/api/v1/tenants/${tenantId}/roi/comparison`)
  }

  // ===========================================================================
  // WEBHOOKS
  // ===========================================================================

  /**
   * List webhook subscriptions
   */
  async listWebhooks(): Promise<ApiResponse<WebhookSubscription[]>> {
    return this.get<WebhookSubscription[]>('/api/v1/webhooks')
  }

  /**
   * Get webhook by ID
   */
  async getWebhook(webhookId: string): Promise<ApiResponse<WebhookSubscription>> {
    return this.get<WebhookSubscription>(`/api/v1/webhooks/${webhookId}`)
  }

  /**
   * Create webhook subscription
   */
  async createWebhook(request: CreateWebhookRequest): Promise<ApiResponse<WebhookSubscription>> {
    return this.post<WebhookSubscription>('/api/v1/webhooks', request)
  }

  /**
   * Update webhook subscription
   */
  async updateWebhook(
    webhookId: string,
    updates: Partial<CreateWebhookRequest & { isActive: boolean }>
  ): Promise<ApiResponse<WebhookSubscription>> {
    return this.patch<WebhookSubscription>(`/api/v1/webhooks/${webhookId}`, updates)
  }

  /**
   * Delete webhook subscription
   */
  async deleteWebhook(webhookId: string): Promise<ApiResponse<void>> {
    return this.delete<void>(`/api/v1/webhooks/${webhookId}`)
  }

  /**
   * Get webhook delivery history
   */
  async getWebhookDeliveries(
    webhookId: string,
    options?: { page?: number; pageSize?: number }
  ): Promise<PaginatedResponse<WebhookDelivery>> {
    const params = new URLSearchParams()
    if (options?.page) params.set('page', String(options.page))
    if (options?.pageSize) params.set('pageSize', String(options.pageSize))

    const query = params.toString()
    return this.get<WebhookDelivery[]>(
      `/api/v1/webhooks/${webhookId}/deliveries${query ? `?${query}` : ''}`
    )
  }

  /**
   * Retry a failed webhook delivery
   */
  async retryWebhookDelivery(
    webhookId: string,
    deliveryId: string
  ): Promise<ApiResponse<WebhookDelivery>> {
    return this.post<WebhookDelivery>(
      `/api/v1/webhooks/${webhookId}/deliveries/${deliveryId}/retry`,
      {}
    )
  }

  /**
   * Test webhook endpoint
   */
  async testWebhook(
    webhookId: string,
    eventType?: EventType
  ): Promise<ApiResponse<WebhookDelivery>> {
    return this.post<WebhookDelivery>(
      `/api/v1/webhooks/${webhookId}/test`,
      { eventType }
    )
  }

  // ===========================================================================
  // UTILITIES
  // ===========================================================================

  /**
   * Get webhook secret for signature verification
   */
  getWebhookSecret(): string {
    return this.config.webhookSecret
  }

  /**
   * Check API health
   */
  async healthCheck(): Promise<ApiResponse<{ status: string; version: string }>> {
    return this.get<{ status: string; version: string }>('/api/v1/health')
  }
}

// =============================================================================
// FACTORY
// =============================================================================

/**
 * Create a RevOps API client
 *
 * @example
 * ```typescript
 * import { createClient } from '@revops/sdk'
 *
 * const client = createClient({
 *   baseUrl: 'https://api.revops.io',
 *   apiKey: process.env.REVOPS_API_KEY,
 *   webhookSecret: process.env.REVOPS_WEBHOOK_SECRET,
 * })
 *
 * // Get tenant health
 * const health = await client.getHealthScore('tenant-123')
 *
 * // List webhooks
 * const webhooks = await client.listWebhooks()
 * ```
 */
export function createClient(config: RevOpsConfig): RevOpsClient {
  return new RevOpsClient(config)
}
