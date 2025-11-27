/**
 * Foundry Client
 * API client for Foundry operations
 */

// =============================================================================
// TYPES
// =============================================================================

interface CustomerConfig {
  customerId: string
  customerName: string
  tier: 'starter' | 'growth' | 'enterprise'
  primaryCrm: 'salesforce' | 'hubspot'
  crmInstanceUrl?: string
  timezone: string
  currency: string
  fiscalYearStart: string
  csOwner?: string
  features?: Record<string, boolean>
}

interface CustomerStatus {
  customerId: string
  customerName: string
  tier: string
  currentStage: string
  completionPct: number
  daysInStage: number
  isBlocked: boolean
  csOwner: string | null
  healthScore: number | null
}

// =============================================================================
// FOUNDRY CLIENT
// =============================================================================

export class FoundryClient {
  private baseUrl: string
  private token: string

  constructor() {
    this.baseUrl = process.env.FOUNDRY_URL || ''
    this.token = process.env.FOUNDRY_TOKEN || ''
  }

  /**
   * Check if workspace exists
   */
  async workspaceExists(customerId: string): Promise<boolean> {
    // In mock mode, always return false
    if (!this.baseUrl || !this.token) {
      return false
    }

    try {
      const response = await this.request(`/api/v2/datasets/ri.foundry.main.folder.${customerId}`)
      return response.ok
    } catch {
      return false
    }
  }

  /**
   * Create workspace
   */
  async createWorkspace(customerId: string): Promise<string> {
    if (!this.baseUrl || !this.token) {
      // Mock mode - simulate delay
      await this.delay(500)
      console.log(`[Mock] Created workspace: /RevOps/${customerId}`)
      return `ri.foundry.main.folder.${customerId}`
    }

    // Real Foundry API call would go here
    const response = await this.request('/api/v2/folders', {
      method: 'POST',
      body: JSON.stringify({
        parentRid: 'ri.foundry.main.folder.revops-root',
        name: customerId,
      }),
    })

    const data = await response.json()
    return data.rid
  }

  /**
   * Create folder structure
   */
  async createFolderStructure(
    customerId: string,
    tier: 'starter' | 'growth' | 'enterprise'
  ): Promise<string[]> {
    const baseFolders = ['Staging', 'Enriched', 'Analytics', 'Dashboard', 'Config']
    const growthFolders = ['ML', 'Coaching']
    const enterpriseFolders = ['Scenarios', 'Territory', 'Custom', 'Exports']

    let folders = [...baseFolders]
    if (tier === 'growth' || tier === 'enterprise') {
      folders = [...folders, ...growthFolders]
    }
    if (tier === 'enterprise') {
      folders = [...folders, ...enterpriseFolders]
    }

    if (!this.baseUrl || !this.token) {
      // Mock mode
      await this.delay(300)
      folders.forEach((f) => {
        console.log(`[Mock] Created folder: /RevOps/${customerId}/${f}`)
      })
      return folders.map((f) => `ri.foundry.main.folder.${customerId}-${f.toLowerCase()}`)
    }

    // Real implementation would create folders in Foundry
    const rids: string[] = []
    for (const folder of folders) {
      const response = await this.request('/api/v2/folders', {
        method: 'POST',
        body: JSON.stringify({
          parentRid: `ri.foundry.main.folder.${customerId}`,
          name: folder,
        }),
      })
      const data = await response.json()
      rids.push(data.rid)
    }

    return rids
  }

  /**
   * Provision datasets
   */
  async provisionDatasets(
    customerId: string,
    tier: 'starter' | 'growth' | 'enterprise'
  ): Promise<string[]> {
    const coreDatasets = [
      'opportunities',
      'accounts',
      'contacts',
      'activities',
      'users',
      'pipeline_health_summary',
      'pipeline_hygiene_alerts',
      'deal_health_scores',
      'forecast_summary',
      'kpis',
    ]

    const growthDatasets = [
      'deal_predictions',
      'rep_performance',
      'coaching_insights',
      'activity_metrics',
    ]

    const enterpriseDatasets = [
      'scenario_results',
      'territory_assignments',
      'account_scores',
      'custom_metrics',
    ]

    let datasets = [...coreDatasets]
    if (tier === 'growth' || tier === 'enterprise') {
      datasets = [...datasets, ...growthDatasets]
    }
    if (tier === 'enterprise') {
      datasets = [...datasets, ...enterpriseDatasets]
    }

    if (!this.baseUrl || !this.token) {
      // Mock mode
      await this.delay(500)
      datasets.forEach((d) => {
        console.log(`[Mock] Created dataset: /RevOps/${customerId}/Staging/${d}`)
      })
      return datasets.map((d) => `ri.foundry.main.dataset.${customerId}-${d}`)
    }

    // Real implementation
    const rids: string[] = []
    for (const dataset of datasets) {
      const response = await this.request('/api/v2/datasets', {
        method: 'POST',
        body: JSON.stringify({
          parentFolderRid: `ri.foundry.main.folder.${customerId}-staging`,
          name: dataset,
        }),
      })
      const data = await response.json()
      rids.push(data.rid)
    }

    return rids
  }

  /**
   * Configure connector
   */
  async configureConnector(
    customerId: string,
    crmType: 'salesforce' | 'hubspot',
    instanceUrl?: string
  ): Promise<void> {
    if (!this.baseUrl || !this.token) {
      // Mock mode
      await this.delay(300)
      console.log(`[Mock] Configured ${crmType} connector for ${customerId}`)
      return
    }

    // Real implementation would configure the connector
    await this.request('/api/v2/connectors', {
      method: 'POST',
      body: JSON.stringify({
        customerId,
        type: crmType,
        instanceUrl,
        status: 'pending_credentials',
      }),
    })
  }

  /**
   * Create schedules
   */
  async createSchedules(
    customerId: string,
    tier: 'starter' | 'growth' | 'enterprise'
  ): Promise<void> {
    const schedules = [
      { name: 'hourly-sync', cron: '0 * * * *', description: 'Hourly data sync' },
      { name: 'daily-analytics', cron: '0 6 * * *', description: 'Daily analytics build' },
    ]

    if (tier === 'growth' || tier === 'enterprise') {
      schedules.push(
        { name: 'daily-ml', cron: '0 7 * * *', description: 'Daily ML predictions' }
      )
    }

    if (tier === 'enterprise') {
      schedules.push(
        { name: 'weekly-reports', cron: '0 8 * * 1', description: 'Weekly executive reports' }
      )
    }

    if (!this.baseUrl || !this.token) {
      // Mock mode
      await this.delay(200)
      schedules.forEach((s) => {
        console.log(`[Mock] Created schedule: ${s.name} (${s.cron})`)
      })
      return
    }

    // Real implementation
    for (const schedule of schedules) {
      await this.request('/api/v2/schedules', {
        method: 'POST',
        body: JSON.stringify({
          customerId,
          ...schedule,
        }),
      })
    }
  }

  /**
   * Save customer config
   */
  async saveCustomerConfig(config: CustomerConfig): Promise<void> {
    if (!this.baseUrl || !this.token) {
      // Mock mode
      await this.delay(200)
      console.log(`[Mock] Saved config for ${config.customerId}`)
      return
    }

    await this.request('/api/v2/configs', {
      method: 'POST',
      body: JSON.stringify(config),
    })
  }

  /**
   * Record implementation status
   */
  async recordImplementationStatus(
    customerId: string,
    stage: string
  ): Promise<void> {
    if (!this.baseUrl || !this.token) {
      // Mock mode
      await this.delay(100)
      console.log(`[Mock] Recorded status: ${customerId} -> ${stage}`)
      return
    }

    await this.request('/api/ops/implementation', {
      method: 'POST',
      body: JSON.stringify({
        customerId,
        currentStage: stage,
        timestamp: new Date().toISOString(),
      }),
    })
  }

  /**
   * Get customer status
   */
  async getCustomerStatus(customerId: string): Promise<CustomerStatus> {
    if (!this.baseUrl || !this.token) {
      // Mock mode - return sample data
      return {
        customerId,
        customerName: customerId.replace(/-/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase()),
        tier: 'growth',
        currentStage: 'dashboard_config',
        completionPct: 60,
        daysInStage: 5,
        isBlocked: false,
        csOwner: 'Sarah Johnson',
        healthScore: 78,
      }
    }

    const response = await this.request(`/api/ops/implementation/${customerId}`)
    return response.json()
  }

  /**
   * Get all customer statuses
   */
  async getAllCustomerStatuses(): Promise<CustomerStatus[]> {
    if (!this.baseUrl || !this.token) {
      // Mock mode - return sample data
      return [
        {
          customerId: 'acme-corp',
          customerName: 'Acme Corporation',
          tier: 'enterprise',
          currentStage: 'live',
          completionPct: 100,
          daysInStage: 45,
          isBlocked: false,
          csOwner: 'Sarah Johnson',
          healthScore: 92,
        },
        {
          customerId: 'techstart-io',
          customerName: 'TechStart.io',
          tier: 'growth',
          currentStage: 'agent_training',
          completionPct: 80,
          daysInStage: 7,
          isBlocked: false,
          csOwner: 'Mike Chen',
          healthScore: 78,
        },
        {
          customerId: 'global-retail',
          customerName: 'Global Retail Inc',
          tier: 'enterprise',
          currentStage: 'dashboard_config',
          completionPct: 60,
          daysInStage: 18,
          isBlocked: true,
          csOwner: 'Sarah Johnson',
          healthScore: 55,
        },
      ]
    }

    const response = await this.request('/api/ops/implementation')
    return response.json()
  }

  // =============================================================================
  // PRIVATE HELPERS
  // =============================================================================

  private async request(
    path: string,
    options: RequestInit = {}
  ): Promise<Response> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${this.token}`,
        ...options.headers,
      },
    })

    if (!response.ok) {
      throw new Error(`Foundry API error: ${response.status} ${response.statusText}`)
    }

    return response
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms))
  }
}
