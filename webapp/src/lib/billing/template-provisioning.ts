/**
 * Template Pack Provisioning
 * Automatically provision template packs based on subscription plan
 */

import { v4 as uuidv4 } from 'uuid'
import type { PlanTier, Subscription, AddOnCode } from './types'
import { getPlanByCode, getAddOnByCode } from './types'
import { getEventBus } from '../events/bus'

// =============================================================================
// TYPES
// =============================================================================

export interface TemplatePack {
  id: string
  code: string
  name: string
  description: string
  category: 'dashboards' | 'reports' | 'workflows' | 'integrations' | 'alerts' | 'ai'
  components: TemplateComponent[]
  requiredPlan?: PlanTier[]
  requiredAddOn?: AddOnCode
}

export interface TemplateComponent {
  id: string
  type: 'dashboard' | 'report' | 'workflow' | 'integration' | 'alert' | 'ai_model'
  name: string
  configTemplate: Record<string, unknown>
}

export interface TenantTemplateStatus {
  tenantId: string
  templatePackCode: string
  status: 'pending' | 'provisioned' | 'failed' | 'removed'
  provisionedAt?: Date
  removedAt?: Date
  error?: string
}

// =============================================================================
// TEMPLATE PACK DEFINITIONS
// =============================================================================

export const TEMPLATE_PACKS: TemplatePack[] = [
  {
    id: 'pack_starter',
    code: 'starter',
    name: 'Starter Pack',
    description: 'Essential dashboards and reports for getting started',
    category: 'dashboards',
    components: [
      {
        id: 'comp_pipeline_dash',
        type: 'dashboard',
        name: 'Pipeline Overview',
        configTemplate: {
          widgets: ['pipeline_funnel', 'deal_velocity', 'stage_conversion'],
          refresh: 'hourly',
        },
      },
      {
        id: 'comp_forecast_summary',
        type: 'report',
        name: 'Forecast Summary Report',
        configTemplate: {
          metrics: ['commit', 'best_case', 'pipeline'],
          groupBy: 'owner',
        },
      },
      {
        id: 'comp_activity_tracker',
        type: 'dashboard',
        name: 'Activity Tracker',
        configTemplate: {
          widgets: ['calls_today', 'emails_sent', 'meetings_scheduled'],
          timeRange: 'week',
        },
      },
    ],
    requiredPlan: ['starter', 'growth', 'enterprise'],
  },
  {
    id: 'pack_growth',
    code: 'growth',
    name: 'Growth Pack',
    description: 'AI-powered insights and advanced analytics',
    category: 'ai',
    components: [
      {
        id: 'comp_deal_scoring',
        type: 'ai_model',
        name: 'AI Deal Scoring',
        configTemplate: {
          model: 'deal_win_probability',
          features: ['engagement', 'timing', 'firmographics', 'champion'],
          refreshFrequency: 'daily',
        },
      },
      {
        id: 'comp_risk_alerts',
        type: 'alert',
        name: 'Deal Risk Alerts',
        configTemplate: {
          triggers: ['no_activity_7d', 'champion_left', 'budget_cut'],
          channels: ['email', 'slack'],
        },
      },
      {
        id: 'comp_rep_analytics',
        type: 'dashboard',
        name: 'Rep Performance Analytics',
        configTemplate: {
          widgets: ['quota_attainment', 'activity_heatmap', 'conversion_trends'],
          benchmarking: true,
        },
      },
      {
        id: 'comp_win_loss',
        type: 'report',
        name: 'Win/Loss Analysis',
        configTemplate: {
          metrics: ['win_rate', 'avg_deal_size', 'sales_cycle'],
          segmentation: ['industry', 'deal_size', 'competitor'],
        },
      },
    ],
    requiredPlan: ['growth', 'enterprise'],
  },
  {
    id: 'pack_enterprise',
    code: 'enterprise',
    name: 'Enterprise Pack',
    description: 'Full suite for enterprise revenue operations',
    category: 'workflows',
    components: [
      {
        id: 'comp_territory_planning',
        type: 'dashboard',
        name: 'Territory Planning',
        configTemplate: {
          features: ['geo_mapping', 'account_scoring', 'capacity_planning'],
          integrations: ['salesforce_territories'],
        },
      },
      {
        id: 'comp_exec_dashboard',
        type: 'dashboard',
        name: 'Executive Dashboard',
        configTemplate: {
          widgets: ['arr_waterfall', 'net_retention', 'sales_efficiency', 'cac_payback'],
          drilldown: true,
        },
      },
      {
        id: 'comp_approval_workflows',
        type: 'workflow',
        name: 'Deal Approval Workflows',
        configTemplate: {
          triggers: ['discount_over_threshold', 'non_standard_terms', 'large_deal'],
          approvers: ['manager', 'deal_desk', 'finance'],
        },
      },
      {
        id: 'comp_custom_integrations',
        type: 'integration',
        name: 'Custom Integration Framework',
        configTemplate: {
          endpoints: ['inbound_webhook', 'outbound_sync', 'bulk_export'],
          auth: ['oauth2', 'api_key'],
        },
      },
    ],
    requiredPlan: ['enterprise'],
  },
  {
    id: 'pack_coaching',
    code: 'coaching_ai',
    name: 'Coaching AI Pack',
    description: 'AI-powered sales coaching and rep development',
    category: 'ai',
    components: [
      {
        id: 'comp_call_analysis',
        type: 'ai_model',
        name: 'Call Analysis AI',
        configTemplate: {
          features: ['talk_ratio', 'filler_words', 'objection_handling'],
          feedback: 'real_time',
        },
      },
      {
        id: 'comp_coaching_dashboard',
        type: 'dashboard',
        name: 'Coaching Dashboard',
        configTemplate: {
          widgets: ['skill_radar', 'improvement_trends', 'peer_comparison'],
          privacy: 'manager_only',
        },
      },
      {
        id: 'comp_playbook_recommender',
        type: 'ai_model',
        name: 'Playbook Recommender',
        configTemplate: {
          inputs: ['deal_stage', 'buyer_persona', 'objections'],
          outputs: ['next_best_action', 'talk_track', 'content'],
        },
      },
    ],
    requiredAddOn: 'coaching_ai',
  },
  {
    id: 'pack_conversation_intel',
    code: 'conversation_intelligence',
    name: 'Conversation Intelligence Pack',
    description: 'Deep analysis of sales conversations',
    category: 'ai',
    components: [
      {
        id: 'comp_transcription',
        type: 'integration',
        name: 'Call Transcription',
        configTemplate: {
          providers: ['gong', 'chorus', 'native'],
          languages: ['en', 'es', 'fr', 'de'],
        },
      },
      {
        id: 'comp_competitor_mentions',
        type: 'alert',
        name: 'Competitor Mention Alerts',
        configTemplate: {
          competitors: 'configurable',
          notification: ['slack', 'email'],
        },
      },
      {
        id: 'comp_conversation_dashboard',
        type: 'dashboard',
        name: 'Conversation Analytics',
        configTemplate: {
          widgets: ['sentiment_trends', 'topic_frequency', 'winning_patterns'],
        },
      },
    ],
    requiredAddOn: 'conversation_intelligence',
  },
]

// =============================================================================
// DEPENDENCIES
// =============================================================================

interface TemplateProvisioningDependencies {
  // Template status tracking
  getTemplateStatus: (tenantId: string, templateCode: string) => Promise<TenantTemplateStatus | null>
  saveTemplateStatus: (status: TenantTemplateStatus) => Promise<void>
  getTemplateStatusesForTenant: (tenantId: string) => Promise<TenantTemplateStatus[]>

  // Actual provisioning (these would create resources in the system)
  provisionDashboard: (tenantId: string, component: TemplateComponent) => Promise<void>
  provisionReport: (tenantId: string, component: TemplateComponent) => Promise<void>
  provisionWorkflow: (tenantId: string, component: TemplateComponent) => Promise<void>
  provisionIntegration: (tenantId: string, component: TemplateComponent) => Promise<void>
  provisionAlert: (tenantId: string, component: TemplateComponent) => Promise<void>
  provisionAIModel: (tenantId: string, component: TemplateComponent) => Promise<void>

  // Deprovisioning
  deprovisionComponent: (tenantId: string, componentId: string, type: string) => Promise<void>
}

// =============================================================================
// TEMPLATE PROVISIONING SERVICE
// =============================================================================

export class TemplateProvisioningService {
  private deps: TemplateProvisioningDependencies

  constructor(deps: TemplateProvisioningDependencies) {
    this.deps = deps
  }

  /**
   * Get template packs available for a plan
   */
  getAvailablePacksForPlan(planCode: PlanTier): TemplatePack[] {
    return TEMPLATE_PACKS.filter(pack => {
      if (pack.requiredAddOn) return false // Add-on packs handled separately
      if (!pack.requiredPlan) return true
      return pack.requiredPlan.includes(planCode)
    })
  }

  /**
   * Get template packs available with an add-on
   */
  getPacksForAddOn(addOnCode: AddOnCode): TemplatePack[] {
    return TEMPLATE_PACKS.filter(pack => pack.requiredAddOn === addOnCode)
  }

  /**
   * Provision all template packs for a new subscription
   */
  async provisionForSubscription(subscription: Subscription): Promise<void> {
    const plan = getPlanByCode(subscription.planCode)
    if (!plan) {
      throw new Error('Invalid plan')
    }

    const packsToProvision = plan.defaultTemplatePacks

    for (const packCode of packsToProvision) {
      await this.provisionPack(subscription.tenantId, packCode)
    }
  }

  /**
   * Provision a single template pack for a tenant
   */
  async provisionPack(tenantId: string, packCode: string): Promise<void> {
    const pack = TEMPLATE_PACKS.find(p => p.code === packCode)
    if (!pack) {
      throw new Error(`Template pack not found: ${packCode}`)
    }

    // Check if already provisioned
    const existingStatus = await this.deps.getTemplateStatus(tenantId, packCode)
    if (existingStatus?.status === 'provisioned') {
      return // Already provisioned
    }

    // Create pending status
    const status: TenantTemplateStatus = {
      tenantId,
      templatePackCode: packCode,
      status: 'pending',
    }
    await this.deps.saveTemplateStatus(status)

    try {
      // Provision each component
      for (const component of pack.components) {
        await this.provisionComponent(tenantId, component)
      }

      // Update status to provisioned
      status.status = 'provisioned'
      status.provisionedAt = new Date()
      await this.deps.saveTemplateStatus(status)

      // Emit event
      const eventBus = getEventBus()
      eventBus.emit({
        eventId: uuidv4(),
        type: 'billing.plan_changed',
        customerId: tenantId,
        timestamp: new Date(),
        data: {
          event: 'template_pack_provisioned',
          packCode,
          packName: pack.name,
          componentCount: pack.components.length,
        },
      })
    } catch (error) {
      status.status = 'failed'
      status.error = error instanceof Error ? error.message : 'Unknown error'
      await this.deps.saveTemplateStatus(status)
      throw error
    }
  }

  /**
   * Provision a single component
   */
  private async provisionComponent(
    tenantId: string,
    component: TemplateComponent
  ): Promise<void> {
    switch (component.type) {
      case 'dashboard':
        await this.deps.provisionDashboard(tenantId, component)
        break
      case 'report':
        await this.deps.provisionReport(tenantId, component)
        break
      case 'workflow':
        await this.deps.provisionWorkflow(tenantId, component)
        break
      case 'integration':
        await this.deps.provisionIntegration(tenantId, component)
        break
      case 'alert':
        await this.deps.provisionAlert(tenantId, component)
        break
      case 'ai_model':
        await this.deps.provisionAIModel(tenantId, component)
        break
    }
  }

  /**
   * Handle plan upgrade - provision new packs
   */
  async handlePlanUpgrade(
    tenantId: string,
    oldPlanCode: PlanTier,
    newPlanCode: PlanTier
  ): Promise<void> {
    const oldPlan = getPlanByCode(oldPlanCode)
    const newPlan = getPlanByCode(newPlanCode)

    if (!oldPlan || !newPlan) {
      throw new Error('Invalid plan')
    }

    // Find packs that are new in the upgraded plan
    const oldPacks = new Set(oldPlan.defaultTemplatePacks)
    const newPacks = newPlan.defaultTemplatePacks.filter(p => !oldPacks.has(p))

    // Provision new packs
    for (const packCode of newPacks) {
      await this.provisionPack(tenantId, packCode)
    }
  }

  /**
   * Handle plan downgrade - mark packs as removed (don't delete data)
   */
  async handlePlanDowngrade(
    tenantId: string,
    oldPlanCode: PlanTier,
    newPlanCode: PlanTier
  ): Promise<void> {
    const oldPlan = getPlanByCode(oldPlanCode)
    const newPlan = getPlanByCode(newPlanCode)

    if (!oldPlan || !newPlan) {
      throw new Error('Invalid plan')
    }

    // Find packs that are no longer available
    const newPacks = new Set(newPlan.defaultTemplatePacks)
    const removedPacks = oldPlan.defaultTemplatePacks.filter(p => !newPacks.has(p))

    // Mark as removed (but don't delete - data preservation)
    for (const packCode of removedPacks) {
      await this.removePack(tenantId, packCode, false)
    }
  }

  /**
   * Handle add-on added - provision add-on packs
   */
  async handleAddOnAdded(tenantId: string, addOnCode: AddOnCode): Promise<void> {
    const packs = this.getPacksForAddOn(addOnCode)

    for (const pack of packs) {
      await this.provisionPack(tenantId, pack.code)
    }
  }

  /**
   * Handle add-on removed - remove add-on packs
   */
  async handleAddOnRemoved(tenantId: string, addOnCode: AddOnCode): Promise<void> {
    const packs = this.getPacksForAddOn(addOnCode)

    for (const pack of packs) {
      await this.removePack(tenantId, pack.code, false)
    }
  }

  /**
   * Remove a template pack (optionally delete data)
   */
  async removePack(
    tenantId: string,
    packCode: string,
    deleteData: boolean = false
  ): Promise<void> {
    const pack = TEMPLATE_PACKS.find(p => p.code === packCode)
    if (!pack) return

    const status = await this.deps.getTemplateStatus(tenantId, packCode)
    if (!status || status.status !== 'provisioned') return

    if (deleteData) {
      // Actually delete the resources
      for (const component of pack.components) {
        await this.deps.deprovisionComponent(tenantId, component.id, component.type)
      }
    }

    // Update status
    status.status = 'removed'
    status.removedAt = new Date()
    await this.deps.saveTemplateStatus(status)

    // Emit event
    const eventBus = getEventBus()
    eventBus.emit({
      eventId: uuidv4(),
      type: 'billing.plan_changed',
      customerId: tenantId,
      timestamp: new Date(),
      data: {
        event: 'template_pack_removed',
        packCode,
        dataDeleted: deleteData,
      },
    })
  }

  /**
   * Get provisioning status for a tenant
   */
  async getTenantProvisioningStatus(tenantId: string): Promise<{
    provisioned: TenantTemplateStatus[]
    pending: TenantTemplateStatus[]
    failed: TenantTemplateStatus[]
    removed: TenantTemplateStatus[]
  }> {
    const statuses = await this.deps.getTemplateStatusesForTenant(tenantId)

    return {
      provisioned: statuses.filter(s => s.status === 'provisioned'),
      pending: statuses.filter(s => s.status === 'pending'),
      failed: statuses.filter(s => s.status === 'failed'),
      removed: statuses.filter(s => s.status === 'removed'),
    }
  }

  /**
   * Retry failed provisioning
   */
  async retryFailedProvisioning(tenantId: string): Promise<void> {
    const statuses = await this.deps.getTemplateStatusesForTenant(tenantId)
    const failed = statuses.filter(s => s.status === 'failed')

    for (const status of failed) {
      await this.provisionPack(tenantId, status.templatePackCode)
    }
  }

  /**
   * Get template pack by code
   */
  getPackByCode(code: string): TemplatePack | undefined {
    return TEMPLATE_PACKS.find(p => p.code === code)
  }

  /**
   * Get all template packs
   */
  getAllPacks(): TemplatePack[] {
    return [...TEMPLATE_PACKS]
  }
}

// =============================================================================
// SINGLETON
// =============================================================================

let templateProvisioningServiceInstance: TemplateProvisioningService | null = null

export function getTemplateProvisioningService(
  deps?: TemplateProvisioningDependencies
): TemplateProvisioningService {
  if (!templateProvisioningServiceInstance && deps) {
    templateProvisioningServiceInstance = new TemplateProvisioningService(deps)
  }
  if (!templateProvisioningServiceInstance) {
    throw new Error('TemplateProvisioningService not initialized')
  }
  return templateProvisioningServiceInstance
}
