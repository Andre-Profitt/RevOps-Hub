/**
 * Template Pack Loader
 * Utilities for loading and applying template packs
 */

import * as fs from 'fs'
import * as path from 'path'

// =============================================================================
// TYPES
// =============================================================================

export type Tier = 'starter' | 'growth' | 'enterprise'

export interface TemplateManifest {
  name: string
  version: string
  tier: Tier
  description: string
  author: string
  minCliVersion: string
  extends?: Tier
  contents: {
    dashboards: DashboardRef[]
    workflows: WorkflowRef[]
    datasets: string[]
  }
  configuration: {
    required: string[]
    optional: string[]
  }
  limits: Record<string, unknown>
  aiModels?: AIModelConfig[]
  integrations?: IntegrationConfig[]
  security?: SecurityConfig
}

export interface DashboardRef {
  id: string
  name: string
  description: string
  file: string
}

export interface WorkflowRef {
  id: string
  name: string
  description: string
  file: string
}

export interface AIModelConfig {
  id: string
  name: string
  type: string
  features?: string[]
  target?: string
  refreshSchedule: string
}

export interface IntegrationConfig {
  id: string
  name: string
  type: string
  direction?: string
  bidirectional?: boolean
}

export interface SecurityConfig {
  sso?: string[]
  rbac?: boolean
  fieldLevelSecurity?: boolean
  auditLog?: boolean
  dataEncryption?: string
  compliance?: string[]
}

export interface DashboardConfig {
  id: string
  name: string
  version: string
  description: string
  layout: unknown
  widgets: unknown[]
  filters: unknown[]
  refreshInterval: number
}

export interface WorkflowConfig {
  id: string
  name: string
  version: string
  description: string
  trigger: unknown
  steps: unknown[]
  errorHandling: unknown
  notifications?: unknown
}

export interface LoadedTemplate {
  manifest: TemplateManifest
  dashboards: Map<string, DashboardConfig>
  workflows: Map<string, WorkflowConfig>
}

// =============================================================================
// LOADER
// =============================================================================

export class TemplateLoader {
  private templatesDir: string

  constructor(templatesDir?: string) {
    this.templatesDir = templatesDir || path.join(__dirname)
  }

  /**
   * Load template manifest for a tier
   */
  loadManifest(tier: Tier): TemplateManifest {
    const manifestPath = path.join(this.templatesDir, tier, 'manifest.json')
    const content = fs.readFileSync(manifestPath, 'utf-8')
    return JSON.parse(content) as TemplateManifest
  }

  /**
   * Load dashboard configuration
   */
  loadDashboard(tier: Tier, dashboardFile: string): DashboardConfig {
    const dashboardPath = path.join(this.templatesDir, tier, dashboardFile)
    const content = fs.readFileSync(dashboardPath, 'utf-8')
    return JSON.parse(content) as DashboardConfig
  }

  /**
   * Load workflow configuration
   */
  loadWorkflow(tier: Tier, workflowFile: string): WorkflowConfig {
    const workflowPath = path.join(this.templatesDir, tier, workflowFile)
    const content = fs.readFileSync(workflowPath, 'utf-8')
    return JSON.parse(content) as WorkflowConfig
  }

  /**
   * Load complete template with all assets
   */
  loadTemplate(tier: Tier): LoadedTemplate {
    const manifest = this.loadManifest(tier)
    const dashboards = new Map<string, DashboardConfig>()
    const workflows = new Map<string, WorkflowConfig>()

    // Load dashboards
    for (const ref of manifest.contents.dashboards) {
      try {
        const dashboard = this.loadDashboard(tier, ref.file)
        dashboards.set(ref.id, dashboard)
      } catch {
        // Dashboard file may not exist yet, skip
      }
    }

    // Load workflows
    for (const ref of manifest.contents.workflows) {
      try {
        const workflow = this.loadWorkflow(tier, ref.file)
        workflows.set(ref.id, workflow)
      } catch {
        // Workflow file may not exist yet, skip
      }
    }

    return { manifest, dashboards, workflows }
  }

  /**
   * Get effective template with inheritance resolved
   */
  getEffectiveTemplate(tier: Tier): LoadedTemplate {
    const template = this.loadTemplate(tier)

    // If template extends another, merge them
    if (template.manifest.extends) {
      const parentTemplate = this.getEffectiveTemplate(template.manifest.extends)

      // Merge dashboards (child overrides parent)
      for (const [id, dashboard] of parentTemplate.dashboards) {
        if (!template.dashboards.has(id)) {
          template.dashboards.set(id, dashboard)
        }
      }

      // Merge workflows (child overrides parent)
      for (const [id, workflow] of parentTemplate.workflows) {
        if (!template.workflows.has(id)) {
          template.workflows.set(id, workflow)
        }
      }

      // Merge datasets
      const allDatasets = new Set([
        ...parentTemplate.manifest.contents.datasets,
        ...template.manifest.contents.datasets,
      ])
      template.manifest.contents.datasets = Array.from(allDatasets)
    }

    return template
  }

  /**
   * List available templates
   */
  listTemplates(): TemplateManifest[] {
    const tiers: Tier[] = ['starter', 'growth', 'enterprise']
    const templates: TemplateManifest[] = []

    for (const tier of tiers) {
      try {
        templates.push(this.loadManifest(tier))
      } catch {
        // Template not found, skip
      }
    }

    return templates
  }

  /**
   * Validate template configuration
   */
  validateConfig(tier: Tier, config: Record<string, unknown>): { valid: boolean; errors: string[] } {
    const manifest = this.loadManifest(tier)
    const errors: string[] = []

    // Check required fields
    for (const field of manifest.configuration.required) {
      if (!(field in config) || config[field] === undefined || config[field] === '') {
        errors.push(`Missing required field: ${field}`)
      }
    }

    return {
      valid: errors.length === 0,
      errors,
    }
  }
}

// =============================================================================
// TEMPLATE RENDERER
// =============================================================================

export class TemplateRenderer {
  /**
   * Render template string with variables
   */
  static render(template: string, variables: Record<string, unknown>): string {
    return template.replace(/\{\{(\w+)\}\}/g, (match, key) => {
      if (key in variables) {
        return String(variables[key])
      }
      return match
    })
  }

  /**
   * Render object recursively
   */
  static renderObject<T>(obj: T, variables: Record<string, unknown>): T {
    if (typeof obj === 'string') {
      return this.render(obj, variables) as T
    }

    if (Array.isArray(obj)) {
      return obj.map(item => this.renderObject(item, variables)) as T
    }

    if (obj && typeof obj === 'object') {
      const result: Record<string, unknown> = {}
      for (const [key, value] of Object.entries(obj)) {
        result[key] = this.renderObject(value, variables)
      }
      return result as T
    }

    return obj
  }
}

// =============================================================================
// EXPORTS
// =============================================================================

export const loader = new TemplateLoader()

export function loadTemplate(tier: Tier): LoadedTemplate {
  return loader.loadTemplate(tier)
}

export function getEffectiveTemplate(tier: Tier): LoadedTemplate {
  return loader.getEffectiveTemplate(tier)
}

export function listTemplates(): TemplateManifest[] {
  return loader.listTemplates()
}

export function validateConfig(tier: Tier, config: Record<string, unknown>) {
  return loader.validateConfig(tier, config)
}
