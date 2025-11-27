/**
 * Validate Command
 * Validates customer configuration files
 */

import chalk from 'chalk'
import ora from 'ora'
import { z } from 'zod'
import fs from 'fs/promises'
import path from 'path'

// =============================================================================
// VALIDATION SCHEMA
// =============================================================================

const FullConfigSchema = z.object({
  customerId: z.string().min(3).max(50).regex(/^[a-z0-9-]+$/),
  customerName: z.string().min(2).max(100),
  tier: z.enum(['starter', 'growth', 'enterprise']),
  primaryCrm: z.enum(['salesforce', 'hubspot']),
  crmInstanceUrl: z.string().url().optional(),
  timezone: z.string(),
  currency: z.string(),
  currencySymbol: z.string().optional(),
  dateFormat: z.string().optional(),
  fiscalYearStart: z.string().regex(/^\d{2}-\d{2}$/),
  features: z.object({
    pipelineHealth: z.boolean(),
    pipelineHygiene: z.boolean(),
    forecastSummary: z.boolean(),
    dealPredictions: z.boolean(),
    coachingInsights: z.boolean(),
    repPerformance: z.boolean(),
    activityAnalytics: z.boolean(),
    scenarioModeling: z.boolean(),
    territoryPlanning: z.boolean(),
    customIntegrations: z.boolean(),
    apiAccess: z.boolean(),
    ssoEnabled: z.boolean(),
    whiteLabeling: z.boolean(),
    dataExport: z.boolean(),
  }).partial(),
  maxUsers: z.number().positive(),
  maxOpportunities: z.number().positive(),
  dataRetentionDays: z.number().positive(),
  thresholds: z.object({
    health: z.object({
      critical: z.number(),
      atRisk: z.number(),
      monitor: z.number(),
      healthy: z.number(),
    }).optional(),
    hygiene: z.object({
      daysStaleWarning: z.number(),
      daysStakeCritical: z.number(),
      daysPastCloseWarning: z.number(),
      daysPastCloseCritical: z.number(),
    }).optional(),
  }).optional(),
  alerts: z.object({
    enabled: z.boolean(),
    defaultChannels: z.array(z.enum(['email', 'slack', 'webhook', 'pagerduty'])),
    slackWebhookUrl: z.string().url().optional(),
    slackChannel: z.string().optional(),
    emailRecipients: z.array(z.string().email()).optional(),
  }).optional(),
})

// =============================================================================
// VALIDATE COMMAND
// =============================================================================

interface ValidateOptions {
  strict?: boolean
}

export async function validateCommand(
  configPath: string,
  options: ValidateOptions
): Promise<void> {
  console.log(chalk.bold('\n🔍 Configuration Validator\n'))

  const spinner = ora('Loading configuration...').start()

  let config: unknown

  try {
    // Check if it's a file path or customer ID
    if (configPath.endsWith('.json') || configPath.endsWith('.yaml')) {
      const content = await fs.readFile(configPath, 'utf-8')
      config = JSON.parse(content)
      spinner.succeed(`Loaded config from ${configPath}`)
    } else {
      // Assume it's a customer ID, look for config file
      const possiblePaths = [
        `configs/${configPath}.json`,
        `../configs/${configPath}.json`,
        `../../configs/${configPath}.json`,
      ]

      let found = false
      for (const p of possiblePaths) {
        try {
          const content = await fs.readFile(p, 'utf-8')
          config = JSON.parse(content)
          spinner.succeed(`Loaded config for customer: ${configPath}`)
          found = true
          break
        } catch {
          // Try next path
        }
      }

      if (!found) {
        spinner.fail(`Config not found for customer: ${configPath}`)
        process.exit(1)
      }
    }
  } catch (error) {
    spinner.fail('Failed to load configuration')
    console.log(chalk.red(`\nError: ${error instanceof Error ? error.message : 'Unknown error'}`))
    process.exit(1)
  }

  // Validate
  const validationSpinner = ora('Validating configuration...').start()

  const result = FullConfigSchema.safeParse(config)

  if (result.success) {
    validationSpinner.succeed('Configuration is valid')

    // Additional checks in strict mode
    if (options.strict) {
      const warnings = runStrictChecks(result.data)
      if (warnings.length > 0) {
        console.log(chalk.yellow('\n⚠️  Strict mode warnings:'))
        warnings.forEach((w) => console.log(chalk.yellow(`  - ${w}`)))
      }
    }

    // Print summary
    console.log(chalk.bold('\n📋 Configuration Summary\n'))
    printValidationSummary(result.data)

    console.log(chalk.green('\n✅ Validation passed'))
  } else {
    validationSpinner.fail('Configuration validation failed')

    console.log(chalk.red('\n❌ Validation errors:\n'))
    result.error.errors.forEach((e) => {
      console.log(chalk.red(`  ${e.path.join('.')}: ${e.message}`))
    })

    process.exit(1)
  }
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

function runStrictChecks(config: z.infer<typeof FullConfigSchema>): string[] {
  const warnings: string[] = []

  // Check tier-feature alignment
  if (config.tier === 'starter') {
    if (config.features?.dealPredictions) {
      warnings.push('Deal predictions enabled but tier is Starter (requires Growth+)')
    }
    if (config.features?.apiAccess) {
      warnings.push('API access enabled but tier is Starter (requires Enterprise)')
    }
  }

  if (config.tier === 'growth') {
    if (config.features?.scenarioModeling) {
      warnings.push('Scenario modeling enabled but tier is Growth (requires Enterprise)')
    }
    if (config.features?.territoryPlanning) {
      warnings.push('Territory planning enabled but tier is Growth (requires Enterprise)')
    }
  }

  // Check limits
  const tierLimits = {
    starter: { users: 10, opportunities: 10000 },
    growth: { users: 50, opportunities: 50000 },
    enterprise: { users: 500, opportunities: 500000 },
  }

  const limits = tierLimits[config.tier]
  if (config.maxUsers > limits.users) {
    warnings.push(`maxUsers (${config.maxUsers}) exceeds tier limit (${limits.users})`)
  }
  if (config.maxOpportunities > limits.opportunities) {
    warnings.push(`maxOpportunities (${config.maxOpportunities}) exceeds tier limit (${limits.opportunities})`)
  }

  // Check alerts
  if (config.alerts?.enabled && (!config.alerts.defaultChannels || config.alerts.defaultChannels.length === 0)) {
    warnings.push('Alerts enabled but no default channels configured')
  }

  if (config.alerts?.defaultChannels?.includes('slack') && !config.alerts.slackWebhookUrl) {
    warnings.push('Slack channel enabled but no webhook URL configured')
  }

  return warnings
}

function printValidationSummary(config: z.infer<typeof FullConfigSchema>): void {
  console.log(chalk.gray('Customer:       ') + config.customerName)
  console.log(chalk.gray('ID:             ') + config.customerId)
  console.log(chalk.gray('Tier:           ') + chalk.cyan(config.tier.toUpperCase()))
  console.log(chalk.gray('CRM:            ') + config.primaryCrm)
  console.log(chalk.gray('Max Users:      ') + config.maxUsers)
  console.log(chalk.gray('Max Opps:       ') + config.maxOpportunities.toLocaleString())
  console.log(chalk.gray('Retention:      ') + config.dataRetentionDays + ' days')

  if (config.features) {
    const enabledFeatures = Object.entries(config.features)
      .filter(([, v]) => v)
      .map(([k]) => k)
    console.log(chalk.gray('Features:       ') + enabledFeatures.length + ' enabled')
  }

  if (config.alerts?.enabled) {
    console.log(chalk.gray('Alerts:         ') + config.alerts.defaultChannels?.join(', ') || 'None')
  }
}
