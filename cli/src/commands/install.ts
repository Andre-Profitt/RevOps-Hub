/**
 * Install Command
 * Interactive guided wizard for new customer setup
 */

import inquirer from 'inquirer'
import chalk from 'chalk'
import ora from 'ora'
import { z } from 'zod'
import { FoundryClient } from '../lib/foundry-client.js'
import { generateHandoffReport } from '../lib/reporter.js'

// =============================================================================
// CONFIGURATION SCHEMA
// =============================================================================

const CustomerConfigSchema = z.object({
  customerId: z.string().min(3).max(50).regex(/^[a-z0-9-]+$/, 'Must be lowercase alphanumeric with dashes'),
  customerName: z.string().min(2).max(100),
  tier: z.enum(['starter', 'growth', 'enterprise']),
  primaryCrm: z.enum(['salesforce', 'hubspot']),
  crmInstanceUrl: z.string().url().optional(),
  timezone: z.string().default('America/New_York'),
  currency: z.string().default('USD'),
  fiscalYearStart: z.string().regex(/^\d{2}-\d{2}$/).default('01-01'),
  csOwner: z.string().optional(),
  features: z.object({
    dealPredictions: z.boolean().default(false),
    coachingInsights: z.boolean().default(false),
    scenarioModeling: z.boolean().default(false),
    territoryPlanning: z.boolean().default(false),
    apiAccess: z.boolean().default(false),
  }).optional(),
})

type CustomerConfig = z.infer<typeof CustomerConfigSchema>

// =============================================================================
// INSTALL COMMAND
// =============================================================================

interface InstallOptions {
  config?: string
  dryRun?: boolean
}

export async function installCommand(options: InstallOptions): Promise<void> {
  console.log(chalk.bold('\n🚀 RevOps Customer Setup Wizard\n'))

  let config: CustomerConfig

  if (options.config) {
    // Load existing config
    console.log(chalk.gray(`Loading config from ${options.config}...`))
    // TODO: Load config from file
    throw new Error('Config file loading not implemented yet')
  } else {
    // Interactive prompts
    config = await runInteractiveSetup()
  }

  // Validate config
  const spinner = ora('Validating configuration...').start()
  try {
    CustomerConfigSchema.parse(config)
    spinner.succeed('Configuration validated')
  } catch (error) {
    spinner.fail('Configuration validation failed')
    if (error instanceof z.ZodError) {
      console.log(chalk.red('\nValidation errors:'))
      error.errors.forEach((e) => {
        console.log(chalk.red(`  - ${e.path.join('.')}: ${e.message}`))
      })
    }
    process.exit(1)
  }

  // Confirm
  console.log(chalk.bold('\n📋 Configuration Summary\n'))
  printConfigSummary(config)

  const { confirmed } = await inquirer.prompt([
    {
      type: 'confirm',
      name: 'confirmed',
      message: options.dryRun
        ? 'Preview the provisioning steps?'
        : 'Proceed with provisioning?',
      default: true,
    },
  ])

  if (!confirmed) {
    console.log(chalk.yellow('\nSetup cancelled.'))
    process.exit(0)
  }

  // Execute provisioning
  if (options.dryRun) {
    await dryRunProvisioning(config)
  } else {
    await executeProvisioning(config)
  }
}

// =============================================================================
// INTERACTIVE SETUP
// =============================================================================

async function runInteractiveSetup(): Promise<CustomerConfig> {
  // Step 1: Basic Info
  console.log(chalk.cyan.bold('Step 1: Basic Information\n'))

  const basicInfo = await inquirer.prompt([
    {
      type: 'input',
      name: 'customerName',
      message: 'Customer name:',
      validate: (input: string) => input.length >= 2 || 'Name must be at least 2 characters',
    },
    {
      type: 'input',
      name: 'customerId',
      message: 'Customer ID (lowercase, alphanumeric, dashes):',
      default: (answers: { customerName: string }) =>
        answers.customerName.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, ''),
      validate: (input: string) => /^[a-z0-9-]+$/.test(input) || 'Must be lowercase alphanumeric with dashes',
    },
  ])

  // Step 2: Subscription
  console.log(chalk.cyan.bold('\nStep 2: Subscription Tier\n'))

  const tierInfo = await inquirer.prompt([
    {
      type: 'list',
      name: 'tier',
      message: 'Select subscription tier:',
      choices: [
        {
          name: `${chalk.bold('Starter')} - Basic pipeline visibility (10 users, 10K opportunities)`,
          value: 'starter',
        },
        {
          name: `${chalk.bold('Growth')} - Advanced analytics & AI (50 users, 50K opportunities)`,
          value: 'growth',
        },
        {
          name: `${chalk.bold('Enterprise')} - Full platform, unlimited (500 users, custom)`,
          value: 'enterprise',
        },
      ],
    },
  ])

  // Step 3: CRM
  console.log(chalk.cyan.bold('\nStep 3: CRM Integration\n'))

  const crmInfo = await inquirer.prompt([
    {
      type: 'list',
      name: 'primaryCrm',
      message: 'Primary CRM system:',
      choices: [
        { name: 'Salesforce', value: 'salesforce' },
        { name: 'HubSpot', value: 'hubspot' },
      ],
    },
    {
      type: 'input',
      name: 'crmInstanceUrl',
      message: 'CRM instance URL (optional):',
      when: (answers: { primaryCrm: string }) => answers.primaryCrm === 'salesforce',
    },
  ])

  // Step 4: Localization
  console.log(chalk.cyan.bold('\nStep 4: Localization\n'))

  const localization = await inquirer.prompt([
    {
      type: 'list',
      name: 'timezone',
      message: 'Primary timezone:',
      choices: [
        'America/New_York',
        'America/Chicago',
        'America/Denver',
        'America/Los_Angeles',
        'Europe/London',
        'Europe/Paris',
        'Asia/Tokyo',
        'Asia/Singapore',
        'Australia/Sydney',
      ],
      default: 'America/New_York',
    },
    {
      type: 'list',
      name: 'currency',
      message: 'Currency:',
      choices: ['USD', 'EUR', 'GBP', 'JPY', 'AUD', 'CAD'],
      default: 'USD',
    },
    {
      type: 'input',
      name: 'fiscalYearStart',
      message: 'Fiscal year start (MM-DD):',
      default: '01-01',
      validate: (input: string) => /^\d{2}-\d{2}$/.test(input) || 'Format: MM-DD',
    },
  ])

  // Step 5: Customer Success
  console.log(chalk.cyan.bold('\nStep 5: Customer Success\n'))

  const csInfo = await inquirer.prompt([
    {
      type: 'input',
      name: 'csOwner',
      message: 'CS Owner name (optional):',
    },
  ])

  // Step 6: Features (tier-dependent)
  console.log(chalk.cyan.bold('\nStep 6: Feature Configuration\n'))

  let features = {
    dealPredictions: false,
    coachingInsights: false,
    scenarioModeling: false,
    territoryPlanning: false,
    apiAccess: false,
  }

  if (tierInfo.tier === 'growth' || tierInfo.tier === 'enterprise') {
    const featureAnswers = await inquirer.prompt([
      {
        type: 'checkbox',
        name: 'enabledFeatures',
        message: 'Enable features:',
        choices: [
          { name: 'Deal Predictions (ML)', value: 'dealPredictions', checked: true },
          { name: 'Coaching Insights', value: 'coachingInsights', checked: true },
          ...(tierInfo.tier === 'enterprise'
            ? [
                { name: 'Scenario Modeling', value: 'scenarioModeling', checked: true },
                { name: 'Territory Planning', value: 'territoryPlanning', checked: true },
                { name: 'API Access', value: 'apiAccess', checked: true },
              ]
            : []),
        ],
      },
    ])

    featureAnswers.enabledFeatures.forEach((f: string) => {
      features[f as keyof typeof features] = true
    })
  }

  return {
    ...basicInfo,
    ...tierInfo,
    ...crmInfo,
    ...localization,
    ...csInfo,
    features,
  }
}

// =============================================================================
// PROVISIONING
// =============================================================================

function printConfigSummary(config: CustomerConfig): void {
  console.log(chalk.gray('┌─────────────────────────────────────────────────────────┐'))
  console.log(chalk.gray('│ ') + chalk.bold('Customer:      ') + config.customerName)
  console.log(chalk.gray('│ ') + chalk.bold('ID:            ') + config.customerId)
  console.log(chalk.gray('│ ') + chalk.bold('Tier:          ') + chalk.cyan(config.tier.toUpperCase()))
  console.log(chalk.gray('│ ') + chalk.bold('CRM:           ') + config.primaryCrm)
  console.log(chalk.gray('│ ') + chalk.bold('Timezone:      ') + config.timezone)
  console.log(chalk.gray('│ ') + chalk.bold('Currency:      ') + config.currency)
  console.log(chalk.gray('│ ') + chalk.bold('Fiscal Year:   ') + config.fiscalYearStart)
  if (config.csOwner) {
    console.log(chalk.gray('│ ') + chalk.bold('CS Owner:      ') + config.csOwner)
  }
  console.log(chalk.gray('└─────────────────────────────────────────────────────────┘'))
}

async function dryRunProvisioning(config: CustomerConfig): Promise<void> {
  console.log(chalk.yellow.bold('\n🔍 Dry Run Mode - Previewing Steps\n'))

  const steps = [
    `Create Foundry workspace: /RevOps/${config.customerId}`,
    'Create folder structure:',
    `  - /RevOps/${config.customerId}/Staging`,
    `  - /RevOps/${config.customerId}/Enriched`,
    `  - /RevOps/${config.customerId}/Analytics`,
    `  - /RevOps/${config.customerId}/Dashboard`,
    'Provision core datasets:',
    '  - opportunities, accounts, contacts, activities',
    `Configure ${config.primaryCrm} connector`,
    `Set up ${config.tier} tier features and limits`,
    'Create build schedules',
    'Generate customer configuration file',
    'Record implementation status',
  ]

  steps.forEach((step, idx) => {
    if (step.startsWith('  -')) {
      console.log(chalk.gray(step))
    } else {
      console.log(chalk.green(`${idx + 1}. ${step}`))
    }
  })

  console.log(chalk.yellow('\n✓ Dry run complete. No changes were made.'))
}

async function executeProvisioning(config: CustomerConfig): Promise<void> {
  console.log(chalk.green.bold('\n🛠️  Provisioning Workspace\n'))

  const client = new FoundryClient()

  // Step 1: Create workspace
  const step1 = ora('Creating Foundry workspace...').start()
  try {
    await client.createWorkspace(config.customerId)
    step1.succeed('Workspace created')
  } catch (error) {
    step1.fail('Failed to create workspace')
    throw error
  }

  // Step 2: Create folder structure
  const step2 = ora('Creating folder structure...').start()
  try {
    await client.createFolderStructure(config.customerId, config.tier)
    step2.succeed('Folder structure created')
  } catch (error) {
    step2.fail('Failed to create folders')
    throw error
  }

  // Step 3: Provision datasets
  const step3 = ora('Provisioning core datasets...').start()
  try {
    await client.provisionDatasets(config.customerId, config.tier)
    step3.succeed('Core datasets provisioned')
  } catch (error) {
    step3.fail('Failed to provision datasets')
    throw error
  }

  // Step 4: Configure connector
  const step4 = ora(`Configuring ${config.primaryCrm} connector...`).start()
  try {
    await client.configureConnector(config.customerId, config.primaryCrm, config.crmInstanceUrl)
    step4.succeed('Connector configured')
  } catch (error) {
    step4.fail('Failed to configure connector')
    throw error
  }

  // Step 5: Set up schedules
  const step5 = ora('Setting up build schedules...').start()
  try {
    await client.createSchedules(config.customerId, config.tier)
    step5.succeed('Schedules created')
  } catch (error) {
    step5.fail('Failed to create schedules')
    throw error
  }

  // Step 6: Save config
  const step6 = ora('Saving configuration...').start()
  try {
    await client.saveCustomerConfig(config)
    step6.succeed('Configuration saved')
  } catch (error) {
    step6.fail('Failed to save configuration')
    throw error
  }

  // Step 7: Record implementation status
  const step7 = ora('Recording implementation status...').start()
  try {
    await client.recordImplementationStatus(config.customerId, 'onboarding')
    step7.succeed('Implementation status recorded')
  } catch (error) {
    step7.fail('Failed to record status')
    throw error
  }

  // Generate handoff report
  console.log(chalk.green.bold('\n✅ Provisioning Complete!\n'))

  const report = generateHandoffReport(config)
  console.log(report)
}
