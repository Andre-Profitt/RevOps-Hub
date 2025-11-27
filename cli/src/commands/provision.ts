/**
 * Provision Command
 * Provisions Foundry workspace for a customer
 */

import chalk from 'chalk'
import ora from 'ora'
import { FoundryClient } from '../lib/foundry-client.js'
import { generateHandoffReport } from '../lib/reporter.js'

// =============================================================================
// PROVISION COMMAND
// =============================================================================

interface ProvisionOptions {
  tier?: 'starter' | 'growth' | 'enterprise'
  crm?: 'salesforce' | 'hubspot'
  dryRun?: boolean
}

export async function provisionCommand(
  customerId: string,
  options: ProvisionOptions
): Promise<void> {
  const tier = options.tier || 'starter'
  const crm = options.crm || 'salesforce'

  console.log(chalk.bold(`\n🛠️  Provisioning workspace for: ${customerId}\n`))
  console.log(chalk.gray(`Tier: ${tier.toUpperCase()}`))
  console.log(chalk.gray(`CRM: ${crm}`))
  console.log()

  if (options.dryRun) {
    await dryRunProvision(customerId, tier, crm)
    return
  }

  const client = new FoundryClient()

  // Step 1: Check if workspace already exists
  const checkSpinner = ora('Checking existing workspace...').start()
  try {
    const exists = await client.workspaceExists(customerId)
    if (exists) {
      checkSpinner.warn('Workspace already exists')
      console.log(chalk.yellow('\nUse --force to reprovision (not implemented)'))
      process.exit(1)
    }
    checkSpinner.succeed('No existing workspace found')
  } catch {
    checkSpinner.succeed('Ready to provision')
  }

  // Step 2: Create workspace
  const wsSpinner = ora('Creating Foundry workspace...').start()
  try {
    await client.createWorkspace(customerId)
    wsSpinner.succeed('Workspace created')
  } catch (error) {
    wsSpinner.fail('Failed to create workspace')
    console.log(chalk.red(`\nError: ${error instanceof Error ? error.message : 'Unknown error'}`))
    process.exit(1)
  }

  // Step 3: Create folder structure
  const folderSpinner = ora('Creating folder structure...').start()
  try {
    await client.createFolderStructure(customerId, tier)
    folderSpinner.succeed('Folder structure created')
  } catch (error) {
    folderSpinner.fail('Failed to create folders')
    throw error
  }

  // Step 4: Provision datasets
  const datasetSpinner = ora('Provisioning datasets...').start()
  try {
    const datasets = await client.provisionDatasets(customerId, tier)
    datasetSpinner.succeed(`Provisioned ${datasets.length} datasets`)
  } catch (error) {
    datasetSpinner.fail('Failed to provision datasets')
    throw error
  }

  // Step 5: Configure connector stub
  const connectorSpinner = ora(`Configuring ${crm} connector...`).start()
  try {
    await client.configureConnector(customerId, crm)
    connectorSpinner.succeed('Connector configured (pending credentials)')
  } catch (error) {
    connectorSpinner.fail('Failed to configure connector')
    throw error
  }

  // Step 6: Create schedules
  const scheduleSpinner = ora('Creating build schedules...').start()
  try {
    await client.createSchedules(customerId, tier)
    scheduleSpinner.succeed('Schedules created')
  } catch (error) {
    scheduleSpinner.fail('Failed to create schedules')
    throw error
  }

  // Step 7: Record status
  const statusSpinner = ora('Recording implementation status...').start()
  try {
    await client.recordImplementationStatus(customerId, 'onboarding')
    statusSpinner.succeed('Status recorded')
  } catch (error) {
    statusSpinner.fail('Failed to record status')
    throw error
  }

  // Success!
  console.log(chalk.green.bold('\n✅ Provisioning Complete!\n'))

  // Print next steps
  console.log(chalk.bold('Next Steps:'))
  console.log(chalk.gray('1. Configure CRM credentials in Foundry'))
  console.log(chalk.gray('2. Run initial data sync'))
  console.log(chalk.gray('3. Verify data quality'))
  console.log(chalk.gray('4. Enable dashboards'))
  console.log()
  console.log(chalk.gray(`Check status: revops-cli status ${customerId}`))
}

// =============================================================================
// DRY RUN
// =============================================================================

async function dryRunProvision(
  customerId: string,
  tier: string,
  crm: string
): Promise<void> {
  console.log(chalk.yellow.bold('🔍 Dry Run Mode\n'))

  const tierConfig = {
    starter: {
      folders: 5,
      datasets: 12,
      schedules: 2,
    },
    growth: {
      folders: 8,
      datasets: 24,
      schedules: 4,
    },
    enterprise: {
      folders: 12,
      datasets: 48,
      schedules: 6,
    },
  }

  const config = tierConfig[tier as keyof typeof tierConfig] || tierConfig.starter

  console.log(chalk.bold('Would create:'))
  console.log(chalk.gray(`  Workspace:   /RevOps/${customerId}`))
  console.log(chalk.gray(`  Folders:     ${config.folders}`))
  console.log(chalk.gray(`  Datasets:    ${config.datasets}`))
  console.log(chalk.gray(`  Schedules:   ${config.schedules}`))
  console.log(chalk.gray(`  Connector:   ${crm}`))
  console.log()

  console.log(chalk.bold('Folder structure:'))
  const folders = [
    'Staging',
    'Enriched',
    'Analytics',
    'Dashboard',
    'Config',
    ...(tier !== 'starter' ? ['ML', 'Coaching'] : []),
    ...(tier === 'enterprise' ? ['Scenarios', 'Territory', 'Custom'] : []),
  ]
  folders.forEach((f) => console.log(chalk.gray(`  /RevOps/${customerId}/${f}`)))
  console.log()

  console.log(chalk.yellow('✓ Dry run complete. No changes were made.'))
}
