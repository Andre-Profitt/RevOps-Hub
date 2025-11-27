/**
 * Status Command
 * Check implementation status for customers
 */

import chalk from 'chalk'
import ora from 'ora'
import { FoundryClient } from '../lib/foundry-client.js'

// =============================================================================
// STATUS TYPES
// =============================================================================

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

const STAGE_EMOJI: Record<string, string> = {
  onboarding: '📝',
  connector_setup: '🔌',
  dashboard_config: '📊',
  agent_training: '🤖',
  live: '✅',
}

const STAGE_COLORS: Record<string, (text: string) => string> = {
  onboarding: chalk.gray,
  connector_setup: chalk.blue,
  dashboard_config: chalk.magenta,
  agent_training: chalk.yellow,
  live: chalk.green,
}

// =============================================================================
// STATUS COMMAND
// =============================================================================

interface StatusOptions {
  json?: boolean
  watch?: boolean
}

export async function statusCommand(
  customerId?: string,
  options: StatusOptions = {}
): Promise<void> {
  const client = new FoundryClient()

  if (options.watch) {
    await watchStatus(client, customerId)
    return
  }

  if (customerId) {
    await showSingleCustomerStatus(client, customerId, options)
  } else {
    await showAllCustomersStatus(client, options)
  }
}

// =============================================================================
// SINGLE CUSTOMER STATUS
// =============================================================================

async function showSingleCustomerStatus(
  client: FoundryClient,
  customerId: string,
  options: StatusOptions
): Promise<void> {
  const spinner = ora(`Loading status for ${customerId}...`).start()

  try {
    const status = await client.getCustomerStatus(customerId)
    spinner.stop()

    if (options.json) {
      console.log(JSON.stringify(status, null, 2))
      return
    }

    printDetailedStatus(status)
  } catch (error) {
    spinner.fail('Failed to load status')
    console.log(chalk.red(`\nError: ${error instanceof Error ? error.message : 'Customer not found'}`))
    process.exit(1)
  }
}

function printDetailedStatus(status: CustomerStatus): void {
  const stageColor = STAGE_COLORS[status.currentStage] || chalk.gray
  const stageEmoji = STAGE_EMOJI[status.currentStage] || '❓'

  console.log()
  console.log(chalk.bold('═══════════════════════════════════════════════════════'))
  console.log(chalk.bold.white(`  ${status.customerName}`))
  console.log(chalk.gray(`  ${status.customerId}`))
  console.log(chalk.bold('═══════════════════════════════════════════════════════'))
  console.log()

  // Status row
  console.log(chalk.gray('  Stage:        ') + stageColor(`${stageEmoji} ${formatStage(status.currentStage)}`))
  console.log(chalk.gray('  Progress:     ') + renderProgressBar(status.completionPct, 30))
  console.log(chalk.gray('  Tier:         ') + chalk.cyan(status.tier.toUpperCase()))
  console.log(chalk.gray('  Days in stage:') + ` ${status.daysInStage}`)

  if (status.isBlocked) {
    console.log(chalk.red.bold('\n  ⚠️  BLOCKED'))
  }

  if (status.csOwner) {
    console.log(chalk.gray('\n  CS Owner:     ') + status.csOwner)
  }

  if (status.healthScore !== null) {
    const healthColor = status.healthScore >= 80 ? chalk.green :
                        status.healthScore >= 60 ? chalk.yellow : chalk.red
    console.log(chalk.gray('  Health Score: ') + healthColor(`${status.healthScore}%`))
  }

  console.log()

  // Stage checklist
  console.log(chalk.bold('  Implementation Stages:'))
  console.log()

  const stages = ['onboarding', 'connector_setup', 'dashboard_config', 'agent_training', 'live']
  const currentIdx = stages.indexOf(status.currentStage)

  stages.forEach((stage, idx) => {
    const emoji = STAGE_EMOJI[stage]
    const color = idx < currentIdx ? chalk.green :
                  idx === currentIdx ? chalk.yellow : chalk.gray

    const checkmark = idx < currentIdx ? '✓' :
                      idx === currentIdx ? '→' : '○'

    console.log(color(`    ${checkmark} ${emoji} ${formatStage(stage)}`))
  })

  console.log()
}

// =============================================================================
// ALL CUSTOMERS STATUS
// =============================================================================

async function showAllCustomersStatus(
  client: FoundryClient,
  options: StatusOptions
): Promise<void> {
  const spinner = ora('Loading all customer statuses...').start()

  try {
    const statuses = await client.getAllCustomerStatuses()
    spinner.stop()

    if (options.json) {
      console.log(JSON.stringify(statuses, null, 2))
      return
    }

    if (statuses.length === 0) {
      console.log(chalk.yellow('\nNo customers found.'))
      return
    }

    printStatusTable(statuses)
  } catch (error) {
    spinner.fail('Failed to load statuses')
    console.log(chalk.red(`\nError: ${error instanceof Error ? error.message : 'Unknown error'}`))
    process.exit(1)
  }
}

function printStatusTable(statuses: CustomerStatus[]): void {
  console.log()
  console.log(chalk.bold('Customer Implementation Status'))
  console.log(chalk.gray('─'.repeat(90)))

  // Header
  console.log(
    chalk.gray(padRight('Customer', 25)) +
    chalk.gray(padRight('Stage', 18)) +
    chalk.gray(padRight('Progress', 12)) +
    chalk.gray(padRight('Days', 8)) +
    chalk.gray(padRight('Tier', 12)) +
    chalk.gray('Status')
  )
  console.log(chalk.gray('─'.repeat(90)))

  // Rows
  statuses.forEach((status) => {
    const stageColor = STAGE_COLORS[status.currentStage] || chalk.gray
    const stageEmoji = STAGE_EMOJI[status.currentStage] || '❓'

    const statusIcon = status.isBlocked ? chalk.red('⚠️ Blocked') :
                       status.currentStage === 'live' ? chalk.green('✅ Live') :
                       chalk.blue('In Progress')

    console.log(
      padRight(truncate(status.customerName, 23), 25) +
      stageColor(padRight(`${stageEmoji} ${formatStage(status.currentStage)}`, 18)) +
      padRight(`${status.completionPct}%`, 12) +
      padRight(`${status.daysInStage}d`, 8) +
      chalk.cyan(padRight(status.tier, 12)) +
      statusIcon
    )
  })

  console.log(chalk.gray('─'.repeat(90)))

  // Summary
  const total = statuses.length
  const live = statuses.filter(s => s.currentStage === 'live').length
  const blocked = statuses.filter(s => s.isBlocked).length
  const inProgress = total - live

  console.log()
  console.log(chalk.gray(`Total: ${total}`) +
              chalk.green(` | Live: ${live}`) +
              chalk.blue(` | In Progress: ${inProgress}`) +
              (blocked > 0 ? chalk.red(` | Blocked: ${blocked}`) : ''))
  console.log()
}

// =============================================================================
// WATCH MODE
// =============================================================================

async function watchStatus(
  client: FoundryClient,
  customerId?: string
): Promise<void> {
  console.log(chalk.cyan('\n👀 Watching for status changes... (Ctrl+C to exit)\n'))

  const refresh = async () => {
    console.clear()
    console.log(chalk.cyan(`Last updated: ${new Date().toLocaleTimeString()}\n`))

    if (customerId) {
      await showSingleCustomerStatus(client, customerId, {})
    } else {
      await showAllCustomersStatus(client, {})
    }
  }

  await refresh()

  // Refresh every 30 seconds
  setInterval(refresh, 30000)

  // Keep process alive
  process.stdin.resume()
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

function formatStage(stage: string): string {
  return stage
    .split('_')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ')
}

function renderProgressBar(percent: number, width: number): string {
  const filled = Math.round((percent / 100) * width)
  const empty = width - filled

  const color = percent >= 80 ? chalk.green :
                percent >= 50 ? chalk.yellow : chalk.red

  return color('█'.repeat(filled)) + chalk.gray('░'.repeat(empty)) + ` ${percent}%`
}

function padRight(str: string, len: number): string {
  return str.padEnd(len)
}

function truncate(str: string, len: number): string {
  if (str.length <= len) return str
  return str.slice(0, len - 1) + '…'
}
