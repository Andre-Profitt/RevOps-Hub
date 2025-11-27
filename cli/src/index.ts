#!/usr/bin/env node
/**
 * RevOps CLI
 * Command-line tool for customer provisioning and management
 */

import { Command } from 'commander'
import chalk from 'chalk'
import { installCommand } from './commands/install.js'
import { validateCommand } from './commands/validate.js'
import { provisionCommand } from './commands/provision.js'
import { statusCommand } from './commands/status.js'

const program = new Command()

// ASCII art banner
const banner = `
${chalk.blue('╔═══════════════════════════════════════╗')}
${chalk.blue('║')}  ${chalk.bold.white('RevOps Command Center CLI')}           ${chalk.blue('║')}
${chalk.blue('║')}  ${chalk.gray('Customer provisioning & management')}   ${chalk.blue('║')}
${chalk.blue('╚═══════════════════════════════════════╝')}
`

program
  .name('revops-cli')
  .description('CLI for RevOps Command Center customer provisioning and management')
  .version('0.1.0')
  .hook('preAction', () => {
    console.log(banner)
  })

// Install command - guided wizard
program
  .command('install')
  .description('Interactive guided wizard for new customer setup')
  .option('-c, --config <path>', 'Path to existing config file')
  .option('--dry-run', 'Preview changes without applying them')
  .action(installCommand)

// Validate command - validate configuration
program
  .command('validate')
  .description('Validate customer configuration')
  .argument('<config>', 'Path to configuration file or customer ID')
  .option('--strict', 'Enable strict validation mode')
  .action(validateCommand)

// Provision command - provision workspace
program
  .command('provision')
  .description('Provision Foundry workspace for a customer')
  .argument('<customer-id>', 'Customer ID to provision')
  .option('-t, --tier <tier>', 'Subscription tier (starter|growth|enterprise)', 'starter')
  .option('--crm <type>', 'Primary CRM type (salesforce|hubspot)', 'salesforce')
  .option('--dry-run', 'Preview changes without applying them')
  .action(provisionCommand)

// Status command - check status
program
  .command('status')
  .description('Check implementation status for a customer')
  .argument('[customer-id]', 'Customer ID (optional, lists all if omitted)')
  .option('--json', 'Output as JSON')
  .option('--watch', 'Watch for changes')
  .action(statusCommand)

// Parse and execute
program.parse()
