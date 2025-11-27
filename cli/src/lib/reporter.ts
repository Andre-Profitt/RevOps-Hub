/**
 * Handoff Report Generator
 * Generates implementation handoff reports for CS
 */

import chalk from 'chalk'

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

/**
 * Generate a handoff report for CS team
 */
export function generateHandoffReport(config: CustomerConfig): string {
  const timestamp = new Date().toISOString()
  const workspacePath = `/RevOps/${config.customerId}`

  const lines: string[] = [
    '',
    chalk.bold('═══════════════════════════════════════════════════════════════'),
    chalk.bold.cyan('  IMPLEMENTATION HANDOFF REPORT'),
    chalk.bold('═══════════════════════════════════════════════════════════════'),
    '',
    chalk.bold('  Customer Information'),
    chalk.gray('  ─────────────────────────────────────────────────────────────'),
    `  ${chalk.gray('Name:')}          ${config.customerName}`,
    `  ${chalk.gray('ID:')}            ${config.customerId}`,
    `  ${chalk.gray('Tier:')}          ${chalk.cyan(config.tier.toUpperCase())}`,
    `  ${chalk.gray('Primary CRM:')}   ${config.primaryCrm}`,
    config.crmInstanceUrl ? `  ${chalk.gray('CRM URL:')}       ${config.crmInstanceUrl}` : '',
    `  ${chalk.gray('Timezone:')}      ${config.timezone}`,
    `  ${chalk.gray('Currency:')}      ${config.currency}`,
    `  ${chalk.gray('Fiscal Year:')}   Starts ${config.fiscalYearStart}`,
    config.csOwner ? `  ${chalk.gray('CS Owner:')}      ${config.csOwner}` : '',
    '',
    chalk.bold('  Foundry Workspace'),
    chalk.gray('  ─────────────────────────────────────────────────────────────'),
    `  ${chalk.gray('Path:')}          ${workspacePath}`,
    `  ${chalk.gray('Staging:')}       ${workspacePath}/Staging`,
    `  ${chalk.gray('Analytics:')}     ${workspacePath}/Analytics`,
    `  ${chalk.gray('Dashboard:')}     ${workspacePath}/Dashboard`,
    '',
    chalk.bold('  Next Steps'),
    chalk.gray('  ─────────────────────────────────────────────────────────────'),
    `  ${chalk.yellow('1.')} Configure ${config.primaryCrm} credentials in Foundry`,
    `  ${chalk.yellow('2.')} Run initial data sync and verify data quality`,
    `  ${chalk.yellow('3.')} Enable Pipeline Health dashboard`,
    `  ${chalk.yellow('4.')} Configure hygiene alert thresholds`,
    `  ${chalk.yellow('5.')} Schedule training call with customer`,
    '',
    chalk.bold('  Access Links'),
    chalk.gray('  ─────────────────────────────────────────────────────────────'),
    `  ${chalk.gray('Workspace:')}     https://foundry.example.com${workspacePath}`,
    `  ${chalk.gray('Dashboard:')}     https://revops-hub.example.com/${config.customerId}`,
    `  ${chalk.gray('Ops Status:')}    https://revops-hub.example.com/ops/customers/${config.customerId}`,
    '',
    chalk.bold('  Implementation Checklist'),
    chalk.gray('  ─────────────────────────────────────────────────────────────'),
    `  ${chalk.green('✓')} Workspace created`,
    `  ${chalk.green('✓')} Folder structure set up`,
    `  ${chalk.green('✓')} Core datasets provisioned`,
    `  ${chalk.yellow('○')} CRM connector configured (pending credentials)`,
    `  ${chalk.gray('○')} Initial data sync`,
    `  ${chalk.gray('○')} Data quality validation`,
    `  ${chalk.gray('○')} Dashboard enablement`,
    `  ${chalk.gray('○')} Agent configuration`,
    `  ${chalk.gray('○')} Go-live approval`,
    '',
    chalk.gray(`  Generated: ${timestamp}`),
    '',
    chalk.bold('═══════════════════════════════════════════════════════════════'),
    '',
  ]

  return lines.filter(Boolean).join('\n')
}

/**
 * Generate a JSON report for programmatic use
 */
export function generateJSONReport(config: CustomerConfig): object {
  return {
    timestamp: new Date().toISOString(),
    customer: {
      id: config.customerId,
      name: config.customerName,
      tier: config.tier,
      crm: config.primaryCrm,
      crmUrl: config.crmInstanceUrl,
      timezone: config.timezone,
      currency: config.currency,
      fiscalYearStart: config.fiscalYearStart,
      csOwner: config.csOwner,
    },
    workspace: {
      path: `/RevOps/${config.customerId}`,
      folders: [
        'Staging',
        'Enriched',
        'Analytics',
        'Dashboard',
        'Config',
        ...(config.tier !== 'starter' ? ['ML', 'Coaching'] : []),
        ...(config.tier === 'enterprise' ? ['Scenarios', 'Territory', 'Custom'] : []),
      ],
    },
    implementationStatus: {
      currentStage: 'onboarding',
      completionPct: 20,
      checklist: {
        workspaceCreated: true,
        folderStructure: true,
        datasetsProvisioned: true,
        connectorConfigured: false,
        initialSync: false,
        dataQualityValidated: false,
        dashboardsEnabled: false,
        agentsConfigured: false,
        goLiveApproved: false,
      },
    },
    nextSteps: [
      'Configure CRM credentials',
      'Run initial data sync',
      'Verify data quality',
      'Enable dashboards',
      'Schedule training',
    ],
    links: {
      workspace: `https://foundry.example.com/RevOps/${config.customerId}`,
      dashboard: `https://revops-hub.example.com/${config.customerId}`,
      opsStatus: `https://revops-hub.example.com/ops/customers/${config.customerId}`,
    },
  }
}

/**
 * Generate markdown report for documentation
 */
export function generateMarkdownReport(config: CustomerConfig): string {
  const timestamp = new Date().toISOString()

  return `# Implementation Handoff Report

## Customer Information

| Field | Value |
|-------|-------|
| Name | ${config.customerName} |
| ID | ${config.customerId} |
| Tier | ${config.tier.toUpperCase()} |
| Primary CRM | ${config.primaryCrm} |
| Timezone | ${config.timezone} |
| Currency | ${config.currency} |
| Fiscal Year Start | ${config.fiscalYearStart} |
${config.csOwner ? `| CS Owner | ${config.csOwner} |` : ''}

## Workspace

- **Path:** \`/RevOps/${config.customerId}\`
- **Staging:** \`/RevOps/${config.customerId}/Staging\`
- **Analytics:** \`/RevOps/${config.customerId}/Analytics\`
- **Dashboard:** \`/RevOps/${config.customerId}/Dashboard\`

## Implementation Checklist

- [x] Workspace created
- [x] Folder structure set up
- [x] Core datasets provisioned
- [ ] CRM connector configured
- [ ] Initial data sync
- [ ] Data quality validation
- [ ] Dashboard enablement
- [ ] Agent configuration
- [ ] Go-live approval

## Next Steps

1. Configure ${config.primaryCrm} credentials in Foundry
2. Run initial data sync and verify data quality
3. Enable Pipeline Health dashboard
4. Configure hygiene alert thresholds
5. Schedule training call with customer

## Links

- [Foundry Workspace](https://foundry.example.com/RevOps/${config.customerId})
- [Dashboard](https://revops-hub.example.com/${config.customerId})
- [Ops Status](https://revops-hub.example.com/ops/customers/${config.customerId})

---
*Generated: ${timestamp}*
`
}
