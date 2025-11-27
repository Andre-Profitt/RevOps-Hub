/**
 * Sync Status Tracker
 * Tracks health of external system syncs (CRM, billing, etc.)
 */

import { v4 as uuidv4 } from 'uuid'
import type {
  SyncStatus,
  SyncSystem,
  SyncHealthStatus,
} from './datasets'
import { getEventBus } from '../events/bus'

// =============================================================================
// DEPENDENCIES
// =============================================================================

interface SyncTrackerDependencies {
  getSyncStatus: (tenantId: string, system: SyncSystem) => Promise<SyncStatus | null>
  getSyncStatusesByTenant: (tenantId: string) => Promise<SyncStatus[]>
  getAllUnhealthySyncs: () => Promise<SyncStatus[]>
  saveSyncStatus: (status: SyncStatus) => Promise<void>
}

// =============================================================================
// SYNC TRACKER
// =============================================================================

export class SyncTracker {
  private deps: SyncTrackerDependencies
  private healthCheckInterval: NodeJS.Timeout | null = null

  constructor(deps: SyncTrackerDependencies) {
    this.deps = deps
  }

  /**
   * Start periodic health checks
   */
  start(intervalMs = 300000): void { // Default: 5 minutes
    if (this.healthCheckInterval) return

    this.healthCheckInterval = setInterval(() => {
      this.checkAllSyncs().catch(console.error)
    }, intervalMs)
  }

  /**
   * Stop health checks
   */
  stop(): void {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval)
      this.healthCheckInterval = null
    }
  }

  /**
   * Record a sync start
   */
  async startSync(
    tenantId: string,
    system: SyncSystem,
    direction: 'inbound' | 'outbound' | 'bidirectional' = 'bidirectional'
  ): Promise<SyncStatus> {
    const existing = await this.deps.getSyncStatus(tenantId, system)

    const status: SyncStatus = {
      id: existing?.id ?? uuidv4(),
      tenantId,
      system,
      direction,
      lastSyncAt: new Date(),
      lastSuccessAt: existing?.lastSuccessAt,
      lastErrorAt: existing?.lastErrorAt,
      status: 'unknown',
      message: 'Sync in progress',
      createdAt: existing?.createdAt ?? new Date(),
      updatedAt: new Date(),
    }

    await this.deps.saveSyncStatus(status)
    return status
  }

  /**
   * Record a successful sync
   */
  async recordSuccess(
    tenantId: string,
    system: SyncSystem,
    options: {
      recordsSynced?: number
      message?: string
      nextScheduledAt?: Date
    } = {}
  ): Promise<SyncStatus> {
    const existing = await this.deps.getSyncStatus(tenantId, system)

    const status: SyncStatus = {
      id: existing?.id ?? uuidv4(),
      tenantId,
      system,
      direction: existing?.direction ?? 'bidirectional',
      lastSyncAt: new Date(),
      lastSuccessAt: new Date(),
      lastErrorAt: existing?.lastErrorAt,
      status: 'ok',
      message: options.message ?? 'Sync completed successfully',
      recordsSynced: options.recordsSynced,
      nextScheduledAt: options.nextScheduledAt,
      createdAt: existing?.createdAt ?? new Date(),
      updatedAt: new Date(),
    }

    await this.deps.saveSyncStatus(status)
    return status
  }

  /**
   * Record a sync failure
   */
  async recordError(
    tenantId: string,
    system: SyncSystem,
    error: {
      message: string
      details?: string
      recordsFailed?: number
    }
  ): Promise<SyncStatus> {
    const existing = await this.deps.getSyncStatus(tenantId, system)

    // Determine severity based on history
    let status: SyncHealthStatus = 'error'
    if (existing?.lastSuccessAt) {
      const hoursSinceSuccess = (Date.now() - existing.lastSuccessAt.getTime()) / (1000 * 60 * 60)
      if (hoursSinceSuccess < 1) {
        status = 'warning' // Recent success, might be transient
      }
    }

    const syncStatus: SyncStatus = {
      id: existing?.id ?? uuidv4(),
      tenantId,
      system,
      direction: existing?.direction ?? 'bidirectional',
      lastSyncAt: new Date(),
      lastSuccessAt: existing?.lastSuccessAt,
      lastErrorAt: new Date(),
      status,
      message: error.message,
      errorDetails: error.details,
      recordsFailed: error.recordsFailed,
      createdAt: existing?.createdAt ?? new Date(),
      updatedAt: new Date(),
    }

    await this.deps.saveSyncStatus(syncStatus)

    // Emit alert for errors
    const eventBus = getEventBus()
    eventBus.emit({
      eventId: uuidv4(),
      type: 'health.alert_triggered',
      customerId: tenantId,
      timestamp: new Date(),
      data: {
        alertType: 'sync_error',
        system,
        message: error.message,
        details: error.details,
      },
    })

    return syncStatus
  }

  /**
   * Get sync status for a tenant
   */
  async getStatus(tenantId: string, system: SyncSystem): Promise<SyncStatus | null> {
    return this.deps.getSyncStatus(tenantId, system)
  }

  /**
   * Get all sync statuses for a tenant
   */
  async getAllStatuses(tenantId: string): Promise<SyncStatus[]> {
    return this.deps.getSyncStatusesByTenant(tenantId)
  }

  /**
   * Get overall sync health for a tenant
   */
  async getTenantHealth(tenantId: string): Promise<{
    overallStatus: SyncHealthStatus
    systems: SyncStatus[]
    healthySystems: number
    unhealthySystems: number
  }> {
    const systems = await this.getAllStatuses(tenantId)

    let overallStatus: SyncHealthStatus = 'ok'
    let unhealthySystems = 0

    for (const system of systems) {
      if (system.status === 'error') {
        overallStatus = 'error'
        unhealthySystems++
      } else if (system.status === 'warning' && overallStatus !== 'error') {
        overallStatus = 'warning'
        unhealthySystems++
      } else if (system.status === 'unknown' && overallStatus === 'ok') {
        overallStatus = 'unknown'
      }
    }

    return {
      overallStatus,
      systems,
      healthySystems: systems.length - unhealthySystems,
      unhealthySystems,
    }
  }

  /**
   * Check all syncs for stale status
   */
  async checkAllSyncs(): Promise<SyncStatus[]> {
    const unhealthySyncs = await this.deps.getAllUnhealthySyncs()

    const alertedSyncs: SyncStatus[] = []

    for (const sync of unhealthySyncs) {
      // Check if sync is stale (no activity in expected timeframe)
      const hoursSinceSync = sync.lastSyncAt
        ? (Date.now() - sync.lastSyncAt.getTime()) / (1000 * 60 * 60)
        : Infinity

      // Different thresholds for different systems
      const staleThresholdHours: Record<SyncSystem, number> = {
        crm: 24,
        billing: 24,
        workspace: 48,
        pricing: 168, // weekly
        usage: 1,
        analytics: 24,
      }

      if (hoursSinceSync > staleThresholdHours[sync.system]) {
        // Mark as warning if was ok
        if (sync.status === 'ok') {
          sync.status = 'warning'
          sync.message = `No sync in ${Math.round(hoursSinceSync)} hours`
          sync.updatedAt = new Date()
          await this.deps.saveSyncStatus(sync)
        }

        alertedSyncs.push(sync)

        // Emit alert
        const eventBus = getEventBus()
        eventBus.emit({
          eventId: uuidv4(),
          type: 'health.alert_triggered',
          customerId: sync.tenantId,
          timestamp: new Date(),
          data: {
            alertType: 'sync_stale',
            system: sync.system,
            hoursSinceSync,
            lastSyncAt: sync.lastSyncAt?.toISOString(),
          },
        })
      }
    }

    return alertedSyncs
  }

  /**
   * Get sync analytics across all tenants
   */
  async getGlobalAnalytics(): Promise<{
    bySystem: Record<SyncSystem, {
      total: number
      healthy: number
      warning: number
      error: number
      unknown: number
    }>
    overallHealth: {
      healthy: number
      unhealthy: number
      total: number
    }
  }> {
    const unhealthy = await this.deps.getAllUnhealthySyncs()

    const bySystem: Record<string, {
      total: number
      healthy: number
      warning: number
      error: number
      unknown: number
    }> = {}

    const systems: SyncSystem[] = ['crm', 'billing', 'workspace', 'pricing', 'usage', 'analytics']
    for (const system of systems) {
      bySystem[system] = { total: 0, healthy: 0, warning: 0, error: 0, unknown: 0 }
    }

    let totalUnhealthy = 0

    for (const sync of unhealthy) {
      if (!bySystem[sync.system]) continue

      bySystem[sync.system].total++
      switch (sync.status) {
        case 'ok':
          bySystem[sync.system].healthy++
          break
        case 'warning':
          bySystem[sync.system].warning++
          totalUnhealthy++
          break
        case 'error':
          bySystem[sync.system].error++
          totalUnhealthy++
          break
        case 'unknown':
          bySystem[sync.system].unknown++
          break
      }
    }

    const total = unhealthy.length

    return {
      bySystem: bySystem as Record<SyncSystem, typeof bySystem[string]>,
      overallHealth: {
        healthy: total - totalUnhealthy,
        unhealthy: totalUnhealthy,
        total,
      },
    }
  }
}

// =============================================================================
// SINGLETON
// =============================================================================

let syncTrackerInstance: SyncTracker | null = null

export function getSyncTracker(deps?: SyncTrackerDependencies): SyncTracker {
  if (!syncTrackerInstance && deps) {
    syncTrackerInstance = new SyncTracker(deps)
  }
  if (!syncTrackerInstance) {
    throw new Error('SyncTracker not initialized')
  }
  return syncTrackerInstance
}

// =============================================================================
// CONVENIENCE DECORATORS
// =============================================================================

/**
 * Wrap a sync function with automatic status tracking
 */
export function withSyncTracking<T>(
  tenantId: string,
  system: SyncSystem,
  syncFn: () => Promise<T>,
  options: {
    direction?: 'inbound' | 'outbound' | 'bidirectional'
    getRecordCount?: (result: T) => number
  } = {}
): () => Promise<T> {
  return async () => {
    const tracker = getSyncTracker()
    await tracker.startSync(tenantId, system, options.direction)

    try {
      const result = await syncFn()

      await tracker.recordSuccess(tenantId, system, {
        recordsSynced: options.getRecordCount?.(result),
      })

      return result
    } catch (error) {
      await tracker.recordError(tenantId, system, {
        message: error instanceof Error ? error.message : String(error),
        details: error instanceof Error ? error.stack : undefined,
      })
      throw error
    }
  }
}
