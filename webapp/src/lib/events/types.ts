/**
 * Event Types for RevOps Command Center
 * Defines all event types that can be emitted and subscribed to
 */

// =============================================================================
// EVENT TYPE CONSTANTS
// =============================================================================

export const EVENT_TYPES = {
  // Tenant lifecycle
  TENANT_CREATED: 'tenant.created',
  TENANT_UPDATED: 'tenant.updated',
  TENANT_DELETED: 'tenant.deleted',
  TENANT_UPGRADED: 'tenant.upgraded',
  TENANT_DOWNGRADED: 'tenant.downgraded',

  // Implementation stages
  IMPLEMENTATION_STAGE_CHANGED: 'implementation.stage_changed',
  IMPLEMENTATION_BLOCKED: 'implementation.blocked',
  IMPLEMENTATION_UNBLOCKED: 'implementation.unblocked',
  IMPLEMENTATION_COMPLETED: 'implementation.completed',

  // Data pipeline
  DATASET_BUILT: 'dataset.built',
  BUILD_FAILED: 'build.failed',
  BUILD_STARTED: 'build.started',
  SYNC_COMPLETED: 'sync.completed',
  SYNC_FAILED: 'sync.failed',

  // Alerts & notifications
  ALERT_TRIGGERED: 'alert.triggered',
  ALERT_RESOLVED: 'alert.resolved',

  // Agent actions
  AGENT_ACTION_PENDING: 'agent.action_pending',
  AGENT_ACTION_EXECUTED: 'agent.action_executed',
  AGENT_ACTION_FAILED: 'agent.action_failed',

  // Escalations
  ESCALATION_CREATED: 'escalation.created',
  ESCALATION_ACKNOWLEDGED: 'escalation.acknowledged',
  ESCALATION_RESOLVED: 'escalation.resolved',

  // Usage & billing
  USAGE_THRESHOLD_CROSSED: 'usage.threshold_crossed',
  USAGE_LIMIT_REACHED: 'usage.limit_reached',
  BILLING_EVENT: 'billing.event',

  // Health
  HEALTH_STATUS_CHANGED: 'health.status_changed',
  HEALTH_CRITICAL: 'health.critical',
} as const

export type EventType = typeof EVENT_TYPES[keyof typeof EVENT_TYPES]

// =============================================================================
// BASE EVENT INTERFACE
// =============================================================================

export interface BaseEvent {
  /** Unique event ID */
  eventId: string
  /** Event type from EVENT_TYPES */
  type: EventType
  /** Tenant/customer ID this event relates to */
  customerId: string
  /** When the event occurred */
  timestamp: Date
  /** Event version for schema evolution */
  version: string
  /** Optional correlation ID for tracing */
  correlationId?: string
  /** Source system that generated the event */
  source: string
}

// =============================================================================
// TENANT EVENTS
// =============================================================================

export interface TenantCreatedEvent extends BaseEvent {
  type: typeof EVENT_TYPES.TENANT_CREATED
  payload: {
    customerName: string
    tier: 'starter' | 'growth' | 'enterprise'
    primaryCrm: 'salesforce' | 'hubspot'
    createdBy: string
  }
}

export interface TenantUpdatedEvent extends BaseEvent {
  type: typeof EVENT_TYPES.TENANT_UPDATED
  payload: {
    changes: Record<string, { old: unknown; new: unknown }>
    updatedBy: string
  }
}

export interface TenantUpgradedEvent extends BaseEvent {
  type: typeof EVENT_TYPES.TENANT_UPGRADED
  payload: {
    previousTier: 'starter' | 'growth' | 'enterprise'
    newTier: 'starter' | 'growth' | 'enterprise'
    effectiveDate: Date
  }
}

// =============================================================================
// IMPLEMENTATION EVENTS
// =============================================================================

export interface ImplementationStageChangedEvent extends BaseEvent {
  type: typeof EVENT_TYPES.IMPLEMENTATION_STAGE_CHANGED
  payload: {
    previousStage: string
    newStage: string
    completionPct: number
    daysInPreviousStage: number
    triggeredBy: 'system' | 'manual'
  }
}

export interface ImplementationBlockedEvent extends BaseEvent {
  type: typeof EVENT_TYPES.IMPLEMENTATION_BLOCKED
  payload: {
    stage: string
    blockReason: string
    daysInStage: number
    suggestedAction: string
  }
}

export interface ImplementationCompletedEvent extends BaseEvent {
  type: typeof EVENT_TYPES.IMPLEMENTATION_COMPLETED
  payload: {
    totalDays: number
    goLiveDate: Date
    csOwner: string
  }
}

// =============================================================================
// DATA PIPELINE EVENTS
// =============================================================================

export interface DatasetBuiltEvent extends BaseEvent {
  type: typeof EVENT_TYPES.DATASET_BUILT
  payload: {
    datasetPath: string
    datasetRid: string
    buildRid: string
    recordCount: number
    durationSeconds: number
    status: 'success' | 'warning'
  }
}

export interface BuildFailedEvent extends BaseEvent {
  type: typeof EVENT_TYPES.BUILD_FAILED
  payload: {
    datasetPath: string
    datasetRid: string
    buildRid: string
    errorMessage: string
    errorType: string
    retryCount: number
  }
}

export interface SyncCompletedEvent extends BaseEvent {
  type: typeof EVENT_TYPES.SYNC_COMPLETED
  payload: {
    syncType: 'crm' | 'activity' | 'full'
    recordsSynced: number
    durationSeconds: number
    nextSyncAt: Date
  }
}

// =============================================================================
// AGENT EVENTS
// =============================================================================

export interface AgentActionExecutedEvent extends BaseEvent {
  type: typeof EVENT_TYPES.AGENT_ACTION_EXECUTED
  payload: {
    actionId: string
    actionType: string
    targetDataset: string
    confidence: number
    outcome: 'success' | 'failure'
    executionTimeMs: number
    wasAutomated: boolean
  }
}

// =============================================================================
// ESCALATION EVENTS
// =============================================================================

export interface EscalationCreatedEvent extends BaseEvent {
  type: typeof EVENT_TYPES.ESCALATION_CREATED
  payload: {
    escalationId: string
    reason: string
    urgency: 'low' | 'medium' | 'high' | 'critical'
    channel: 'slack' | 'email' | 'pagerduty'
    assignedTo: string
  }
}

// =============================================================================
// USAGE EVENTS
// =============================================================================

export interface UsageThresholdCrossedEvent extends BaseEvent {
  type: typeof EVENT_TYPES.USAGE_THRESHOLD_CROSSED
  payload: {
    metric: 'users' | 'opportunities' | 'api_calls' | 'compute_hours'
    currentValue: number
    limitValue: number
    utilizationPct: number
    threshold: 80 | 90 | 100
  }
}

// =============================================================================
// HEALTH EVENTS
// =============================================================================

export interface HealthStatusChangedEvent extends BaseEvent {
  type: typeof EVENT_TYPES.HEALTH_STATUS_CHANGED
  payload: {
    previousStatus: 'healthy' | 'monitor' | 'at_risk' | 'critical'
    newStatus: 'healthy' | 'monitor' | 'at_risk' | 'critical'
    compositeScore: number
    scoreChange: number
    triggeringFactor: string
  }
}

// =============================================================================
// UNION TYPE
// =============================================================================

export type RevOpsEvent =
  | TenantCreatedEvent
  | TenantUpdatedEvent
  | TenantUpgradedEvent
  | ImplementationStageChangedEvent
  | ImplementationBlockedEvent
  | ImplementationCompletedEvent
  | DatasetBuiltEvent
  | BuildFailedEvent
  | SyncCompletedEvent
  | AgentActionExecutedEvent
  | EscalationCreatedEvent
  | UsageThresholdCrossedEvent
  | HealthStatusChangedEvent

// =============================================================================
// EVENT METADATA
// =============================================================================

export interface EventMetadata {
  type: EventType
  description: string
  category: 'tenant' | 'implementation' | 'data' | 'agent' | 'usage' | 'health'
  severity: 'info' | 'warning' | 'critical'
  webhookable: boolean
}

export const EVENT_METADATA: Record<EventType, EventMetadata> = {
  [EVENT_TYPES.TENANT_CREATED]: {
    type: EVENT_TYPES.TENANT_CREATED,
    description: 'A new tenant workspace was created',
    category: 'tenant',
    severity: 'info',
    webhookable: true,
  },
  [EVENT_TYPES.TENANT_UPDATED]: {
    type: EVENT_TYPES.TENANT_UPDATED,
    description: 'Tenant configuration was updated',
    category: 'tenant',
    severity: 'info',
    webhookable: true,
  },
  [EVENT_TYPES.TENANT_DELETED]: {
    type: EVENT_TYPES.TENANT_DELETED,
    description: 'A tenant workspace was deleted',
    category: 'tenant',
    severity: 'warning',
    webhookable: true,
  },
  [EVENT_TYPES.TENANT_UPGRADED]: {
    type: EVENT_TYPES.TENANT_UPGRADED,
    description: 'Tenant tier was upgraded',
    category: 'tenant',
    severity: 'info',
    webhookable: true,
  },
  [EVENT_TYPES.TENANT_DOWNGRADED]: {
    type: EVENT_TYPES.TENANT_DOWNGRADED,
    description: 'Tenant tier was downgraded',
    category: 'tenant',
    severity: 'warning',
    webhookable: true,
  },
  [EVENT_TYPES.IMPLEMENTATION_STAGE_CHANGED]: {
    type: EVENT_TYPES.IMPLEMENTATION_STAGE_CHANGED,
    description: 'Implementation stage progressed',
    category: 'implementation',
    severity: 'info',
    webhookable: true,
  },
  [EVENT_TYPES.IMPLEMENTATION_BLOCKED]: {
    type: EVENT_TYPES.IMPLEMENTATION_BLOCKED,
    description: 'Implementation is blocked',
    category: 'implementation',
    severity: 'warning',
    webhookable: true,
  },
  [EVENT_TYPES.IMPLEMENTATION_UNBLOCKED]: {
    type: EVENT_TYPES.IMPLEMENTATION_UNBLOCKED,
    description: 'Implementation blocker resolved',
    category: 'implementation',
    severity: 'info',
    webhookable: true,
  },
  [EVENT_TYPES.IMPLEMENTATION_COMPLETED]: {
    type: EVENT_TYPES.IMPLEMENTATION_COMPLETED,
    description: 'Customer went live',
    category: 'implementation',
    severity: 'info',
    webhookable: true,
  },
  [EVENT_TYPES.DATASET_BUILT]: {
    type: EVENT_TYPES.DATASET_BUILT,
    description: 'Dataset build completed',
    category: 'data',
    severity: 'info',
    webhookable: true,
  },
  [EVENT_TYPES.BUILD_FAILED]: {
    type: EVENT_TYPES.BUILD_FAILED,
    description: 'Dataset build failed',
    category: 'data',
    severity: 'critical',
    webhookable: true,
  },
  [EVENT_TYPES.BUILD_STARTED]: {
    type: EVENT_TYPES.BUILD_STARTED,
    description: 'Dataset build started',
    category: 'data',
    severity: 'info',
    webhookable: false,
  },
  [EVENT_TYPES.SYNC_COMPLETED]: {
    type: EVENT_TYPES.SYNC_COMPLETED,
    description: 'Data sync completed',
    category: 'data',
    severity: 'info',
    webhookable: true,
  },
  [EVENT_TYPES.SYNC_FAILED]: {
    type: EVENT_TYPES.SYNC_FAILED,
    description: 'Data sync failed',
    category: 'data',
    severity: 'critical',
    webhookable: true,
  },
  [EVENT_TYPES.ALERT_TRIGGERED]: {
    type: EVENT_TYPES.ALERT_TRIGGERED,
    description: 'Alert condition triggered',
    category: 'health',
    severity: 'warning',
    webhookable: true,
  },
  [EVENT_TYPES.ALERT_RESOLVED]: {
    type: EVENT_TYPES.ALERT_RESOLVED,
    description: 'Alert condition resolved',
    category: 'health',
    severity: 'info',
    webhookable: true,
  },
  [EVENT_TYPES.AGENT_ACTION_PENDING]: {
    type: EVENT_TYPES.AGENT_ACTION_PENDING,
    description: 'Agent action awaiting approval',
    category: 'agent',
    severity: 'info',
    webhookable: true,
  },
  [EVENT_TYPES.AGENT_ACTION_EXECUTED]: {
    type: EVENT_TYPES.AGENT_ACTION_EXECUTED,
    description: 'Agent action was executed',
    category: 'agent',
    severity: 'info',
    webhookable: true,
  },
  [EVENT_TYPES.AGENT_ACTION_FAILED]: {
    type: EVENT_TYPES.AGENT_ACTION_FAILED,
    description: 'Agent action failed',
    category: 'agent',
    severity: 'warning',
    webhookable: true,
  },
  [EVENT_TYPES.ESCALATION_CREATED]: {
    type: EVENT_TYPES.ESCALATION_CREATED,
    description: 'Issue was escalated',
    category: 'agent',
    severity: 'warning',
    webhookable: true,
  },
  [EVENT_TYPES.ESCALATION_ACKNOWLEDGED]: {
    type: EVENT_TYPES.ESCALATION_ACKNOWLEDGED,
    description: 'Escalation was acknowledged',
    category: 'agent',
    severity: 'info',
    webhookable: true,
  },
  [EVENT_TYPES.ESCALATION_RESOLVED]: {
    type: EVENT_TYPES.ESCALATION_RESOLVED,
    description: 'Escalation was resolved',
    category: 'agent',
    severity: 'info',
    webhookable: true,
  },
  [EVENT_TYPES.USAGE_THRESHOLD_CROSSED]: {
    type: EVENT_TYPES.USAGE_THRESHOLD_CROSSED,
    description: 'Usage threshold was crossed',
    category: 'usage',
    severity: 'warning',
    webhookable: true,
  },
  [EVENT_TYPES.USAGE_LIMIT_REACHED]: {
    type: EVENT_TYPES.USAGE_LIMIT_REACHED,
    description: 'Usage limit was reached',
    category: 'usage',
    severity: 'critical',
    webhookable: true,
  },
  [EVENT_TYPES.BILLING_EVENT]: {
    type: EVENT_TYPES.BILLING_EVENT,
    description: 'Billing event occurred',
    category: 'usage',
    severity: 'info',
    webhookable: true,
  },
  [EVENT_TYPES.HEALTH_STATUS_CHANGED]: {
    type: EVENT_TYPES.HEALTH_STATUS_CHANGED,
    description: 'Health status changed',
    category: 'health',
    severity: 'warning',
    webhookable: true,
  },
  [EVENT_TYPES.HEALTH_CRITICAL]: {
    type: EVENT_TYPES.HEALTH_CRITICAL,
    description: 'Health became critical',
    category: 'health',
    severity: 'critical',
    webhookable: true,
  },
}
