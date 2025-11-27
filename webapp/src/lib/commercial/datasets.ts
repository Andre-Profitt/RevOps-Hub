/**
 * Commercial Datasets
 * Canonical dataset definitions for the RevOps data graph
 *
 * These 8 datasets form the core commercial data layer:
 * 1. commercial_action_feedback - user feedback on agent/actions
 * 2. commercial_action_logs - execution logs for actions
 * 3. commercial_notification_events - audit of notifications sent
 * 4. commercial_subscription_events - audit trail for subs/invoices
 * 5. commercial_webhook_events - audit of outbound webhooks
 * 6. commercial_user_notification_prefs - per-user notification config
 * 7. commercial_webhook_configs - webhook endpoint configurations
 * 8. commercial_sync_status - status of external syncs
 */

import { v4 as uuidv4 } from 'uuid'

// =============================================================================
// 1. ACTION FEEDBACK
// =============================================================================

export type FeedbackLabel = 'helpful' | 'unhelpful' | 'wrong' | 'needs_followup'

export interface ActionFeedback {
  id: string
  tenantId: string
  userId: string
  actionId: string        // which agent/action
  actionType: string      // e.g., 'deal_insight', 'forecast_update', 'coaching_tip'
  contextId?: string      // deal/opportunity id, call id, etc.
  contextType?: string    // 'deal', 'call', 'forecast', etc.
  rating?: number         // 1-5
  label?: FeedbackLabel
  comment?: string
  createdAt: Date
}

// =============================================================================
// 2. ACTION LOGS
// =============================================================================

export type ActionStatus = 'success' | 'error' | 'timeout' | 'skipped' | 'pending'

export interface ActionLog {
  id: string
  tenantId: string
  userId?: string         // null for system-initiated actions
  actionId: string
  actionType: string
  contextId?: string
  contextType?: string
  status: ActionStatus
  durationMs?: number
  inputSummary?: string   // sanitized summary of inputs
  outputSummary?: string  // sanitized summary of outputs
  errorCode?: string
  errorMessage?: string
  metadata?: Record<string, unknown>
  createdAt: Date
}

// =============================================================================
// 3. NOTIFICATION EVENTS
// =============================================================================

export type NotificationChannel = 'email' | 'in_app' | 'slack' | 'sms' | 'webhook' | 'push'
export type NotificationStatus = 'queued' | 'sent' | 'delivered' | 'failed' | 'bounced' | 'opened'

export interface NotificationEvent {
  id: string
  tenantId: string
  userId?: string         // recipient user
  recipientEmail?: string
  recipientPhone?: string
  channel: NotificationChannel
  templateCode: string    // e.g., 'renewal_reminder_30d', 'usage_threshold_90'
  category: string        // 'billing', 'usage', 'alerts', 'cs_updates', 'system'
  subject?: string
  payloadSummary?: string // sanitized preview
  status: NotificationStatus
  errorMessage?: string
  sentAt?: Date
  deliveredAt?: Date
  openedAt?: Date
  createdAt: Date
}

// =============================================================================
// 4. SUBSCRIPTION EVENTS (re-export from billing for consistency)
// =============================================================================

// This mirrors the SubscriptionEvent from billing but in the commercial namespace
export type SubscriptionEventType =
  | 'subscription.created'
  | 'subscription.activated'
  | 'subscription.plan_changed'
  | 'subscription.discount_applied'
  | 'subscription.discount_removed'
  | 'subscription.addon_added'
  | 'subscription.addon_removed'
  | 'subscription.canceled'
  | 'subscription.reactivated'
  | 'subscription.renewed'
  | 'payment.method_added'
  | 'payment.method_removed'
  | 'payment.succeeded'
  | 'payment.failed'
  | 'invoice.created'
  | 'invoice.paid'
  | 'invoice.voided'
  | 'usage.threshold_reached'
  | 'usage.hard_cap_hit'

export interface CommercialSubscriptionEvent {
  id: string
  subscriptionId: string
  tenantId: string
  type: SubscriptionEventType
  actor: 'system' | string // userId if human
  previousValue?: unknown
  newValue?: unknown
  metadata?: Record<string, unknown>
  occurredAt: Date
}

// =============================================================================
// 5. WEBHOOK EVENTS
// =============================================================================

export type WebhookDeliveryStatus = 'pending' | 'delivered' | 'failed' | 'retrying'

export interface WebhookEvent {
  id: string
  tenantId: string
  webhookConfigId: string
  eventType: string       // e.g., 'deal.updated', 'subscription.renewed'
  eventId: string         // reference to the source event
  targetUrl: string
  status: WebhookDeliveryStatus
  httpStatus?: number
  attempt: number
  maxAttempts: number
  latencyMs?: number
  requestBodyHash?: string // for deduplication
  responseBody?: string   // truncated
  errorMessage?: string
  scheduledAt?: Date      // for retries
  deliveredAt?: Date
  createdAt: Date
}

// =============================================================================
// 6. USER NOTIFICATION PREFERENCES
// =============================================================================

export type NotificationType =
  | 'billing'
  | 'usage'
  | 'renewal'
  | 'alerts'
  | 'cs_updates'
  | 'product_updates'
  | 'weekly_digest'
  | 'deal_insights'
  | 'coaching'

export interface UserNotificationPref {
  id: string
  userId: string
  tenantId: string
  channel: NotificationChannel
  notificationType: NotificationType
  enabled: boolean
  frequency?: 'immediate' | 'daily' | 'weekly'  // for digests
  quietHoursStart?: string  // "22:00"
  quietHoursEnd?: string    // "08:00"
  timezone?: string
  createdAt: Date
  updatedAt: Date
}

// Default preferences for new users
export const DEFAULT_NOTIFICATION_PREFS: Omit<UserNotificationPref, 'id' | 'userId' | 'tenantId' | 'createdAt' | 'updatedAt'>[] = [
  { channel: 'email', notificationType: 'billing', enabled: true },
  { channel: 'email', notificationType: 'usage', enabled: true },
  { channel: 'email', notificationType: 'renewal', enabled: true },
  { channel: 'email', notificationType: 'alerts', enabled: true },
  { channel: 'in_app', notificationType: 'deal_insights', enabled: true },
  { channel: 'in_app', notificationType: 'alerts', enabled: true },
  { channel: 'email', notificationType: 'weekly_digest', enabled: true, frequency: 'weekly' },
]

// =============================================================================
// 7. WEBHOOK CONFIGURATIONS
// =============================================================================

export type WebhookStatus = 'active' | 'disabled' | 'failing' | 'suspended'

export interface WebhookConfig {
  id: string
  tenantId: string
  name: string
  description?: string
  targetUrl: string
  secret: string          // for HMAC signing
  subscribedEvents: string[]  // list of event types
  headers?: Record<string, string>  // custom headers
  status: WebhookStatus
  retryPolicy: {
    maxAttempts: number
    backoffMs: number
    backoffMultiplier: number
  }
  // Health tracking
  successCount: number
  failureCount: number
  lastSuccessAt?: Date
  lastFailureAt?: Date
  consecutiveFailures: number
  createdAt: Date
  updatedAt: Date
}

// =============================================================================
// 8. SYNC STATUS
// =============================================================================

export type SyncSystem = 'crm' | 'billing' | 'workspace' | 'pricing' | 'usage' | 'analytics'
export type SyncHealthStatus = 'ok' | 'warning' | 'error' | 'unknown'

export interface SyncStatus {
  id: string
  tenantId: string
  system: SyncSystem
  direction: 'inbound' | 'outbound' | 'bidirectional'
  lastSyncAt?: Date
  lastSuccessAt?: Date
  lastErrorAt?: Date
  status: SyncHealthStatus
  message?: string
  errorDetails?: string
  recordsSynced?: number
  recordsFailed?: number
  nextScheduledAt?: Date
  createdAt: Date
  updatedAt: Date
}

// =============================================================================
// DATASET METADATA (for manifest generation)
// =============================================================================

export interface DatasetMetadata {
  name: string
  displayName: string
  description: string
  category: 'input' | 'output' | 'analytics' | 'config'
  schema: string  // TypeScript interface name
  producers: string[]  // what writes to this
  consumers: string[]  // what reads from this
  retentionDays?: number
  partitionKey?: string
}

export const COMMERCIAL_DATASETS: DatasetMetadata[] = [
  {
    name: 'commercial_action_feedback',
    displayName: 'Action Feedback',
    description: 'User feedback on agent/action quality and usefulness',
    category: 'input',
    schema: 'ActionFeedback',
    producers: ['webapp_ui', 'sdk_api'],
    consumers: ['ai_training', 'quality_dashboard', 'ops_agent'],
    retentionDays: 365,
    partitionKey: 'tenantId',
  },
  {
    name: 'commercial_action_logs',
    displayName: 'Action Execution Logs',
    description: 'Execution logs for all agent/action invocations',
    category: 'input',
    schema: 'ActionLog',
    producers: ['agent_executor', 'action_dispatcher'],
    consumers: ['usage_tracking', 'analytics_dashboard', 'debugging'],
    retentionDays: 90,
    partitionKey: 'tenantId',
  },
  {
    name: 'commercial_notification_events',
    displayName: 'Notification Events',
    description: 'Audit trail of all notifications sent across channels',
    category: 'input',
    schema: 'NotificationEvent',
    producers: ['notification_service'],
    consumers: ['notification_analytics', 'cs_console', 'ops_agent'],
    retentionDays: 180,
    partitionKey: 'tenantId',
  },
  {
    name: 'commercial_subscription_events',
    displayName: 'Subscription Events',
    description: 'Audit trail for subscription and billing events',
    category: 'input',
    schema: 'CommercialSubscriptionEvent',
    producers: ['billing_service', 'contract_service'],
    consumers: ['revenue_analytics', 'cs_console', 'audit_reports'],
    retentionDays: 2555, // 7 years for compliance
    partitionKey: 'tenantId',
  },
  {
    name: 'commercial_webhook_events',
    displayName: 'Webhook Delivery Events',
    description: 'Audit of outbound webhook delivery attempts',
    category: 'input',
    schema: 'WebhookEvent',
    producers: ['webhook_dispatcher'],
    consumers: ['webhook_analytics', 'integration_health', 'ops_agent'],
    retentionDays: 30,
    partitionKey: 'tenantId',
  },
  {
    name: 'commercial_user_notification_prefs',
    displayName: 'User Notification Preferences',
    description: 'Per-user configuration for notification channels and types',
    category: 'config',
    schema: 'UserNotificationPref',
    producers: ['settings_ui', 'onboarding'],
    consumers: ['notification_service'],
    partitionKey: 'tenantId',
  },
  {
    name: 'commercial_webhook_configs',
    displayName: 'Webhook Configurations',
    description: 'Webhook endpoint configurations per tenant',
    category: 'config',
    schema: 'WebhookConfig',
    producers: ['settings_ui', 'api'],
    consumers: ['webhook_dispatcher', 'integration_health'],
    partitionKey: 'tenantId',
  },
  {
    name: 'commercial_sync_status',
    displayName: 'Sync Status',
    description: 'Health status of external system syncs',
    category: 'analytics',
    schema: 'SyncStatus',
    producers: ['crm_sync', 'billing_sync', 'usage_sync'],
    consumers: ['ops_dashboard', 'ops_agent', 'alerting'],
    partitionKey: 'tenantId',
  },
]

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

export function getDatasetByName(name: string): DatasetMetadata | undefined {
  return COMMERCIAL_DATASETS.find(d => d.name === name)
}

export function getDatasetsByCategory(category: DatasetMetadata['category']): DatasetMetadata[] {
  return COMMERCIAL_DATASETS.filter(d => d.category === category)
}

export function getInputDatasets(): DatasetMetadata[] {
  return getDatasetsByCategory('input')
}

export function getConfigDatasets(): DatasetMetadata[] {
  return getDatasetsByCategory('config')
}
