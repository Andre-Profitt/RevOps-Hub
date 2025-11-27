/**
 * Commercial Module
 * Canonical datasets and services for the RevOps data graph
 */

// =============================================================================
// DATASET TYPES
// =============================================================================

export type {
  // Action feedback/logs
  ActionFeedback,
  ActionLog,
  ActionStatus,
  FeedbackLabel,

  // Notification events
  NotificationEvent,
  NotificationChannel,
  NotificationStatus,
  NotificationType,
  UserNotificationPref,

  // Subscription events
  CommercialSubscriptionEvent,
  SubscriptionEventType,

  // Webhook events
  WebhookEvent,
  WebhookDeliveryStatus,
  WebhookConfig,
  WebhookStatus,

  // Sync status
  SyncStatus,
  SyncSystem,
  SyncHealthStatus,

  // Dataset metadata
  DatasetMetadata,
} from './datasets'

export {
  COMMERCIAL_DATASETS,
  DEFAULT_NOTIFICATION_PREFS,
  getDatasetByName,
  getDatasetsByCategory,
  getInputDatasets,
  getConfigDatasets,
} from './datasets'

// =============================================================================
// ACTION LOGGER
// =============================================================================

export {
  ActionLogger,
  getActionLogger,
  logAction,
  recordActionFeedback,
} from './action-logger'

// =============================================================================
// NOTIFICATION SERVICE
// =============================================================================

export {
  NotificationService,
  getNotificationService,
} from './notification-service'

// =============================================================================
// WEBHOOK DISPATCHER
// =============================================================================

export {
  WebhookDispatcher,
  getWebhookDispatcher,
} from './webhook-dispatcher'

// =============================================================================
// SYNC TRACKER
// =============================================================================

export {
  SyncTracker,
  getSyncTracker,
  withSyncTracking,
} from './sync-tracker'

// =============================================================================
// ANALYTICS VIEWS
// =============================================================================

export type {
  NotificationAnalyticsView,
  WebhookAnalyticsView,
  IntegrationHealthView,
  CommercialOpsDashboardView,
} from './analytics-views'

export {
  AnalyticsViewGenerator,
  getAnalyticsViewGenerator,
} from './analytics-views'
