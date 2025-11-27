/**
 * Notification Service
 * Handles notification delivery and user preferences
 */

import { v4 as uuidv4 } from 'uuid'
import type {
  NotificationEvent,
  NotificationChannel,
  NotificationStatus,
  NotificationType,
  UserNotificationPref,
  DEFAULT_NOTIFICATION_PREFS,
} from './datasets'
import { getEventBus } from '../events/bus'

// =============================================================================
// DEPENDENCIES
// =============================================================================

interface NotificationServiceDependencies {
  // Event storage
  saveNotificationEvent: (event: NotificationEvent) => Promise<void>
  getNotificationEvents: (tenantId: string, options?: {
    channel?: NotificationChannel
    status?: NotificationStatus
    since?: Date
    limit?: number
  }) => Promise<NotificationEvent[]>

  // User preferences
  getUserPrefs: (userId: string, tenantId: string) => Promise<UserNotificationPref[]>
  saveUserPref: (pref: UserNotificationPref) => Promise<void>
  getUsersByTenant: (tenantId: string) => Promise<{ id: string; email: string; role: string }[]>

  // Delivery providers
  sendEmail: (to: string, subject: string, body: string, templateCode?: string) => Promise<{ messageId: string }>
  sendSlack: (channel: string, message: string) => Promise<{ ts: string }>
  sendInApp: (userId: string, notification: { title: string; body: string; link?: string }) => Promise<void>
  sendSms: (phone: string, message: string) => Promise<{ messageId: string }>
  sendPush: (userId: string, notification: { title: string; body: string }) => Promise<void>
}

// =============================================================================
// NOTIFICATION SERVICE
// =============================================================================

export class NotificationService {
  private deps: NotificationServiceDependencies

  constructor(deps: NotificationServiceDependencies) {
    this.deps = deps
  }

  /**
   * Send a notification respecting user preferences
   */
  async send(params: {
    tenantId: string
    userId?: string
    recipientEmail?: string
    recipientPhone?: string
    channel: NotificationChannel
    category: NotificationType | string
    templateCode: string
    subject?: string
    body: string
    link?: string
    metadata?: Record<string, unknown>
  }): Promise<NotificationEvent> {
    // Check user preferences if userId provided
    if (params.userId) {
      const shouldSend = await this.checkUserPreference(
        params.userId,
        params.tenantId,
        params.channel,
        params.category as NotificationType
      )

      if (!shouldSend) {
        // Log as skipped but don't send
        return this.recordEvent({
          ...params,
          status: 'failed',
          errorMessage: 'User has disabled this notification type',
        })
      }
    }

    // Create initial event
    const event = await this.recordEvent({
      ...params,
      status: 'queued',
    })

    // Attempt delivery
    try {
      await this.deliver(event, params)

      // Update to sent
      event.status = 'sent'
      event.sentAt = new Date()
      await this.deps.saveNotificationEvent(event)

      // Emit event for analytics
      const eventBus = getEventBus()
      eventBus.emit({
        eventId: uuidv4(),
        type: 'health.alert_triggered',
        customerId: params.tenantId,
        timestamp: new Date(),
        data: {
          alertType: 'notification_sent',
          channel: params.channel,
          templateCode: params.templateCode,
        },
      })

      return event
    } catch (error) {
      event.status = 'failed'
      event.errorMessage = error instanceof Error ? error.message : String(error)
      await this.deps.saveNotificationEvent(event)
      throw error
    }
  }

  /**
   * Record a notification event
   */
  private async recordEvent(params: {
    tenantId: string
    userId?: string
    recipientEmail?: string
    recipientPhone?: string
    channel: NotificationChannel
    category: string
    templateCode: string
    subject?: string
    body?: string
    status: NotificationStatus
    errorMessage?: string
  }): Promise<NotificationEvent> {
    const event: NotificationEvent = {
      id: uuidv4(),
      tenantId: params.tenantId,
      userId: params.userId,
      recipientEmail: params.recipientEmail,
      recipientPhone: params.recipientPhone,
      channel: params.channel,
      templateCode: params.templateCode,
      category: params.category,
      subject: params.subject,
      payloadSummary: params.body?.substring(0, 200),
      status: params.status,
      errorMessage: params.errorMessage,
      createdAt: new Date(),
    }

    await this.deps.saveNotificationEvent(event)
    return event
  }

  /**
   * Deliver notification via appropriate channel
   */
  private async deliver(
    event: NotificationEvent,
    params: {
      channel: NotificationChannel
      recipientEmail?: string
      recipientPhone?: string
      userId?: string
      subject?: string
      body: string
      link?: string
      templateCode: string
    }
  ): Promise<void> {
    switch (params.channel) {
      case 'email':
        if (!params.recipientEmail) {
          throw new Error('Email recipient required')
        }
        await this.deps.sendEmail(
          params.recipientEmail,
          params.subject ?? 'Notification',
          params.body,
          params.templateCode
        )
        break

      case 'slack':
        await this.deps.sendSlack('#notifications', params.body)
        break

      case 'in_app':
        if (!params.userId) {
          throw new Error('User ID required for in-app notifications')
        }
        await this.deps.sendInApp(params.userId, {
          title: params.subject ?? 'Notification',
          body: params.body,
          link: params.link,
        })
        break

      case 'sms':
        if (!params.recipientPhone) {
          throw new Error('Phone number required for SMS')
        }
        await this.deps.sendSms(params.recipientPhone, params.body)
        break

      case 'push':
        if (!params.userId) {
          throw new Error('User ID required for push notifications')
        }
        await this.deps.sendPush(params.userId, {
          title: params.subject ?? 'Notification',
          body: params.body,
        })
        break

      default:
        throw new Error(`Unsupported channel: ${params.channel}`)
    }
  }

  /**
   * Check if user has enabled this notification type
   */
  async checkUserPreference(
    userId: string,
    tenantId: string,
    channel: NotificationChannel,
    notificationType: NotificationType
  ): Promise<boolean> {
    const prefs = await this.deps.getUserPrefs(userId, tenantId)

    const pref = prefs.find(
      p => p.channel === channel && p.notificationType === notificationType
    )

    // Default to enabled if no preference set
    return pref?.enabled ?? true
  }

  /**
   * Update user notification preference
   */
  async updatePreference(params: {
    userId: string
    tenantId: string
    channel: NotificationChannel
    notificationType: NotificationType
    enabled: boolean
    frequency?: 'immediate' | 'daily' | 'weekly'
    quietHoursStart?: string
    quietHoursEnd?: string
    timezone?: string
  }): Promise<UserNotificationPref> {
    const existingPrefs = await this.deps.getUserPrefs(params.userId, params.tenantId)
    const existing = existingPrefs.find(
      p => p.channel === params.channel && p.notificationType === params.notificationType
    )

    const pref: UserNotificationPref = {
      id: existing?.id ?? uuidv4(),
      userId: params.userId,
      tenantId: params.tenantId,
      channel: params.channel,
      notificationType: params.notificationType,
      enabled: params.enabled,
      frequency: params.frequency,
      quietHoursStart: params.quietHoursStart,
      quietHoursEnd: params.quietHoursEnd,
      timezone: params.timezone,
      createdAt: existing?.createdAt ?? new Date(),
      updatedAt: new Date(),
    }

    await this.deps.saveUserPref(pref)
    return pref
  }

  /**
   * Get all preferences for a user
   */
  async getUserPreferences(
    userId: string,
    tenantId: string
  ): Promise<UserNotificationPref[]> {
    return this.deps.getUserPrefs(userId, tenantId)
  }

  /**
   * Broadcast notification to all users in a tenant
   */
  async broadcast(params: {
    tenantId: string
    channel: NotificationChannel
    category: NotificationType | string
    templateCode: string
    subject?: string
    body: string
    roles?: string[]  // filter by role
  }): Promise<NotificationEvent[]> {
    const users = await this.deps.getUsersByTenant(params.tenantId)
    const filteredUsers = params.roles
      ? users.filter(u => params.roles!.includes(u.role))
      : users

    const events: NotificationEvent[] = []

    for (const user of filteredUsers) {
      try {
        const event = await this.send({
          tenantId: params.tenantId,
          userId: user.id,
          recipientEmail: user.email,
          channel: params.channel,
          category: params.category,
          templateCode: params.templateCode,
          subject: params.subject,
          body: params.body,
        })
        events.push(event)
      } catch (error) {
        console.error(`Failed to send notification to user ${user.id}:`, error)
      }
    }

    return events
  }

  /**
   * Get notification events for analytics
   */
  async getEvents(
    tenantId: string,
    options?: {
      channel?: NotificationChannel
      status?: NotificationStatus
      since?: Date
      limit?: number
    }
  ): Promise<NotificationEvent[]> {
    return this.deps.getNotificationEvents(tenantId, options)
  }

  /**
   * Get notification statistics
   */
  async getStats(
    tenantId: string,
    since?: Date
  ): Promise<{
    total: number
    byChannel: Record<NotificationChannel, number>
    byStatus: Record<NotificationStatus, number>
    deliveryRate: number
  }> {
    const events = await this.getEvents(tenantId, { since })

    const byChannel: Record<string, number> = {}
    const byStatus: Record<string, number> = {}

    let delivered = 0

    for (const event of events) {
      byChannel[event.channel] = (byChannel[event.channel] ?? 0) + 1
      byStatus[event.status] = (byStatus[event.status] ?? 0) + 1

      if (event.status === 'delivered' || event.status === 'opened') {
        delivered++
      }
    }

    return {
      total: events.length,
      byChannel: byChannel as Record<NotificationChannel, number>,
      byStatus: byStatus as Record<NotificationStatus, number>,
      deliveryRate: events.length > 0 ? delivered / events.length : 0,
    }
  }

  /**
   * Mark notification as delivered (for tracking)
   */
  async markDelivered(eventId: string): Promise<void> {
    // This would update the event in storage
    // Implementation depends on storage layer
  }

  /**
   * Mark notification as opened (for tracking)
   */
  async markOpened(eventId: string): Promise<void> {
    // This would update the event in storage
    // Implementation depends on storage layer
  }
}

// =============================================================================
// SINGLETON
// =============================================================================

let notificationServiceInstance: NotificationService | null = null

export function getNotificationService(
  deps?: NotificationServiceDependencies
): NotificationService {
  if (!notificationServiceInstance && deps) {
    notificationServiceInstance = new NotificationService(deps)
  }
  if (!notificationServiceInstance) {
    throw new Error('NotificationService not initialized')
  }
  return notificationServiceInstance
}
