/**
 * Event Bus for RevOps Command Center
 * Provides pub/sub functionality for real-time event processing
 */

import { v4 as uuidv4 } from 'uuid'
import type { EventType, RevOpsEvent, BaseEvent } from './types'
import { EVENT_METADATA } from './types'

// =============================================================================
// TYPES
// =============================================================================

export type EventHandler<T extends BaseEvent = BaseEvent> = (event: T) => void | Promise<void>

export interface EventSubscription {
  id: string
  eventType: EventType | '*'
  handler: EventHandler
  filter?: (event: BaseEvent) => boolean
}

export interface EventBusOptions {
  /** Enable async processing */
  async?: boolean
  /** Max retries for failed handlers */
  maxRetries?: number
  /** Enable debug logging */
  debug?: boolean
}

// =============================================================================
// EVENT BUS CLASS
// =============================================================================

class EventBus {
  private subscriptions: Map<string, EventSubscription> = new Map()
  private eventHistory: BaseEvent[] = []
  private historyLimit = 1000
  private options: EventBusOptions

  constructor(options: EventBusOptions = {}) {
    this.options = {
      async: true,
      maxRetries: 3,
      debug: false,
      ...options,
    }
  }

  /**
   * Subscribe to events
   */
  subscribe<T extends BaseEvent>(
    eventType: EventType | '*',
    handler: EventHandler<T>,
    filter?: (event: T) => boolean
  ): string {
    const id = uuidv4()
    const subscription: EventSubscription = {
      id,
      eventType,
      handler: handler as EventHandler,
      filter: filter as ((event: BaseEvent) => boolean) | undefined,
    }
    this.subscriptions.set(id, subscription)

    if (this.options.debug) {
      console.log(`[EventBus] Subscribed to ${eventType} (id: ${id})`)
    }

    return id
  }

  /**
   * Unsubscribe from events
   */
  unsubscribe(subscriptionId: string): boolean {
    const removed = this.subscriptions.delete(subscriptionId)
    if (this.options.debug && removed) {
      console.log(`[EventBus] Unsubscribed (id: ${subscriptionId})`)
    }
    return removed
  }

  /**
   * Emit an event
   */
  async emit<T extends BaseEvent>(event: T): Promise<void> {
    // Ensure event has required fields
    const enrichedEvent: T = {
      ...event,
      eventId: event.eventId || uuidv4(),
      timestamp: event.timestamp || new Date(),
      version: event.version || '1.0',
      source: event.source || 'revops-hub',
    }

    // Add to history
    this.addToHistory(enrichedEvent)

    if (this.options.debug) {
      console.log(`[EventBus] Emitting ${enrichedEvent.type}`, enrichedEvent)
    }

    // Get matching subscriptions
    const matchingSubscriptions = Array.from(this.subscriptions.values()).filter(
      (sub) => sub.eventType === '*' || sub.eventType === enrichedEvent.type
    )

    // Execute handlers
    const handlers = matchingSubscriptions
      .filter((sub) => !sub.filter || sub.filter(enrichedEvent))
      .map((sub) => this.executeHandler(sub.handler, enrichedEvent))

    if (this.options.async) {
      // Fire and forget, but log errors
      Promise.all(handlers).catch((error) => {
        console.error('[EventBus] Handler error:', error)
      })
    } else {
      await Promise.all(handlers)
    }
  }

  /**
   * Execute a handler with retries
   */
  private async executeHandler(
    handler: EventHandler,
    event: BaseEvent,
    attempt = 1
  ): Promise<void> {
    try {
      await handler(event)
    } catch (error) {
      if (attempt < (this.options.maxRetries || 3)) {
        if (this.options.debug) {
          console.log(`[EventBus] Retry ${attempt} for ${event.type}`)
        }
        // Exponential backoff
        await new Promise((resolve) => setTimeout(resolve, Math.pow(2, attempt) * 100))
        return this.executeHandler(handler, event, attempt + 1)
      }
      console.error(`[EventBus] Handler failed after ${attempt} attempts:`, error)
      throw error
    }
  }

  /**
   * Add event to history
   */
  private addToHistory(event: BaseEvent): void {
    this.eventHistory.unshift(event)
    if (this.eventHistory.length > this.historyLimit) {
      this.eventHistory = this.eventHistory.slice(0, this.historyLimit)
    }
  }

  /**
   * Get recent events
   */
  getHistory(options?: {
    type?: EventType
    customerId?: string
    limit?: number
    since?: Date
  }): BaseEvent[] {
    let events = [...this.eventHistory]

    if (options?.type) {
      events = events.filter((e) => e.type === options.type)
    }
    if (options?.customerId) {
      events = events.filter((e) => e.customerId === options.customerId)
    }
    if (options?.since) {
      events = events.filter((e) => e.timestamp >= options.since!)
    }
    if (options?.limit) {
      events = events.slice(0, options.limit)
    }

    return events
  }

  /**
   * Get subscription count
   */
  getSubscriptionCount(): number {
    return this.subscriptions.size
  }

  /**
   * Clear all subscriptions
   */
  clearSubscriptions(): void {
    this.subscriptions.clear()
  }

  /**
   * Clear event history
   */
  clearHistory(): void {
    this.eventHistory = []
  }
}

// =============================================================================
// SINGLETON INSTANCE
// =============================================================================

let eventBusInstance: EventBus | null = null

export function getEventBus(options?: EventBusOptions): EventBus {
  if (!eventBusInstance) {
    eventBusInstance = new EventBus(options)
  }
  return eventBusInstance
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Create a typed event
 */
export function createEvent<T extends BaseEvent>(
  type: T['type'],
  customerId: string,
  payload: Omit<T, keyof BaseEvent>
): T {
  return {
    eventId: uuidv4(),
    type,
    customerId,
    timestamp: new Date(),
    version: '1.0',
    source: 'revops-hub',
    ...payload,
  } as T
}

/**
 * Check if event type is webhookable
 */
export function isWebhookable(eventType: EventType): boolean {
  return EVENT_METADATA[eventType]?.webhookable ?? false
}

/**
 * Get events by category
 */
export function getEventsByCategory(
  category: 'tenant' | 'implementation' | 'data' | 'agent' | 'usage' | 'health'
): EventType[] {
  return Object.values(EVENT_METADATA)
    .filter((meta) => meta.category === category)
    .map((meta) => meta.type)
}

// =============================================================================
// EXPORTS
// =============================================================================

export { EventBus }
export default getEventBus
