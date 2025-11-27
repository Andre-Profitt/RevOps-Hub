/**
 * Action Logger
 * Tracks action execution and user feedback for the commercial data graph
 */

import { v4 as uuidv4 } from 'uuid'
import type {
  ActionFeedback,
  ActionLog,
  ActionStatus,
  FeedbackLabel,
} from './datasets'

// =============================================================================
// DEPENDENCIES
// =============================================================================

interface ActionLoggerDependencies {
  saveActionLog: (log: ActionLog) => Promise<void>
  saveActionFeedback: (feedback: ActionFeedback) => Promise<void>
  getActionLogs: (tenantId: string, options?: {
    actionType?: string
    status?: ActionStatus
    since?: Date
    limit?: number
  }) => Promise<ActionLog[]>
  getActionFeedback: (tenantId: string, options?: {
    actionType?: string
    rating?: number
    since?: Date
    limit?: number
  }) => Promise<ActionFeedback[]>
}

// =============================================================================
// ACTION LOGGER
// =============================================================================

export class ActionLogger {
  private deps: ActionLoggerDependencies
  private buffer: ActionLog[] = []
  private flushInterval: NodeJS.Timeout | null = null

  constructor(deps: ActionLoggerDependencies) {
    this.deps = deps
  }

  /**
   * Start background flushing
   */
  start(flushIntervalMs = 5000): void {
    if (this.flushInterval) return

    this.flushInterval = setInterval(() => {
      this.flush().catch(console.error)
    }, flushIntervalMs)
  }

  /**
   * Stop background flushing
   */
  stop(): void {
    if (this.flushInterval) {
      clearInterval(this.flushInterval)
      this.flushInterval = null
    }
  }

  /**
   * Log the start of an action execution
   */
  startAction(params: {
    tenantId: string
    userId?: string
    actionId: string
    actionType: string
    contextId?: string
    contextType?: string
    inputSummary?: string
    metadata?: Record<string, unknown>
  }): ActionLog {
    const log: ActionLog = {
      id: uuidv4(),
      tenantId: params.tenantId,
      userId: params.userId,
      actionId: params.actionId,
      actionType: params.actionType,
      contextId: params.contextId,
      contextType: params.contextType,
      inputSummary: params.inputSummary,
      metadata: params.metadata,
      status: 'pending',
      createdAt: new Date(),
    }

    return log
  }

  /**
   * Complete an action log with result
   */
  async completeAction(
    log: ActionLog,
    result: {
      status: ActionStatus
      outputSummary?: string
      errorCode?: string
      errorMessage?: string
      durationMs?: number
    }
  ): Promise<ActionLog> {
    const completedLog: ActionLog = {
      ...log,
      status: result.status,
      outputSummary: result.outputSummary,
      errorCode: result.errorCode,
      errorMessage: result.errorMessage,
      durationMs: result.durationMs,
    }

    this.buffer.push(completedLog)

    // Flush errors immediately
    if (result.status === 'error') {
      await this.flush()
    }

    return completedLog
  }

  /**
   * Execute an action with automatic logging
   */
  async executeWithLogging<T>(
    params: {
      tenantId: string
      userId?: string
      actionId: string
      actionType: string
      contextId?: string
      contextType?: string
      inputSummary?: string
      metadata?: Record<string, unknown>
    },
    action: () => Promise<T>,
    summarizeOutput?: (result: T) => string
  ): Promise<T> {
    const log = this.startAction(params)
    const startTime = Date.now()

    try {
      const result = await action()
      const durationMs = Date.now() - startTime

      await this.completeAction(log, {
        status: 'success',
        outputSummary: summarizeOutput ? summarizeOutput(result) : undefined,
        durationMs,
      })

      return result
    } catch (error) {
      const durationMs = Date.now() - startTime

      await this.completeAction(log, {
        status: 'error',
        errorCode: error instanceof Error ? error.name : 'UnknownError',
        errorMessage: error instanceof Error ? error.message : String(error),
        durationMs,
      })

      throw error
    }
  }

  /**
   * Flush buffered logs to storage
   */
  async flush(): Promise<void> {
    if (this.buffer.length === 0) return

    const logs = [...this.buffer]
    this.buffer = []

    for (const log of logs) {
      await this.deps.saveActionLog(log)
    }
  }

  /**
   * Record user feedback on an action
   */
  async recordFeedback(params: {
    tenantId: string
    userId: string
    actionId: string
    actionType: string
    contextId?: string
    contextType?: string
    rating?: number
    label?: FeedbackLabel
    comment?: string
  }): Promise<ActionFeedback> {
    const feedback: ActionFeedback = {
      id: uuidv4(),
      tenantId: params.tenantId,
      userId: params.userId,
      actionId: params.actionId,
      actionType: params.actionType,
      contextId: params.contextId,
      contextType: params.contextType,
      rating: params.rating,
      label: params.label,
      comment: params.comment,
      createdAt: new Date(),
    }

    await this.deps.saveActionFeedback(feedback)

    return feedback
  }

  /**
   * Get action logs for a tenant
   */
  async getLogs(
    tenantId: string,
    options?: {
      actionType?: string
      status?: ActionStatus
      since?: Date
      limit?: number
    }
  ): Promise<ActionLog[]> {
    return this.deps.getActionLogs(tenantId, options)
  }

  /**
   * Get feedback for a tenant
   */
  async getFeedback(
    tenantId: string,
    options?: {
      actionType?: string
      rating?: number
      since?: Date
      limit?: number
    }
  ): Promise<ActionFeedback[]> {
    return this.deps.getActionFeedback(tenantId, options)
  }

  /**
   * Calculate action success rate
   */
  async getSuccessRate(
    tenantId: string,
    actionType?: string,
    since?: Date
  ): Promise<{
    total: number
    success: number
    error: number
    timeout: number
    skipped: number
    successRate: number
  }> {
    const logs = await this.getLogs(tenantId, { actionType, since })

    const counts = {
      total: logs.length,
      success: 0,
      error: 0,
      timeout: 0,
      skipped: 0,
    }

    for (const log of logs) {
      switch (log.status) {
        case 'success':
          counts.success++
          break
        case 'error':
          counts.error++
          break
        case 'timeout':
          counts.timeout++
          break
        case 'skipped':
          counts.skipped++
          break
      }
    }

    return {
      ...counts,
      successRate: counts.total > 0 ? counts.success / counts.total : 0,
    }
  }

  /**
   * Calculate average feedback rating
   */
  async getAverageRating(
    tenantId: string,
    actionType?: string,
    since?: Date
  ): Promise<{
    total: number
    rated: number
    averageRating: number | null
    labelCounts: Record<FeedbackLabel, number>
  }> {
    const feedback = await this.getFeedback(tenantId, { actionType, since })

    const labelCounts: Record<FeedbackLabel, number> = {
      helpful: 0,
      unhelpful: 0,
      wrong: 0,
      needs_followup: 0,
    }

    let ratingSum = 0
    let ratedCount = 0

    for (const fb of feedback) {
      if (fb.rating !== undefined) {
        ratingSum += fb.rating
        ratedCount++
      }
      if (fb.label) {
        labelCounts[fb.label]++
      }
    }

    return {
      total: feedback.length,
      rated: ratedCount,
      averageRating: ratedCount > 0 ? ratingSum / ratedCount : null,
      labelCounts,
    }
  }
}

// =============================================================================
// SINGLETON
// =============================================================================

let actionLoggerInstance: ActionLogger | null = null

export function getActionLogger(deps?: ActionLoggerDependencies): ActionLogger {
  if (!actionLoggerInstance && deps) {
    actionLoggerInstance = new ActionLogger(deps)
  }
  if (!actionLoggerInstance) {
    throw new Error('ActionLogger not initialized')
  }
  return actionLoggerInstance
}

// =============================================================================
// CONVENIENCE FUNCTIONS
// =============================================================================

export async function logAction(
  params: Parameters<ActionLogger['startAction']>[0],
  result: Parameters<ActionLogger['completeAction']>[1]
): Promise<ActionLog> {
  const logger = getActionLogger()
  const log = logger.startAction(params)
  return logger.completeAction(log, result)
}

export async function recordActionFeedback(
  params: Parameters<ActionLogger['recordFeedback']>[0]
): Promise<ActionFeedback> {
  const logger = getActionLogger()
  return logger.recordFeedback(params)
}
