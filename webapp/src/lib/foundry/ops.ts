/**
 * Operations & Commercial data fetchers
 * System health, build metrics, usage metering, and license status
 */

import { USE_MOCK_DATA, executeFoundrySQL } from './core'

// ============================================================================
// SYSTEM HEALTH
// ============================================================================

export async function getSystemHealth(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        customer_id as customerId,
        health_score as healthScore,
        health_status as healthStatus,
        build_success_rate as buildSuccessRate,
        sync_success_rate as syncSuccessRate,
        avg_data_quality as avgDataQuality,
        active_alert_count as activeAlertCount,
        critical_alerts as criticalAlerts,
        warning_alerts as warningAlerts,
        sync_age_hours as syncAgeHours,
        snapshot_time as snapshotTime
      FROM \`/RevOps/Ops/system_health\`
      ORDER BY health_score ASC
    `,
  })
}

// ============================================================================
// BUILD PERFORMANCE
// ============================================================================

export async function getBuildPerformance(timeWindow: string = '24h'): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        customer_id as customerId,
        dataset_path as datasetPath,
        time_window as timeWindow,
        build_count as buildCount,
        success_count as successCount,
        failure_count as failureCount,
        success_rate as successRate,
        avg_duration_seconds as avgDurationSeconds,
        p50_duration as p50Duration,
        p95_duration as p95Duration,
        p99_duration as p99Duration,
        snapshot_time as snapshotTime
      FROM \`/RevOps/Ops/build_performance\`
      WHERE time_window = '${timeWindow}'
      ORDER BY failure_count DESC, avg_duration_seconds DESC
    `,
  })
}

export async function getBuildSummary(): Promise<any> {
  if (USE_MOCK_DATA) {
    return null
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        total_builds as totalBuilds,
        successful_builds as successfulBuilds,
        failed_builds as failedBuilds,
        success_rate as successRate,
        avg_duration_seconds as avgDurationSeconds,
        total_compute_hours as totalComputeHours,
        snapshot_time as snapshotTime
      FROM \`/RevOps/Monitoring/build_summary\`
      ORDER BY snapshot_time DESC
      LIMIT 1
    `,
  })

  return results[0] || null
}

// ============================================================================
// DATA FRESHNESS
// ============================================================================

export async function getDataFreshness(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        customer_id as customerId,
        dataset_path as datasetPath,
        last_successful_build as lastSuccessfulBuild,
        hours_since_build as hoursSinceBuild,
        days_since_build as daysSinceBuild,
        freshness_status as freshnessStatus,
        expected_freshness_hours as expectedFreshnessHours,
        is_overdue as isOverdue
      FROM \`/RevOps/Ops/data_freshness\`
      WHERE is_overdue = true
      ORDER BY hours_since_build DESC
    `,
  })
}

export async function getStaleDatasets(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        dataset_path as datasetPath,
        last_build_time as lastBuildTime,
        hours_stale as hoursStale,
        expected_refresh_hours as expectedRefreshHours,
        severity,
        owner_team as ownerTeam
      FROM \`/RevOps/Monitoring/stale_datasets\`
      ORDER BY hours_stale DESC
    `,
  })
}

// ============================================================================
// SLA TRACKING
// ============================================================================

export async function getSlaTracking(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        customer_id as customerId,
        build_success_rate as buildSuccessRate,
        build_sla_target as buildSlaTarget,
        build_sla_met as buildSlaMet,
        freshness_rate as freshnessRate,
        freshness_sla_target as freshnessSlaTarget,
        freshness_sla_met as freshnessSlaMet,
        overall_sla_met as overallSlaMet,
        sla_status as slaStatus,
        snapshot_time as snapshotTime
      FROM \`/RevOps/Ops/sla_tracking\`
      ORDER BY overall_sla_met ASC, build_success_rate ASC
    `,
  })
}

// ============================================================================
// USAGE METRICS
// ============================================================================

export async function getUsageMetrics(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        customer_id as customerId,
        subscription_tier as subscriptionTier,
        builds_30d as builds30d,
        datasets_built as datasetsBuilt,
        total_compute_seconds as totalComputeSeconds,
        avg_compute_minutes_per_day as avgComputeMinutesPerDay,
        active_days as activeDays,
        activity_score as activityScore,
        user_limit as userLimit,
        opportunity_limit as opportunityLimit,
        snapshot_time as snapshotTime
      FROM \`/RevOps/Ops/usage_metrics\`
      ORDER BY activity_score DESC
    `,
  })
}

// ============================================================================
// LICENSE STATUS
// ============================================================================

export async function getLicenseStatus(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        customer_id as customerId,
        tier,
        billing_period as billingPeriod,
        license_status as licenseStatus,
        peak_active_users as peakActiveUsers,
        limit_users as limitUsers,
        user_utilization as userUtilization,
        peak_opportunities as peakOpportunities,
        limit_opportunities as limitOpportunities,
        opportunity_utilization as opportunityUtilization,
        total_api_calls as totalApiCalls,
        limit_api_calls as limitApiCalls,
        api_utilization as apiUtilization,
        total_compute_hours as totalComputeHours,
        limit_compute_hours as limitComputeHours,
        compute_utilization as computeUtilization,
        has_overage as hasOverage,
        upgrade_recommended as upgradeRecommended,
        recommended_tier as recommendedTier,
        compliance_message as complianceMessage,
        checked_at as checkedAt
      FROM \`/RevOps/Commercial/license_status\`
      ORDER BY has_overage DESC, user_utilization DESC
    `,
  })
}

// ============================================================================
// AIOPS TELEMETRY - Pipeline Events & Ops Agent Queue
// ============================================================================

export async function getPipelineEvents(limit: number = 100): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        event_id as eventId,
        event_ts as eventTs,
        run_id as runId,
        dataset_path as datasetPath,
        domain,
        event_type as eventType,
        severity,
        message,
        details,
        requires_action as requiresAction,
        suggested_action as suggestedAction,
        priority_score as priorityScore,
        action_status as actionStatus
      FROM \`/RevOps/Monitoring/pipeline_events\`
      ORDER BY event_ts DESC
      LIMIT ${limit}
    `,
  })
}

export async function getPipelineEventsByType(eventType: string): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        event_id as eventId,
        event_ts as eventTs,
        dataset_path as datasetPath,
        domain,
        event_type as eventType,
        severity,
        message,
        details,
        suggested_action as suggestedAction,
        priority_score as priorityScore
      FROM \`/RevOps/Monitoring/pipeline_events\`
      WHERE event_type = '${eventType}'
      ORDER BY event_ts DESC
      LIMIT 50
    `,
  })
}

export async function getOpsAgentQueue(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        event_id as eventId,
        event_ts as eventTs,
        dataset_path as datasetPath,
        domain,
        event_type as eventType,
        severity,
        message,
        details,
        suggested_action as suggestedAction,
        priority_score as priorityScore,
        action_status as actionStatus,
        queue_position as queuePosition,
        estimated_resolution_min as estimatedResolutionMin
      FROM \`/RevOps/Monitoring/ops_agent_queue\`
      WHERE action_status = 'pending'
      ORDER BY queue_position ASC
    `,
  })
}

export async function getOpsAgentQueueSummary(): Promise<any> {
  if (USE_MOCK_DATA) {
    return null
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        COUNT(*) as totalItems,
        SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) as criticalCount,
        SUM(CASE WHEN severity = 'warning' THEN 1 ELSE 0 END) as warningCount,
        SUM(estimated_resolution_min) as totalEstimatedMinutes,
        MAX(priority_score) as maxPriority
      FROM \`/RevOps/Monitoring/ops_agent_queue\`
      WHERE action_status = 'pending'
    `,
  })

  return results[0] || null
}

// ============================================================================
// ENHANCED BUILD METRICS WITH TELEMETRY
// ============================================================================

export async function getBuildMetricsWithTelemetry(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        dataset_path as datasetPath,
        domain,
        last_build_ts as lastBuildTs,
        record_count as recordCount,
        status,
        schema_snapshot as schemaSnapshot,
        read_latency_ms as readLatencyMs,
        error_details as errorDetails,
        column_count as columnCount,
        sample_null_pct as sampleNullPct,
        is_healthy as isHealthy,
        is_slow as isSlow,
        has_quality_issues as hasQualityIssues,
        run_id as runId
      FROM \`/RevOps/Monitoring/build_metrics\`
      ORDER BY is_healthy ASC, read_latency_ms DESC
    `,
  })
}

export async function getBuildMetricsByDomain(domain: string): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        dataset_path as datasetPath,
        last_build_ts as lastBuildTs,
        record_count as recordCount,
        status,
        read_latency_ms as readLatencyMs,
        column_count as columnCount,
        sample_null_pct as sampleNullPct,
        is_healthy as isHealthy,
        is_slow as isSlow
      FROM \`/RevOps/Monitoring/build_metrics\`
      WHERE domain = '${domain}'
      ORDER BY dataset_path
    `,
  })
}

export async function getBuildSummaryByDomain(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        domain,
        total_datasets as totalDatasets,
        healthy_datasets as healthyDatasets,
        empty_datasets as emptyDatasets,
        error_datasets as errorDatasets,
        total_records as totalRecords,
        latest_build as latestBuild,
        avg_latency_ms as avgLatencyMs,
        max_latency_ms as maxLatencyMs,
        slow_datasets as slowDatasets,
        avg_null_pct as avgNullPct,
        quality_issue_datasets as qualityIssueDatasets,
        total_columns as totalColumns,
        health_pct as healthPct,
        performance_score as performanceScore,
        quality_score as qualityScore,
        latest_run_id as latestRunId
      FROM \`/RevOps/Monitoring/build_summary\`
      ORDER BY health_pct ASC
    `,
  })
}

export async function getDatasetHealthOverview(): Promise<any> {
  if (USE_MOCK_DATA) {
    return null
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        COUNT(*) as totalDatasets,
        SUM(CASE WHEN is_healthy THEN 1 ELSE 0 END) as healthyDatasets,
        SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as errorDatasets,
        SUM(CASE WHEN is_slow THEN 1 ELSE 0 END) as slowDatasets,
        SUM(CASE WHEN has_quality_issues THEN 1 ELSE 0 END) as qualityIssues,
        AVG(read_latency_ms) as avgLatencyMs,
        AVG(sample_null_pct) as avgNullPct,
        SUM(record_count) as totalRecords
      FROM \`/RevOps/Monitoring/build_metrics\`
    `,
  })

  return results[0] || null
}

// ============================================================================
// OPS AGENT - Decisions, Actions, and Escalations
// ============================================================================

export async function getAgentDecisions(limit: number = 50): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        decision_id as decisionId,
        event_id as eventId,
        dataset_path as datasetPath,
        suggested_action as suggestedAction,
        confidence,
        decision,
        decision_reason as decisionReason,
        decision_ts as decisionTs,
        requires_human_review as requiresHumanReview,
        escalation_channel as escalationChannel
      FROM \`/RevOps/Monitoring/agent_decisions\`
      ORDER BY decision_ts DESC
      LIMIT ${limit}
    `,
  })
}

export async function getAgentActionLog(limit: number = 50): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        action_id as actionId,
        event_id as eventId,
        dataset_path as datasetPath,
        action_type as actionType,
        confidence,
        initiated_ts as initiatedTs,
        outcome,
        completed_ts as completedTs,
        outcome_details as outcomeDetails,
        agent_reasoning as agentReasoning
      FROM \`/RevOps/Monitoring/agent_action_log\`
      ORDER BY initiated_ts DESC
      LIMIT ${limit}
    `,
  })
}

export async function getEscalationQueue(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        escalation_id as escalationId,
        event_id as eventId,
        dataset_path as datasetPath,
        suggested_action as suggestedAction,
        confidence,
        decision,
        decision_reason as decisionReason,
        escalation_channel as escalationChannel,
        escalation_ts as escalationTs,
        escalation_status as escalationStatus,
        assigned_to as assignedTo,
        acknowledged_ts as acknowledgedTs,
        resolved_ts as resolvedTs,
        urgency
      FROM \`/RevOps/Monitoring/escalation_queue\`
      WHERE escalation_status IN ('pending', 'acknowledged')
      ORDER BY urgency DESC, escalation_ts ASC
    `,
  })
}

export async function getEscalationTracker(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        escalation_id as escalationId,
        dataset_path as datasetPath,
        suggested_action as suggestedAction,
        escalation_status as escalationStatus,
        urgency_level as urgencyLevel,
        hours_since_escalation as hoursSinceEscalation,
        is_overdue as isOverdue,
        channel,
        recipient,
        delivery_status as deliveryStatus
      FROM \`/RevOps/Monitoring/escalation_tracker\`
      ORDER BY is_overdue DESC, hours_since_escalation DESC
    `,
  })
}

export async function getAgentPerformance(): Promise<any> {
  if (USE_MOCK_DATA) {
    return null
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        metric_ts as metricTs,
        total_actions as totalActions,
        successful_actions as successfulActions,
        failed_actions as failedActions,
        success_rate as successRate,
        total_escalations as totalEscalations,
        resolved_escalations as resolvedEscalations,
        escalation_rate as escalationRate,
        avg_confidence as avgConfidence,
        agent_health_score as agentHealthScore
      FROM \`/RevOps/Monitoring/agent_performance\`
      ORDER BY metric_ts DESC
      LIMIT 1
    `,
  })

  return results[0] || null
}

export async function getAgentStatus(): Promise<any> {
  if (USE_MOCK_DATA) {
    return null
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        agent_status as agentStatus,
        agent_health_score as agentHealthScore,
        queue_depth as queueDepth,
        critical_items as criticalItems,
        total_estimated_minutes as totalEstimatedMinutes,
        decisions_24h as decisions24h,
        auto_executed_24h as autoExecuted24h,
        escalated_24h as escalated24h,
        avg_confidence_24h as avgConfidence24h,
        status_ts as statusTs
      FROM \`/RevOps/Monitoring/agent_status_dashboard\`
      ORDER BY status_ts DESC
      LIMIT 1
    `,
  })

  return results[0] || null
}

export async function getActionExecutionResults(limit: number = 50): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        execution_id as executionId,
        decision_id as decisionId,
        event_id as eventId,
        dataset_path as datasetPath,
        action_type as actionType,
        execution_ts as executionTs,
        execution_status as executionStatus,
        execution_result as executionResult,
        error_message as errorMessage,
        api_response as apiResponse
      FROM \`/RevOps/Monitoring/action_execution_results\`
      ORDER BY execution_ts DESC
      LIMIT ${limit}
    `,
  })
}

export async function getNotificationLog(limit: number = 50): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        notification_id as notificationId,
        escalation_id as escalationId,
        channel,
        sent_ts as sentTs,
        delivery_status as deliveryStatus,
        message_content as messageContent,
        recipient
      FROM \`/RevOps/Monitoring/notification_log\`
      ORDER BY sent_ts DESC
      LIMIT ${limit}
    `,
  })
}

export async function getActionFeedback(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        execution_id as executionId,
        event_id as eventId,
        dataset_path as datasetPath,
        action_type as actionType,
        execution_ts as executionTs,
        execution_status as executionStatus,
        current_status as currentStatus,
        current_is_healthy as currentIsHealthy,
        latest_build_ts as latestBuildTs,
        action_effective as actionEffective,
        feedback_ts as feedbackTs
      FROM \`/RevOps/Monitoring/action_feedback\`
      ORDER BY feedback_ts DESC
      LIMIT 100
    `,
  })
}

// ============================================================================
// API EXECUTION & CONFIG AGENT
// ============================================================================

export async function getApiExecutionSummary(): Promise<any> {
  if (USE_MOCK_DATA) {
    return null
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        summary_ts as summaryTs,
        build_api_requests as buildApiRequests,
        jira_tickets_created as jiraTicketsCreated,
        slack_messages_sent as slackMessagesSent,
        pagerduty_incidents as pagerdutyIncidents,
        total_api_calls as totalApiCalls
      FROM \`/RevOps/Monitoring/api_execution_summary\`
      ORDER BY summary_ts DESC
      LIMIT 1
    `,
  })

  return results[0] || null
}

export async function getConfigAgentStatus(): Promise<any> {
  if (USE_MOCK_DATA) {
    return null
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        status_ts as statusTs,
        agent_status as agentStatus,
        schema_drift_alerts as schemaDriftAlerts,
        config_recommendations as configRecommendations,
        model_drift_detected as modelDriftDetected,
        total_issues as totalIssues
      FROM \`/RevOps/Monitoring/config_agent_status\`
      ORDER BY status_ts DESC
      LIMIT 1
    `,
  })

  return results[0] || null
}

export async function getModelExplanations(limit: number = 50): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        explanation_id as explanationId,
        opportunity_id as opportunityId,
        prediction,
        top_positive_factors as topPositiveFactors,
        top_negative_factors as topNegativeFactors,
        explanation_text as explanationText,
        generated_ts as generatedTs
      FROM \`/RevOps/Monitoring/model_explanations\`
      ORDER BY generated_ts DESC
      LIMIT ${limit}
    `,
  })
}

// ============================================================================
// SELF-SERVICE DASHBOARD
// ============================================================================

export async function getSelfServiceDashboard(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        customer_id as customerId,
        dashboard_ts as dashboardTs,
        onboarding_complete as onboardingComplete,
        onboarding_pct as onboardingPct,
        next_wizard_step as nextWizardStep,
        active_recommendations as activeRecommendations,
        integration_opportunities as integrationOpportunities,
        config_status as configStatus,
        overall_health as overallHealth
      FROM \`/RevOps/SelfService/self_service_dashboard\`
      ORDER BY dashboard_ts DESC
    `,
  })
}

// ============================================================================
// IMPLEMENTATION STATUS & CUSTOMER MANAGEMENT
// ============================================================================

export interface CustomerImplementation {
  customerId: string
  customerName: string
  tier: string
  currentStage: string
  stageOrder: number
  completionPct: number
  nextStage: string | null
  daysInStage: number
  isBlocked: boolean
  onboardedAt: string
  workspaceCreated: boolean
  adminUserAdded: boolean
  crmConnected: boolean
  firstSyncComplete: boolean
  lastSyncAt: string | null
  pipelineDashboardEnabled: boolean
  hygieneDashboardEnabled: boolean
  dashboardsEnabledCount: number
  opsAgentEnabled: boolean
  agentFirstAction: boolean
  totalAgentActions: number
  lastAgentActionAt: string | null
  goLiveApproved: boolean
  goLiveDate: string | null
  csHandoffComplete: boolean
  csOwner: string | null
  evaluatedAt: string
}

export async function getCustomerImplementationStatus(): Promise<CustomerImplementation[]> {
  if (USE_MOCK_DATA) {
    return getMockCustomerImplementations()
  }

  return executeFoundrySQL<CustomerImplementation>({
    query: `
      SELECT
        customer_id as customerId,
        customer_name as customerName,
        tier,
        current_stage as currentStage,
        stage_order as stageOrder,
        completion_pct as completionPct,
        next_stage as nextStage,
        days_in_stage as daysInStage,
        is_blocked as isBlocked,
        onboarded_at as onboardedAt,
        workspace_created as workspaceCreated,
        admin_user_added as adminUserAdded,
        crm_connected as crmConnected,
        first_sync_complete as firstSyncComplete,
        last_sync_at as lastSyncAt,
        pipeline_dashboard_enabled as pipelineDashboardEnabled,
        hygiene_dashboard_enabled as hygieneDashboardEnabled,
        dashboards_enabled_count as dashboardsEnabledCount,
        ops_agent_enabled as opsAgentEnabled,
        agent_first_action as agentFirstAction,
        total_agent_actions as totalAgentActions,
        last_agent_action_at as lastAgentActionAt,
        go_live_approved as goLiveApproved,
        go_live_date as goLiveDate,
        cs_handoff_complete as csHandoffComplete,
        cs_owner as csOwner,
        evaluated_at as evaluatedAt
      FROM \`/RevOps/Commercial/implementation_status\`
      ORDER BY stage_order ASC, days_in_stage DESC
    `,
  })
}

export async function getImplementationSummary(): Promise<any> {
  if (USE_MOCK_DATA) {
    const customers = getMockCustomerImplementations()
    return {
      totalCustomers: customers.length,
      liveCustomers: customers.filter(c => c.currentStage === 'live').length,
      blockedCustomers: customers.filter(c => c.isBlocked).length,
      avgCompletionPct: customers.reduce((sum, c) => sum + c.completionPct, 0) / customers.length,
    }
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        generated_at as generatedAt,
        total_customers as totalCustomers,
        live_customers as liveCustomers,
        blocked_customers as blockedCustomers,
        avg_completion_pct as avgCompletionPct,
        live_pct as livePct,
        blocked_pct as blockedPct,
        stage_breakdown as stageBreakdown
      FROM \`/RevOps/Commercial/implementation_summary\`
      ORDER BY generated_at DESC
      LIMIT 1
    `,
  })

  return results[0] || null
}

// ============================================================================
// TENANT HEALTH METRICS
// ============================================================================

export interface TenantHealth {
  customerId: string
  customerName: string
  tier: string
  compositeScore: number
  healthStatus: string
  dataFreshnessScore: number
  adoptionScore: number
  aiAccuracyScore: number
  buildHealthScore: number
  hoursSinceSync: number
  activeDays: number
  avgDailyUsers: number
  acceptanceRate: number
  buildSuccessRate: number
  csRecommendation: string
  scoredAt: string
}

export async function getTenantHealthScores(): Promise<TenantHealth[]> {
  if (USE_MOCK_DATA) {
    return getMockTenantHealth()
  }

  return executeFoundrySQL<TenantHealth>({
    query: `
      SELECT
        customer_id as customerId,
        customer_name as customerName,
        tier,
        composite_score as compositeScore,
        health_status as healthStatus,
        data_freshness_score as dataFreshnessScore,
        adoption_score as adoptionScore,
        ai_accuracy_score as aiAccuracyScore,
        build_health_score as buildHealthScore,
        hours_since_sync as hoursSinceSync,
        active_days as activeDays,
        avg_daily_users as avgDailyUsers,
        acceptance_rate as acceptanceRate,
        build_success_rate as buildSuccessRate,
        cs_recommendation as csRecommendation,
        scored_at as scoredAt
      FROM \`/RevOps/Commercial/tenant_health_scores\`
      ORDER BY composite_score ASC
    `,
  })
}

export async function getTenantHealthTrends(customerId: string): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        customer_id as customerId,
        week_number as weekNumber,
        year,
        snapshot_date as snapshotDate,
        composite_score as compositeScore,
        data_freshness_score as dataFreshnessScore,
        adoption_score as adoptionScore,
        ai_accuracy_score as aiAccuracyScore,
        build_health_score as buildHealthScore,
        health_status as healthStatus,
        score_change as scoreChange,
        trend_direction as trendDirection
      FROM \`/RevOps/Commercial/tenant_health_trends\`
      WHERE customer_id = '${customerId}'
      ORDER BY year DESC, week_number DESC
      LIMIT 12
    `,
  })
}

// ============================================================================
// TENANT ROI METRICS
// ============================================================================

export interface TenantROI {
  customerId: string
  customerName: string
  tier: string
  daysLive: number
  baselineWinRate: number
  currentWinRate: number
  winRateImprovementPct: number
  baselineAvgCycleDays: number
  currentAvgCycleDays: number
  cycleTimeReductionDays: number
  cycleTimeReductionPct: number
  forecastAccuracyImprovementPct: number
  hygieneImprovementPct: number
  estimatedRevenueImpact: number
  roiStatus: string
  calculatedAt: string
}

export async function getTenantROIMetrics(): Promise<TenantROI[]> {
  if (USE_MOCK_DATA) {
    return getMockTenantROI()
  }

  return executeFoundrySQL<TenantROI>({
    query: `
      SELECT
        customer_id as customerId,
        customer_name as customerName,
        tier,
        days_live as daysLive,
        baseline_win_rate as baselineWinRate,
        current_win_rate as currentWinRate,
        win_rate_improvement_pct as winRateImprovementPct,
        baseline_avg_cycle_days as baselineAvgCycleDays,
        current_avg_cycle_days as currentAvgCycleDays,
        cycle_time_reduction_days as cycleTimeReductionDays,
        cycle_time_reduction_pct as cycleTimeReductionPct,
        forecast_accuracy_improvement_pct as forecastAccuracyImprovementPct,
        hygiene_improvement_pct as hygieneImprovementPct,
        estimated_revenue_impact as estimatedRevenueImpact,
        roi_status as roiStatus,
        calculated_at as calculatedAt
      FROM \`/RevOps/Commercial/tenant_roi_summary\`
      ORDER BY estimated_revenue_impact DESC
    `,
  })
}

// ============================================================================
// BILLING & PLANS
// ============================================================================

export async function getTenantPlans(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        customer_id as customerId,
        customer_name as customerName,
        plan_id as planId,
        plan_name as planName,
        billing_cycle as billingCycle,
        subscription_status as subscriptionStatus,
        started_at as startedAt,
        current_period_start as currentPeriodStart,
        current_period_end as currentPeriodEnd,
        next_billing_date as nextBillingDate,
        mrr,
        arr,
        is_trial as isTrial,
        trial_ends_at as trialEndsAt,
        cancel_at_period_end as cancelAtPeriodEnd
      FROM \`/RevOps/Commercial/tenant_plans\`
      ORDER BY mrr DESC
    `,
  })
}

export async function getBillingSummary(): Promise<any> {
  if (USE_MOCK_DATA) {
    return null
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT
        summary_date as summaryDate,
        total_customers as totalCustomers,
        total_mrr as totalMrr,
        total_arr as totalArr,
        customers_by_plan as customersByPlan,
        mrr_by_plan as mrrByPlan,
        trial_customers as trialCustomers,
        churning_customers as churningCustomers
      FROM \`/RevOps/Commercial/billing_summary\`
      ORDER BY summary_date DESC
      LIMIT 1
    `,
  })

  return results[0] || null
}

// ============================================================================
// MOCK DATA FOR DEVELOPMENT
// ============================================================================

function getMockCustomerImplementations(): CustomerImplementation[] {
  return [
    {
      customerId: 'acme-corp',
      customerName: 'Acme Corporation',
      tier: 'enterprise',
      currentStage: 'live',
      stageOrder: 5,
      completionPct: 100,
      nextStage: null,
      daysInStage: 45,
      isBlocked: false,
      onboardedAt: '2024-09-15T00:00:00Z',
      workspaceCreated: true,
      adminUserAdded: true,
      crmConnected: true,
      firstSyncComplete: true,
      lastSyncAt: '2024-11-27T10:00:00Z',
      pipelineDashboardEnabled: true,
      hygieneDashboardEnabled: true,
      dashboardsEnabledCount: 5,
      opsAgentEnabled: true,
      agentFirstAction: true,
      totalAgentActions: 127,
      lastAgentActionAt: '2024-11-27T09:45:00Z',
      goLiveApproved: true,
      goLiveDate: '2024-10-12T00:00:00Z',
      csHandoffComplete: true,
      csOwner: 'Sarah Johnson',
      evaluatedAt: '2024-11-27T10:30:00Z',
    },
    {
      customerId: 'techstart-io',
      customerName: 'TechStart.io',
      tier: 'growth',
      currentStage: 'agent_training',
      stageOrder: 4,
      completionPct: 80,
      nextStage: 'live',
      daysInStage: 7,
      isBlocked: false,
      onboardedAt: '2024-11-01T00:00:00Z',
      workspaceCreated: true,
      adminUserAdded: true,
      crmConnected: true,
      firstSyncComplete: true,
      lastSyncAt: '2024-11-27T08:00:00Z',
      pipelineDashboardEnabled: true,
      hygieneDashboardEnabled: true,
      dashboardsEnabledCount: 3,
      opsAgentEnabled: true,
      agentFirstAction: true,
      totalAgentActions: 15,
      lastAgentActionAt: '2024-11-26T16:00:00Z',
      goLiveApproved: false,
      goLiveDate: null,
      csHandoffComplete: false,
      csOwner: 'Mike Chen',
      evaluatedAt: '2024-11-27T10:30:00Z',
    },
    {
      customerId: 'global-retail',
      customerName: 'Global Retail Inc',
      tier: 'enterprise',
      currentStage: 'dashboard_config',
      stageOrder: 3,
      completionPct: 60,
      nextStage: 'agent_training',
      daysInStage: 18,
      isBlocked: true,
      onboardedAt: '2024-10-20T00:00:00Z',
      workspaceCreated: true,
      adminUserAdded: true,
      crmConnected: true,
      firstSyncComplete: true,
      lastSyncAt: '2024-11-25T12:00:00Z',
      pipelineDashboardEnabled: true,
      hygieneDashboardEnabled: false,
      dashboardsEnabledCount: 1,
      opsAgentEnabled: false,
      agentFirstAction: false,
      totalAgentActions: 0,
      lastAgentActionAt: null,
      goLiveApproved: false,
      goLiveDate: null,
      csHandoffComplete: false,
      csOwner: 'Sarah Johnson',
      evaluatedAt: '2024-11-27T10:30:00Z',
    },
    {
      customerId: 'startup-xyz',
      customerName: 'Startup XYZ',
      tier: 'starter',
      currentStage: 'connector_setup',
      stageOrder: 2,
      completionPct: 40,
      nextStage: 'dashboard_config',
      daysInStage: 5,
      isBlocked: false,
      onboardedAt: '2024-11-20T00:00:00Z',
      workspaceCreated: true,
      adminUserAdded: true,
      crmConnected: true,
      firstSyncComplete: false,
      lastSyncAt: null,
      pipelineDashboardEnabled: false,
      hygieneDashboardEnabled: false,
      dashboardsEnabledCount: 0,
      opsAgentEnabled: false,
      agentFirstAction: false,
      totalAgentActions: 0,
      lastAgentActionAt: null,
      goLiveApproved: false,
      goLiveDate: null,
      csHandoffComplete: false,
      csOwner: null,
      evaluatedAt: '2024-11-27T10:30:00Z',
    },
    {
      customerId: 'newco-fresh',
      customerName: 'NewCo Fresh',
      tier: 'growth',
      currentStage: 'onboarding',
      stageOrder: 1,
      completionPct: 20,
      nextStage: 'connector_setup',
      daysInStage: 2,
      isBlocked: false,
      onboardedAt: '2024-11-25T00:00:00Z',
      workspaceCreated: true,
      adminUserAdded: true,
      crmConnected: false,
      firstSyncComplete: false,
      lastSyncAt: null,
      pipelineDashboardEnabled: false,
      hygieneDashboardEnabled: false,
      dashboardsEnabledCount: 0,
      opsAgentEnabled: false,
      agentFirstAction: false,
      totalAgentActions: 0,
      lastAgentActionAt: null,
      goLiveApproved: false,
      goLiveDate: null,
      csHandoffComplete: false,
      csOwner: 'Mike Chen',
      evaluatedAt: '2024-11-27T10:30:00Z',
    },
  ]
}

function getMockTenantHealth(): TenantHealth[] {
  return [
    {
      customerId: 'acme-corp',
      customerName: 'Acme Corporation',
      tier: 'enterprise',
      compositeScore: 92,
      healthStatus: 'healthy',
      dataFreshnessScore: 95,
      adoptionScore: 88,
      aiAccuracyScore: 92,
      buildHealthScore: 98,
      hoursSinceSync: 2,
      activeDays: 7,
      avgDailyUsers: 45,
      acceptanceRate: 0.85,
      buildSuccessRate: 0.99,
      csRecommendation: 'Healthy engagement. Quarterly check-in sufficient.',
      scoredAt: '2024-11-27T10:30:00Z',
    },
    {
      customerId: 'techstart-io',
      customerName: 'TechStart.io',
      tier: 'growth',
      compositeScore: 78,
      healthStatus: 'monitor',
      dataFreshnessScore: 85,
      adoptionScore: 72,
      aiAccuracyScore: 75,
      buildHealthScore: 95,
      hoursSinceSync: 4,
      activeDays: 5,
      avgDailyUsers: 12,
      acceptanceRate: 0.70,
      buildSuccessRate: 0.95,
      csRecommendation: 'Monitor weekly. Consider training refresh.',
      scoredAt: '2024-11-27T10:30:00Z',
    },
    {
      customerId: 'global-retail',
      customerName: 'Global Retail Inc',
      tier: 'enterprise',
      compositeScore: 55,
      healthStatus: 'at_risk',
      dataFreshnessScore: 40,
      adoptionScore: 45,
      aiAccuracyScore: 60,
      buildHealthScore: 85,
      hoursSinceSync: 48,
      activeDays: 2,
      avgDailyUsers: 8,
      acceptanceRate: 0.50,
      buildSuccessRate: 0.90,
      csRecommendation: 'Proactive outreach recommended. Check adoption blockers.',
      scoredAt: '2024-11-27T10:30:00Z',
    },
  ]
}

function getMockTenantROI(): TenantROI[] {
  return [
    {
      customerId: 'acme-corp',
      customerName: 'Acme Corporation',
      tier: 'enterprise',
      daysLive: 45,
      baselineWinRate: 0.22,
      currentWinRate: 0.28,
      winRateImprovementPct: 27.3,
      baselineAvgCycleDays: 52,
      currentAvgCycleDays: 41,
      cycleTimeReductionDays: 11,
      cycleTimeReductionPct: 21.2,
      forecastAccuracyImprovementPct: 15,
      hygieneImprovementPct: 35,
      estimatedRevenueImpact: 450000,
      roiStatus: 'strong_roi',
      calculatedAt: '2024-11-27T10:30:00Z',
    },
    {
      customerId: 'techstart-io',
      customerName: 'TechStart.io',
      tier: 'growth',
      daysLive: 0,
      baselineWinRate: 0.18,
      currentWinRate: 0.18,
      winRateImprovementPct: 0,
      baselineAvgCycleDays: 35,
      currentAvgCycleDays: 35,
      cycleTimeReductionDays: 0,
      cycleTimeReductionPct: 0,
      forecastAccuracyImprovementPct: 0,
      hygieneImprovementPct: 0,
      estimatedRevenueImpact: 0,
      roiStatus: 'ramping',
      calculatedAt: '2024-11-27T10:30:00Z',
    },
  ]
}
