/**
 * Foundry API Integration Layer
 *
 * This module provides functions to connect the React app to Foundry data.
 * In development, it returns mock data. In production, it calls the secure
 * API route which proxies to Foundry (keeping credentials server-side).
 *
 * Mock data is lazy-loaded only when USE_MOCK_DATA is true to avoid
 * bundling it into production builds.
 *
 * Domain modules:
 * - core: Shared utilities, types, and configuration
 * - pipeline: Pipeline Health Dashboard data
 * - coaching: Rep Coaching data
 * - hygiene: Pipeline Hygiene data
 * - predictions: AI Predictions and Leading Indicators
 * - telemetry: Cross-Functional Telemetry data
 * - qbr: Quarterly Business Review data
 * - territory: Territory Planning data
 * - capacity: Capacity Planning data
 * - scenarios: Scenario Modeling data
 * - customers: Customer Health data
 * - forecasting: Forecasting Hub data
 * - winloss: Win/Loss Analysis data
 * - dealdesk: Deal Desk data
 * - compensation: Compensation & Attainment data
 * - data-quality: Data Quality Admin data
 * - alerts: Alerts & Notifications data
 */

// Core utilities and types
export {
  USE_MOCK_DATA,
  getDataSource,
  getDataSourceAsync,
  checkFoundryConnection,
  type DashboardFilters,
} from './core'

// Pipeline Health Dashboard
export {
  getDashboardKPIs,
  getForecastTrend,
  getStuckDeals,
  getHealthDistribution,
  getProcessBottlenecks,
  getCompetitiveLosses,
  getStageVelocity,
  getPipelineFunnel,
  getPipelineSummary,
  getReworkAnalysis,
} from './pipeline'

// Rep Coaching
export {
  getRepPerformance,
  getDriverComparisons,
  getActivityMetrics,
  getNextBestActions,
  getCoachingInsights,
  getSalesReps,
  getTopPerformerPatterns,
  getRepRecommendations,
} from './coaching'

// Pipeline Hygiene
export {
  getHygieneAlerts,
  getHygieneSummary,
  getHygieneByOwner,
  getHygieneTrends,
  getHygieneAlertFeed,
} from './hygiene'

// AI Predictions
export {
  getDealPredictions,
  getLeadingIndicatorsSummary,
  getNextBestActionsDashboard,
  getAccountRecommendations,
} from './predictions'

// Cross-Functional Telemetry
export {
  getTelemetrySummary,
  getFunnelHandoffs,
  getCrossTeamActivity,
  getSupportHealthByAccount,
  getUnifiedTimeline,
  getMarketingActivities,
} from './telemetry'

// QBR
export {
  getQBRExecutiveSummary,
  getQBRRepPerformance,
  getQBRWinLossAnalysis,
} from './qbr'

// Territory Planning
export {
  getTerritorySummary,
  getAccountScores,
  getTerritoryBalance,
  getWhiteSpaceAccounts,
} from './territory'

// Capacity Planning
export {
  getCapacityPlanningSummary,
  getRepCapacity,
  getTeamCapacitySummary,
  getRampCohortAnalysis,
} from './capacity'

// Scenario Modeling
export {
  getScenarioSummary,
  getWinRateScenarios,
  getDealSizeScenarios,
  getCycleTimeScenarios,
  getScenarioBaseMetrics,
} from './scenarios'

// Customer Health
export {
  getCustomerHealthSummary,
  getCustomerHealth,
  getExpansionOpportunities,
  getChurnRiskAccounts,
  getChurnCohorts,
} from './customers'

// Forecasting Hub
export {
  getForecastSummary,
  getForecastBySegment,
  getForecastHistory,
  getForecastAccuracy,
  getCompanyForecast,
  getManagerForecast,
  getRegionForecast,
  getSwingDeals,
} from './forecasting'

// Win/Loss Analysis
export {
  getWinLossSummary,
  getLossReasons,
  getWinFactors,
  getCompetitiveBattles,
  getWinLossBySegment,
} from './winloss'

// Deal Desk
export {
  getDealDeskSummary,
  getDealApprovals,
  getDiscountAnalysis,
} from './dealdesk'

// Compensation & Attainment
export {
  getCompPlanSummary,
  getRepAttainment,
  getAttainmentTrend,
} from './compensation'

// Data Quality Admin
export {
  getDataQualitySummary,
  getDataQualityMetrics,
  getSyncStatus,
} from './data-quality'

// Alerts & Notifications
export {
  getAlertSummary,
  getAlerts,
  getAlertRules,
  getMonitoringAlertSummary,
  getAlertHistory,
} from './alerts'

// Operations & Commercial
export {
  getSystemHealth,
  getBuildPerformance,
  getBuildSummary,
  getDataFreshness,
  getStaleDatasets,
  getSlaTracking,
  getUsageMetrics,
  getLicenseStatus,
  // AIOps Telemetry
  getPipelineEvents,
  getPipelineEventsByType,
  getOpsAgentQueue,
  getOpsAgentQueueSummary,
  getBuildMetricsWithTelemetry,
  getBuildMetricsByDomain,
  getBuildSummaryByDomain,
  getDatasetHealthOverview,
  // Ops Agent
  getAgentDecisions,
  getAgentActionLog,
  getEscalationQueue,
  getEscalationTracker,
  getAgentPerformance,
  getAgentStatus,
  getActionExecutionResults,
  getNotificationLog,
  getActionFeedback,
  // API Execution & Config Agent
  getApiExecutionSummary,
  getConfigAgentStatus,
  getModelExplanations,
  // Self-Service
  getSelfServiceDashboard,
  // Implementation & Customer Management
  getCustomerImplementationStatus,
  getImplementationSummary,
  getTenantHealthScores,
  getTenantHealthTrends,
  getTenantROIMetrics,
  getTenantPlans,
  getBillingSummary,
  // Types
  type CustomerImplementation,
  type TenantHealth,
  type TenantROI,
} from './ops'

// Staging & Sample Data
export {
  getHubspotOpportunities,
  getHubspotAccounts,
  getHubspotContacts,
  getHubspotActivities,
  getSampleAccounts,
  getSampleActivities,
  getScenarioAccounts,
  getScenarioActivities,
} from './staging'
