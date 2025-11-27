/**
 * Authentication and Authorization Types
 *
 * Defines the types for user roles, permissions, and session management.
 */

// =============================================================================
// ROLES
// =============================================================================

export type UserRole =
  | 'platform_admin'
  | 'customer_admin'
  | 'revops_manager'
  | 'revops_analyst'
  | 'sales_manager'
  | 'sales_rep'
  | 'support_engineer';

export const ROLE_HIERARCHY: Record<UserRole, number> = {
  platform_admin: 100,
  customer_admin: 90,
  support_engineer: 85,
  revops_manager: 70,
  revops_analyst: 60,
  sales_manager: 50,
  sales_rep: 40,
};

// =============================================================================
// PERMISSIONS
// =============================================================================

export type Permission =
  | 'view_all_customers'
  | 'manage_customer_config'
  | 'manage_users'
  | 'configure_thresholds'
  | 'configure_alerts'
  | 'view_all_analytics'
  | 'view_team_analytics'
  | 'view_own_analytics'
  | 'export_data'
  | 'trigger_builds'
  | 'view_build_logs'
  | 'access_api';

export const ROLE_PERMISSIONS: Record<UserRole, Permission[]> = {
  platform_admin: [
    'view_all_customers',
    'manage_customer_config',
    'manage_users',
    'configure_thresholds',
    'configure_alerts',
    'view_all_analytics',
    'view_team_analytics',
    'view_own_analytics',
    'export_data',
    'trigger_builds',
    'view_build_logs',
    'access_api',
  ],
  customer_admin: [
    'manage_customer_config',
    'manage_users',
    'configure_thresholds',
    'configure_alerts',
    'view_all_analytics',
    'view_team_analytics',
    'view_own_analytics',
    'export_data',
    'trigger_builds',
    'view_build_logs',
    'access_api',
  ],
  revops_manager: [
    'configure_thresholds',
    'configure_alerts',
    'view_all_analytics',
    'view_team_analytics',
    'view_own_analytics',
    'trigger_builds',
    'view_build_logs',
    'access_api',
  ],
  revops_analyst: [
    'view_all_analytics',
    'view_team_analytics',
    'view_own_analytics',
  ],
  sales_manager: [
    'view_team_analytics',
    'view_own_analytics',
  ],
  sales_rep: [
    'view_own_analytics',
  ],
  support_engineer: [
    'view_all_customers',
    'view_all_analytics',
    'view_team_analytics',
    'view_own_analytics',
    'view_build_logs',
  ],
};

// =============================================================================
// FEATURES
// =============================================================================

export type Feature =
  | 'pipeline_health'
  | 'pipeline_hygiene'
  | 'forecast_summary'
  | 'deal_predictions'
  | 'coaching_insights'
  | 'rep_performance'
  | 'activity_analytics'
  | 'scenario_modeling'
  | 'territory_planning'
  | 'custom_integrations'
  | 'api_access'
  | 'sso_enabled'
  | 'white_labeling'
  | 'data_export';

export type Tier = 'starter' | 'growth' | 'enterprise';

export const TIER_FEATURES: Record<Tier, Feature[]> = {
  starter: [
    'pipeline_health',
    'pipeline_hygiene',
    'forecast_summary',
  ],
  growth: [
    'pipeline_health',
    'pipeline_hygiene',
    'forecast_summary',
    'deal_predictions',
    'coaching_insights',
    'rep_performance',
    'activity_analytics',
  ],
  enterprise: [
    'pipeline_health',
    'pipeline_hygiene',
    'forecast_summary',
    'deal_predictions',
    'coaching_insights',
    'rep_performance',
    'activity_analytics',
    'scenario_modeling',
    'territory_planning',
    'custom_integrations',
    'api_access',
    'sso_enabled',
    'white_labeling',
    'data_export',
  ],
};

// =============================================================================
// USER & SESSION
// =============================================================================

export interface User {
  id: string;
  email: string;
  name: string;
  role: UserRole;
  customerId: string;
  customerName: string;
  tier: Tier;
  teamMembers?: string[]; // For managers
  managerId?: string;
  avatarUrl?: string;
}

export interface Session {
  user: User;
  accessToken: string;
  refreshToken?: string;
  expiresAt: number;
}

export interface AuthState {
  isAuthenticated: boolean;
  isLoading: boolean;
  user: User | null;
  error: string | null;
}

// =============================================================================
// ROW-LEVEL SECURITY
// =============================================================================

export interface RLSContext {
  userId: string;
  role: UserRole;
  customerId: string;
  teamMembers: string[];
}

export type RLSPolicy = 'none' | 'team' | 'self';

export const ROLE_RLS_POLICY: Record<UserRole, RLSPolicy> = {
  platform_admin: 'none',
  customer_admin: 'none',
  revops_manager: 'none',
  revops_analyst: 'none',
  sales_manager: 'team',
  sales_rep: 'self',
  support_engineer: 'none',
};
