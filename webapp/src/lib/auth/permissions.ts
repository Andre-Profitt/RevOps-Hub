/**
 * Permission and Role Enforcement Utilities
 *
 * Functions for checking user permissions, roles, and feature access.
 */

import {
  UserRole,
  Permission,
  Feature,
  Tier,
  User,
  RLSContext,
  RLSPolicy,
  ROLE_HIERARCHY,
  ROLE_PERMISSIONS,
  TIER_FEATURES,
  ROLE_RLS_POLICY,
} from './types';

// =============================================================================
// ROLE CHECKS
// =============================================================================

/**
 * Check if a user has a specific role.
 */
export function hasRole(user: User | null, role: UserRole): boolean {
  if (!user) return false;
  return user.role === role;
}

/**
 * Check if a user has a role at or above the specified level.
 */
export function hasMinRole(user: User | null, minRole: UserRole): boolean {
  if (!user) return false;
  return ROLE_HIERARCHY[user.role] >= ROLE_HIERARCHY[minRole];
}

/**
 * Check if user role is in a list of allowed roles.
 */
export function hasAnyRole(user: User | null, roles: UserRole[]): boolean {
  if (!user) return false;
  return roles.includes(user.role);
}

/**
 * Get the user's role level (for comparisons).
 */
export function getRoleLevel(role: UserRole): number {
  return ROLE_HIERARCHY[role] ?? 0;
}

// =============================================================================
// PERMISSION CHECKS
// =============================================================================

/**
 * Check if a user has a specific permission.
 */
export function hasPermission(user: User | null, permission: Permission): boolean {
  if (!user) return false;
  const permissions = ROLE_PERMISSIONS[user.role] ?? [];
  return permissions.includes(permission);
}

/**
 * Check if a user has all specified permissions.
 */
export function hasAllPermissions(user: User | null, permissions: Permission[]): boolean {
  if (!user) return false;
  return permissions.every(p => hasPermission(user, p));
}

/**
 * Check if a user has any of the specified permissions.
 */
export function hasAnyPermission(user: User | null, permissions: Permission[]): boolean {
  if (!user) return false;
  return permissions.some(p => hasPermission(user, p));
}

/**
 * Get all permissions for a user.
 */
export function getUserPermissions(user: User | null): Permission[] {
  if (!user) return [];
  return ROLE_PERMISSIONS[user.role] ?? [];
}

// =============================================================================
// FEATURE CHECKS
// =============================================================================

/**
 * Check if a tier has access to a feature.
 */
export function tierHasFeature(tier: Tier, feature: Feature): boolean {
  const features = TIER_FEATURES[tier] ?? [];
  return features.includes(feature);
}

/**
 * Check if a user has access to a feature (based on their tier).
 */
export function hasFeature(user: User | null, feature: Feature): boolean {
  if (!user) return false;
  return tierHasFeature(user.tier, feature);
}

/**
 * Get all features available to a tier.
 */
export function getTierFeatures(tier: Tier): Feature[] {
  return TIER_FEATURES[tier] ?? [];
}

/**
 * Check if user can access a specific page/dashboard.
 */
export function canAccessPage(user: User | null, page: string): boolean {
  if (!user) return false;

  // Map pages to required features
  const pageFeatureMap: Record<string, Feature | null> = {
    '/': null, // Home - always accessible
    '/forecast': 'forecast_summary',
    '/coaching': 'coaching_insights',
    '/scenarios': 'scenario_modeling',
    '/territory': 'territory_planning',
    '/compensation': 'rep_performance',
    '/data-quality': null, // Core feature
    '/alerts': null, // Core feature
    '/dealdesk': 'deal_predictions',
    '/customers': 'activity_analytics',
    '/winloss': 'deal_predictions',
    '/qbr': 'coaching_insights',
    '/capacity': 'territory_planning',
  };

  const requiredFeature = pageFeatureMap[page];

  // No feature requirement - check role permissions
  if (requiredFeature === null) {
    // All authenticated users can access core pages
    return true;
  }

  // Undefined page - admin only
  if (requiredFeature === undefined) {
    return hasMinRole(user, 'revops_manager');
  }

  // Check feature and role
  return hasFeature(user, requiredFeature) && hasPermission(user, 'view_own_analytics');
}

// =============================================================================
// ROW-LEVEL SECURITY
// =============================================================================

/**
 * Get the RLS policy for a user's role.
 */
export function getRLSPolicy(user: User | null): RLSPolicy {
  if (!user) return 'self';
  return ROLE_RLS_POLICY[user.role] ?? 'self';
}

/**
 * Build RLS context for a user.
 */
export function buildRLSContext(user: User): RLSContext {
  return {
    userId: user.id,
    role: user.role,
    customerId: user.customerId,
    teamMembers: user.teamMembers ?? [],
  };
}

/**
 * Filter data based on RLS policy.
 * Returns a predicate function that can be used with Array.filter.
 */
export function createRLSFilter<T extends { owner_id?: string; ownerId?: string }>(
  user: User
): (item: T) => boolean {
  const policy = getRLSPolicy(user);
  const teamMembers = user.teamMembers ?? [];

  switch (policy) {
    case 'none':
      // No filtering - user sees all data
      return () => true;

    case 'team':
      // User sees their data and their team's data
      return (item: T) => {
        const ownerId = item.owner_id ?? item.ownerId;
        return ownerId === user.id || teamMembers.includes(ownerId ?? '');
      };

    case 'self':
      // User sees only their own data
      return (item: T) => {
        const ownerId = item.owner_id ?? item.ownerId;
        return ownerId === user.id;
      };

    default:
      return () => false;
  }
}

/**
 * Apply RLS filter to an array of data.
 */
export function applyRLS<T extends { owner_id?: string; ownerId?: string }>(
  data: T[],
  user: User
): T[] {
  const filter = createRLSFilter<T>(user);
  return data.filter(filter);
}

// =============================================================================
// ACCESS CONTROL HELPERS
// =============================================================================

/**
 * Check if user can view another user's data.
 */
export function canViewUserData(viewer: User | null, targetUserId: string): boolean {
  if (!viewer) return false;

  // User can always view their own data
  if (viewer.id === targetUserId) return true;

  // Admins and analysts can view all
  if (hasMinRole(viewer, 'revops_analyst')) return true;

  // Managers can view their team
  if (viewer.role === 'sales_manager') {
    return viewer.teamMembers?.includes(targetUserId) ?? false;
  }

  return false;
}

/**
 * Check if user can modify configuration.
 */
export function canModifyConfig(user: User | null): boolean {
  return hasPermission(user, 'manage_customer_config');
}

/**
 * Check if user can export data.
 */
export function canExportData(user: User | null): boolean {
  if (!user) return false;
  return hasPermission(user, 'export_data') && hasFeature(user, 'data_export');
}

/**
 * Check if user can trigger data builds.
 */
export function canTriggerBuild(user: User | null): boolean {
  return hasPermission(user, 'trigger_builds');
}

// =============================================================================
// UI HELPERS
// =============================================================================

/**
 * Get navigation items filtered by user permissions.
 */
export interface NavItem {
  name: string;
  href: string;
  icon?: string;
  feature?: Feature;
  minRole?: UserRole;
}

export function filterNavItems(items: NavItem[], user: User | null): NavItem[] {
  if (!user) return [];

  return items.filter(item => {
    // Check feature requirement
    if (item.feature && !hasFeature(user, item.feature)) {
      return false;
    }

    // Check role requirement
    if (item.minRole && !hasMinRole(user, item.minRole)) {
      return false;
    }

    return true;
  });
}

/**
 * Get display text for a role.
 */
export function getRoleDisplayName(role: UserRole): string {
  const displayNames: Record<UserRole, string> = {
    platform_admin: 'Platform Admin',
    customer_admin: 'Admin',
    revops_manager: 'RevOps Manager',
    revops_analyst: 'Analyst',
    sales_manager: 'Sales Manager',
    sales_rep: 'Sales Rep',
    support_engineer: 'Support',
  };
  return displayNames[role] ?? role;
}

/**
 * Get display text for a tier.
 */
export function getTierDisplayName(tier: Tier): string {
  const displayNames: Record<Tier, string> = {
    starter: 'Starter',
    growth: 'Growth',
    enterprise: 'Enterprise',
  };
  return displayNames[tier] ?? tier;
}
