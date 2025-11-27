/**
 * Authentication and Authorization Module
 *
 * Export all auth-related utilities, types, and components.
 */

// Types
export type {
  UserRole,
  Permission,
  Feature,
  Tier,
  User,
  Session,
  AuthState,
  RLSContext,
  RLSPolicy,
  NavItem,
} from './types';

export {
  ROLE_HIERARCHY,
  ROLE_PERMISSIONS,
  TIER_FEATURES,
  ROLE_RLS_POLICY,
} from './types';

// Permission utilities
export {
  hasRole,
  hasMinRole,
  hasAnyRole,
  getRoleLevel,
  hasPermission,
  hasAllPermissions,
  hasAnyPermission,
  getUserPermissions,
  tierHasFeature,
  hasFeature,
  getTierFeatures,
  canAccessPage,
  getRLSPolicy,
  buildRLSContext,
  createRLSFilter,
  applyRLS,
  canViewUserData,
  canModifyConfig,
  canExportData,
  canTriggerBuild,
  filterNavItems,
  getRoleDisplayName,
  getTierDisplayName,
} from './permissions';

// React context and hooks
export {
  AuthProvider,
  useAuth,
  useUser,
  useIsAuthenticated,
  usePermission,
  useFeature,
  withAuth,
  RequireAuth,
  RequireRole,
  RequireFeature,
  RequirePermission,
} from './context';
