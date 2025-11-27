'use client';

/**
 * Authentication Context Provider
 *
 * Provides authentication state and user information throughout the app.
 */

import React, { createContext, useContext, useEffect, useState, useCallback } from 'react';
import type { User, AuthState, Session, UserRole, Tier } from './types';
import { hasPermission, hasFeature, hasMinRole, canAccessPage } from './permissions';

// =============================================================================
// CONTEXT TYPES
// =============================================================================

interface AuthContextValue extends AuthState {
  login: (email: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
  checkPermission: (permission: string) => boolean;
  checkFeature: (feature: string) => boolean;
  checkRole: (role: UserRole) => boolean;
  checkPageAccess: (page: string) => boolean;
}

// =============================================================================
// MOCK USER DATA (for development)
// =============================================================================

const MOCK_USERS: Record<string, User> = {
  'admin@acme.com': {
    id: 'user-001',
    email: 'admin@acme.com',
    name: 'Admin User',
    role: 'customer_admin',
    customerId: 'acme-corp',
    customerName: 'Acme Corporation',
    tier: 'enterprise',
  },
  'manager@acme.com': {
    id: 'user-002',
    email: 'manager@acme.com',
    name: 'Sales Manager',
    role: 'sales_manager',
    customerId: 'acme-corp',
    customerName: 'Acme Corporation',
    tier: 'enterprise',
    teamMembers: ['user-003', 'user-004', 'user-005'],
  },
  'rep@acme.com': {
    id: 'user-003',
    email: 'rep@acme.com',
    name: 'Sales Rep',
    role: 'sales_rep',
    customerId: 'acme-corp',
    customerName: 'Acme Corporation',
    tier: 'enterprise',
    managerId: 'user-002',
  },
  'analyst@startup.co': {
    id: 'user-010',
    email: 'analyst@startup.co',
    name: 'RevOps Analyst',
    role: 'revops_analyst',
    customerId: 'startup-co',
    customerName: 'StartupCo Inc',
    tier: 'starter',
  },
};

// =============================================================================
// CONTEXT
// =============================================================================

const AuthContext = createContext<AuthContextValue | null>(null);

// =============================================================================
// PROVIDER
// =============================================================================

interface AuthProviderProps {
  children: React.ReactNode;
}

export function AuthProvider({ children }: AuthProviderProps) {
  const [state, setState] = useState<AuthState>({
    isAuthenticated: false,
    isLoading: true,
    user: null,
    error: null,
  });

  // Check for existing session on mount
  useEffect(() => {
    const checkSession = async () => {
      try {
        // In production, validate token with backend
        const savedSession = localStorage.getItem('revops_session');
        if (savedSession) {
          const session: Session = JSON.parse(savedSession);
          if (session.expiresAt > Date.now()) {
            setState({
              isAuthenticated: true,
              isLoading: false,
              user: session.user,
              error: null,
            });
            return;
          }
          // Session expired
          localStorage.removeItem('revops_session');
        }
      } catch (err) {
        console.error('Session check failed:', err);
      }

      setState(prev => ({ ...prev, isLoading: false }));
    };

    checkSession();
  }, []);

  const login = useCallback(async (email: string, password: string) => {
    setState(prev => ({ ...prev, isLoading: true, error: null }));

    try {
      // In production, call auth API
      // const response = await fetch('/api/auth/login', {
      //   method: 'POST',
      //   body: JSON.stringify({ email, password }),
      // });

      // For development, use mock users
      const user = MOCK_USERS[email.toLowerCase()];
      if (!user) {
        throw new Error('Invalid email or password');
      }

      // Simulate API delay
      await new Promise(resolve => setTimeout(resolve, 500));

      const session: Session = {
        user,
        accessToken: 'mock-token-' + Date.now(),
        expiresAt: Date.now() + 24 * 60 * 60 * 1000, // 24 hours
      };

      localStorage.setItem('revops_session', JSON.stringify(session));

      setState({
        isAuthenticated: true,
        isLoading: false,
        user,
        error: null,
      });
    } catch (err) {
      setState({
        isAuthenticated: false,
        isLoading: false,
        user: null,
        error: err instanceof Error ? err.message : 'Login failed',
      });
      throw err;
    }
  }, []);

  const logout = useCallback(async () => {
    // In production, call logout API
    localStorage.removeItem('revops_session');
    setState({
      isAuthenticated: false,
      isLoading: false,
      user: null,
      error: null,
    });
  }, []);

  const checkPermission = useCallback(
    (permission: string) => hasPermission(state.user, permission as any),
    [state.user]
  );

  const checkFeature = useCallback(
    (feature: string) => hasFeature(state.user, feature as any),
    [state.user]
  );

  const checkRole = useCallback(
    (role: UserRole) => hasMinRole(state.user, role),
    [state.user]
  );

  const checkPageAccess = useCallback(
    (page: string) => canAccessPage(state.user, page),
    [state.user]
  );

  const value: AuthContextValue = {
    ...state,
    login,
    logout,
    checkPermission,
    checkFeature,
    checkRole,
    checkPageAccess,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

// =============================================================================
// HOOKS
// =============================================================================

/**
 * Hook to access auth context.
 */
export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
}

/**
 * Hook to get current user.
 */
export function useUser() {
  const { user } = useAuth();
  return user;
}

/**
 * Hook to check if user is authenticated.
 */
export function useIsAuthenticated() {
  const { isAuthenticated, isLoading } = useAuth();
  return { isAuthenticated, isLoading };
}

/**
 * Hook to check permissions.
 */
export function usePermission(permission: string) {
  const { checkPermission } = useAuth();
  return checkPermission(permission);
}

/**
 * Hook to check feature access.
 */
export function useFeature(feature: string) {
  const { checkFeature } = useAuth();
  return checkFeature(feature);
}

// =============================================================================
// HIGHER-ORDER COMPONENTS
// =============================================================================

interface WithAuthProps {
  requiredRole?: UserRole;
  requiredPermission?: string;
  requiredFeature?: string;
  fallback?: React.ReactNode;
}

/**
 * HOC to protect a component with auth requirements.
 */
export function withAuth<P extends object>(
  Component: React.ComponentType<P>,
  options: WithAuthProps = {}
) {
  const { requiredRole, requiredPermission, requiredFeature, fallback = null } = options;

  return function ProtectedComponent(props: P) {
    const { isAuthenticated, isLoading, user, checkRole, checkPermission, checkFeature } =
      useAuth();

    if (isLoading) {
      return <div>Loading...</div>;
    }

    if (!isAuthenticated || !user) {
      return fallback as React.ReactElement | null;
    }

    if (requiredRole && !checkRole(requiredRole)) {
      return fallback as React.ReactElement | null;
    }

    if (requiredPermission && !checkPermission(requiredPermission)) {
      return fallback as React.ReactElement | null;
    }

    if (requiredFeature && !checkFeature(requiredFeature)) {
      return fallback as React.ReactElement | null;
    }

    return <Component {...props} />;
  };
}

// =============================================================================
// RENDER PROP COMPONENTS
// =============================================================================

interface RequireAuthProps {
  children: React.ReactNode;
  fallback?: React.ReactNode;
}

/**
 * Component that only renders children if user is authenticated.
 */
export function RequireAuth({ children, fallback = null }: RequireAuthProps) {
  const { isAuthenticated, isLoading } = useAuth();

  if (isLoading) {
    return <div>Loading...</div>;
  }

  if (!isAuthenticated) {
    return fallback as React.ReactElement | null;
  }

  return <>{children}</>;
}

interface RequireRoleProps {
  role: UserRole;
  children: React.ReactNode;
  fallback?: React.ReactNode;
}

/**
 * Component that only renders children if user has required role.
 */
export function RequireRole({ role, children, fallback = null }: RequireRoleProps) {
  const { checkRole, isLoading } = useAuth();

  if (isLoading) {
    return null;
  }

  if (!checkRole(role)) {
    return fallback as React.ReactElement | null;
  }

  return <>{children}</>;
}

interface RequireFeatureProps {
  feature: string;
  children: React.ReactNode;
  fallback?: React.ReactNode;
}

/**
 * Component that only renders children if customer has feature.
 */
export function RequireFeature({ feature, children, fallback = null }: RequireFeatureProps) {
  const { checkFeature, isLoading } = useAuth();

  if (isLoading) {
    return null;
  }

  if (!checkFeature(feature)) {
    return fallback as React.ReactElement | null;
  }

  return <>{children}</>;
}

interface RequirePermissionProps {
  permission: string;
  children: React.ReactNode;
  fallback?: React.ReactNode;
}

/**
 * Component that only renders children if user has permission.
 */
export function RequirePermission({
  permission,
  children,
  fallback = null,
}: RequirePermissionProps) {
  const { checkPermission, isLoading } = useAuth();

  if (isLoading) {
    return null;
  }

  if (!checkPermission(permission)) {
    return fallback as React.ReactElement | null;
  }

  return <>{children}</>;
}
