/**
 * Core Foundry utilities shared across all domain modules
 */

import { getMockData } from '../mockLoader'

// Configuration - only USE_MOCK_DATA is client-safe
export const USE_MOCK_DATA = process.env.NEXT_PUBLIC_USE_MOCK_DATA !== 'false'

// Helper to get mock data lazily
export async function mock<K extends keyof Awaited<ReturnType<typeof getMockData>>>(
  key: K
): Promise<Awaited<ReturnType<typeof getMockData>>[K]> {
  const data = await getMockData()
  return data[key]
}

// =====================================================
// FILTER TYPES
// =====================================================

export interface DashboardFilters {
  quarter: string  // e.g., 'Q4 2024'
  team: string     // e.g., 'All Teams', 'West', 'Central', etc.
}

// Helper to build SQL WHERE clause for filters
export function buildFilterClause(
  filters: DashboardFilters,
  quarterColumn = 'quarter',
  teamColumn = 'region'
): string {
  const clauses: string[] = []

  if (filters.quarter) {
    clauses.push(`${quarterColumn} = '${filters.quarter}'`)
  }

  if (filters.team && filters.team !== 'All Teams') {
    clauses.push(`${teamColumn} = '${filters.team}'`)
  }

  return clauses.length > 0 ? clauses.join(' AND ') : '1=1'
}

// =====================================================
// API CLIENT (calls secure server-side route)
// =====================================================

interface QueryParams {
  query: string
  fallbackBranchIds?: string[]
}

export async function executeFoundrySQL<T>(params: QueryParams): Promise<T[]> {
  // Call our secure API route instead of Foundry directly
  const response = await fetch('/api/foundry', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      query: params.query,
      fallbackBranchIds: params.fallbackBranchIds || ['master'],
    }),
  })

  const result = await response.json()

  // Server tells us to use mock data if Foundry isn't configured
  if (result.useMock) {
    throw new Error('Mock mode - Foundry not configured on server')
  }

  if (!response.ok) {
    throw new Error(result.error || `Foundry SQL error: ${response.status}`)
  }

  return result.rows as T[]
}

// =====================================================
// DATA SOURCE INDICATOR
// =====================================================

/**
 * Synchronous data source check based on client-side env var.
 * NOTE: This doesn't know if the server fell back to mock mode.
 * Use getDataSourceAsync() for accurate detection.
 */
export function getDataSource(): 'foundry' | 'mock' {
  return USE_MOCK_DATA ? 'mock' : 'foundry'
}

/**
 * Async data source check that queries the server to determine
 * the actual data source. Returns 'foundry' only if both:
 * 1. Client is not in mock mode (NEXT_PUBLIC_USE_MOCK_DATA !== 'false')
 * 2. Server has Foundry configured (FOUNDRY_URL and FOUNDRY_TOKEN set)
 */
export async function getDataSourceAsync(): Promise<'foundry' | 'mock'> {
  // Client-side mock mode takes precedence
  if (USE_MOCK_DATA) {
    return 'mock'
  }

  try {
    // Check server configuration via health endpoint
    const response = await fetch('/api/foundry', { method: 'GET' })
    if (!response.ok) {
      return 'mock'
    }

    const result = await response.json()
    return result.configured ? 'foundry' : 'mock'
  } catch {
    return 'mock'
  }
}

// =====================================================
// CONNECTION CHECK
// =====================================================

export async function checkFoundryConnection(): Promise<boolean> {
  if (USE_MOCK_DATA) {
    return false
  }

  try {
    const response = await fetch('/api/foundry', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: 'SELECT 1 as health_check',
        fallbackBranchIds: ['master'],
      }),
    })

    if (!response.ok) return false

    const result = await response.json()
    return !result.useMock
  } catch {
    return false
  }
}
