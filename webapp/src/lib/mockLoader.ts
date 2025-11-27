/**
 * Lazy Mock Data Loader
 *
 * Only loads mock data when needed, keeping it out of the production bundle
 * when NEXT_PUBLIC_USE_MOCK_DATA is false.
 */

// Cache for lazy-loaded mock data
let mockDataCache: typeof import('@/data/mockData') | null = null

/**
 * Lazily loads mock data only when needed.
 * Uses dynamic import to keep mock data out of the client bundle in production.
 */
export async function getMockData(): Promise<typeof import('@/data/mockData')> {
  if (mockDataCache) {
    return mockDataCache
  }

  // Dynamic import - only loads when called
  const data = await import('@/data/mockData')
  mockDataCache = data
  return data
}

/**
 * Gets a specific mock data key lazily.
 * Returns undefined if mock data loading fails.
 */
export async function getMockDataKey<K extends keyof typeof import('@/data/mockData')>(
  key: K
): Promise<(typeof import('@/data/mockData'))[K] | undefined> {
  try {
    const data = await getMockData()
    return data[key]
  } catch {
    return undefined
  }
}

/**
 * Synchronous check if we should use mock data.
 * This can be checked before deciding whether to load mock data.
 */
export function shouldUseMockData(): boolean {
  // In browser, check the env var
  if (typeof window !== 'undefined') {
    return process.env.NEXT_PUBLIC_USE_MOCK_DATA !== 'false'
  }
  // On server, default to true unless explicitly disabled
  return process.env.NEXT_PUBLIC_USE_MOCK_DATA !== 'false'
}
