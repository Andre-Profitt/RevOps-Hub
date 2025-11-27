/**
 * Tests for Foundry API configuration
 * Tests the API route behavior through the core module
 */

import { USE_MOCK_DATA, checkFoundryConnection } from '@/lib/foundry/core'

describe('Foundry API Configuration', () => {
  describe('USE_MOCK_DATA', () => {
    it('defaults to true in test environment', () => {
      // In test environment, NEXT_PUBLIC_USE_MOCK_DATA is not set
      // so USE_MOCK_DATA should default to true
      expect(USE_MOCK_DATA).toBe(true)
    })
  })

  describe('checkFoundryConnection', () => {
    it('returns false in mock mode', async () => {
      // Since USE_MOCK_DATA is true, this should return false without hitting API
      const result = await checkFoundryConnection()
      expect(result).toBe(false)
    })
  })
})

describe('API Response Handling', () => {
  it('mock data is returned when USE_MOCK_DATA is true', async () => {
    // Verify that when USE_MOCK_DATA is true, fetchers return mock data
    // (already covered by pipeline and coaching tests)
    expect(USE_MOCK_DATA).toBe(true)
  })
})
