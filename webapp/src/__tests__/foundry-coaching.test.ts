/**
 * Integration tests for Rep Coaching domain module
 * Tests the fetcher functions with mock data
 */

import {
  getRepPerformance,
  getDriverComparisons,
  getActivityMetrics,
  getNextBestActions,
  getSalesReps,
} from '@/lib/foundry'

describe('Rep Coaching Fetchers', () => {
  describe('getSalesReps', () => {
    it('returns array of sales reps', async () => {
      const reps = await getSalesReps()

      expect(Array.isArray(reps)).toBe(true)
      expect(reps.length).toBeGreaterThan(0)

      const rep = reps[0]
      expect(rep.id).toBeDefined()
      expect(rep.name).toBeDefined()
      expect(rep.team).toBeDefined()
    })

    it('all reps have required fields', async () => {
      const reps = await getSalesReps()

      reps.forEach((rep) => {
        expect(typeof rep.id).toBe('string')
        expect(typeof rep.name).toBe('string')
        expect(typeof rep.team).toBe('string')
      })
    })
  })

  describe('getRepPerformance', () => {
    it('returns rep performance data', async () => {
      const perf = await getRepPerformance('REP-001')

      expect(perf).toBeDefined()
      expect(typeof perf.ytdAttainment).toBe('number')
      expect(typeof perf.winRate).toBe('number')
      expect(typeof perf.avgDealSize).toBe('number')
      expect(typeof perf.avgSalesCycle).toBe('number')
      expect(typeof perf.overallRank).toBe('number')
      expect(typeof perf.totalReps).toBe('number')
    })

    it('includes team comparison metrics', async () => {
      const perf = await getRepPerformance('REP-001')

      expect(typeof perf.teamAvgWinRate).toBe('number')
      expect(typeof perf.teamAvgDealSize).toBe('number')
      expect(typeof perf.teamAvgCycle).toBe('number')
    })
  })

  describe('getDriverComparisons', () => {
    it('returns array of driver comparisons', async () => {
      const drivers = await getDriverComparisons('REP-001')

      expect(Array.isArray(drivers)).toBe(true)
    })
  })

  describe('getActivityMetrics', () => {
    it('returns activity metrics with breakdown', async () => {
      const metrics = await getActivityMetrics('REP-001')

      expect(metrics).toBeDefined()
      expect(typeof metrics.totalActivities).toBe('number')
      expect(typeof metrics.calls).toBe('number')
      expect(typeof metrics.emails).toBe('number')
      expect(typeof metrics.meetings).toBe('number')
      expect(typeof metrics.demos).toBe('number')
    })

    it('includes daily comparison metrics', async () => {
      const metrics = await getActivityMetrics('REP-001')

      expect(typeof metrics.avgDaily).toBe('number')
      expect(typeof metrics.teamAvgDaily).toBe('number')
      expect(typeof metrics.topPerformerDaily).toBe('number')
    })
  })

  describe('getNextBestActions', () => {
    it('returns array of next best actions', async () => {
      const actions = await getNextBestActions('REP-001')

      expect(Array.isArray(actions)).toBe(true)
    })
  })
})
