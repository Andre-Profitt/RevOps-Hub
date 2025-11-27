/**
 * Integration tests for Pipeline Health domain module
 * Tests the fetcher functions with mock data
 */

import {
  getDashboardKPIs,
  getForecastTrend,
  getStuckDeals,
  getHealthDistribution,
  getProcessBottlenecks,
  getCompetitiveLosses,
  getStageVelocity,
  getPipelineFunnel,
} from '@/lib/foundry'

// These tests run in mock mode (USE_MOCK_DATA = true by default)

describe('Pipeline Health Fetchers', () => {
  describe('getDashboardKPIs', () => {
    it('returns KPI data with all required fields', async () => {
      const kpis = await getDashboardKPIs()

      expect(kpis).toBeDefined()
      expect(typeof kpis.quarterTarget).toBe('number')
      expect(typeof kpis.closedWonAmount).toBe('number')
      expect(typeof kpis.aiForecast).toBe('number')
      expect(typeof kpis.commitAmount).toBe('number')
      expect(typeof kpis.coverageRatio).toBe('number')
      expect(['Low', 'Medium', 'High', 'Critical']).toContain(kpis.riskLevel)
    })

    it('accepts filter parameters', async () => {
      const kpis = await getDashboardKPIs({ quarter: 'Q4 2024', team: 'West' })
      expect(kpis).toBeDefined()
    })
  })

  describe('getForecastTrend', () => {
    it('returns array of forecast trend points', async () => {
      const trend = await getForecastTrend()

      expect(Array.isArray(trend)).toBe(true)
      expect(trend.length).toBeGreaterThan(0)

      const point = trend[0]
      expect(typeof point.week).toBe('string')
      expect(typeof point.weekNumber).toBe('number')
      expect(typeof point.target).toBe('number')
      expect(typeof point.closed).toBe('number')
    })
  })

  describe('getStuckDeals', () => {
    it('returns array of stuck deals', async () => {
      const deals = await getStuckDeals()

      expect(Array.isArray(deals)).toBe(true)
      expect(deals.length).toBeGreaterThan(0)

      const deal = deals[0]
      expect(deal.id).toBeDefined()
      expect(deal.accountName).toBeDefined()
      expect(deal.amount).toBeDefined()
      expect(deal.healthScore).toBeDefined()
      expect(['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']).toContain(deal.urgency)
    })

    it('filters by team', async () => {
      const westDeals = await getStuckDeals({ quarter: 'Q4 2024', team: 'West' })
      expect(Array.isArray(westDeals)).toBe(true)
    })
  })

  describe('getHealthDistribution', () => {
    it('returns health distribution with all categories', async () => {
      const health = await getHealthDistribution()

      expect(health.healthy).toBeDefined()
      expect(health.monitor).toBeDefined()
      expect(health.atRisk).toBeDefined()
      expect(health.critical).toBeDefined()

      expect(typeof health.healthy.count).toBe('number')
      expect(typeof health.healthy.amount).toBe('number')
      expect(typeof health.healthy.pct).toBe('number')
    })
  })

  describe('getProcessBottlenecks', () => {
    it('returns array of bottlenecks', async () => {
      const bottlenecks = await getProcessBottlenecks()

      expect(Array.isArray(bottlenecks)).toBe(true)
      expect(bottlenecks.length).toBeGreaterThan(0)

      const bottleneck = bottlenecks[0]
      expect(bottleneck.activity).toBeDefined()
      expect(typeof bottleneck.avgDuration).toBe('number')
      expect(typeof bottleneck.benchmarkDays).toBe('number')
    })
  })

  describe('getCompetitiveLosses', () => {
    it('returns array of competitive losses', async () => {
      const losses = await getCompetitiveLosses()

      expect(Array.isArray(losses)).toBe(true)
      expect(losses.length).toBeGreaterThan(0)

      const loss = losses[0]
      expect(loss.competitor).toBeDefined()
      expect(typeof loss.dealsLost).toBe('number')
      expect(typeof loss.revenueLost).toBe('number')
    })
  })

  describe('getStageVelocity', () => {
    it('returns array of stage velocity metrics', async () => {
      const velocity = await getStageVelocity()

      expect(Array.isArray(velocity)).toBe(true)
      expect(velocity.length).toBeGreaterThan(0)

      const stage = velocity[0]
      expect(stage.stageName).toBeDefined()
      expect(typeof stage.stageOrder).toBe('number')
      expect(typeof stage.avgDuration).toBe('number')
      expect(typeof stage.benchmark).toBe('number')
      expect(typeof stage.conversionRate).toBe('number')
      expect(typeof stage.dealsInStage).toBe('number')
      expect(typeof stage.amountInStage).toBe('number')
    })

    it('stages are ordered by stageOrder', async () => {
      const velocity = await getStageVelocity()

      for (let i = 1; i < velocity.length; i++) {
        expect(velocity[i].stageOrder).toBeGreaterThan(velocity[i - 1].stageOrder)
      }
    })
  })

  describe('getPipelineFunnel', () => {
    it('returns array of funnel stages', async () => {
      const funnel = await getPipelineFunnel()

      expect(Array.isArray(funnel)).toBe(true)
      expect(funnel.length).toBeGreaterThan(0)

      const stage = funnel[0]
      expect(stage.name).toBeDefined()
      expect(typeof stage.amount).toBe('number')
      expect(typeof stage.count).toBe('number')
    })
  })
})
