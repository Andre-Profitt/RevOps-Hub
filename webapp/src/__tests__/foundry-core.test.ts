import { buildFilterClause, getDataSource, USE_MOCK_DATA } from '@/lib/foundry/core'

describe('buildFilterClause', () => {
  it('returns 1=1 when no filters applied', () => {
    const result = buildFilterClause({ quarter: '', team: '' })
    expect(result).toBe('1=1')
  })

  it('filters by quarter', () => {
    const result = buildFilterClause({ quarter: 'Q4 2024', team: '' })
    expect(result).toBe("quarter = 'Q4 2024'")
  })

  it('filters by team (not All Teams)', () => {
    const result = buildFilterClause({ quarter: '', team: 'West' })
    expect(result).toBe("region = 'West'")
  })

  it('ignores All Teams filter', () => {
    const result = buildFilterClause({ quarter: '', team: 'All Teams' })
    expect(result).toBe('1=1')
  })

  it('combines quarter and team filters', () => {
    const result = buildFilterClause({ quarter: 'Q4 2024', team: 'West' })
    expect(result).toContain("quarter = 'Q4 2024'")
    expect(result).toContain("region = 'West'")
    expect(result).toContain(' AND ')
  })

  it('uses custom column names', () => {
    const result = buildFilterClause(
      { quarter: 'Q4 2024', team: 'West' },
      'fiscal_quarter',
      'territory'
    )
    expect(result).toContain("fiscal_quarter = 'Q4 2024'")
    expect(result).toContain("territory = 'West'")
  })
})

describe('getDataSource', () => {
  it('returns mock or foundry based on USE_MOCK_DATA', () => {
    const source = getDataSource()
    expect(['mock', 'foundry']).toContain(source)

    if (USE_MOCK_DATA) {
      expect(source).toBe('mock')
    } else {
      expect(source).toBe('foundry')
    }
  })
})

describe('USE_MOCK_DATA', () => {
  it('is a boolean', () => {
    expect(typeof USE_MOCK_DATA).toBe('boolean')
  })
})
