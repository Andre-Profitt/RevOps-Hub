import { formatCurrency, formatPercent, formatDelta, cn, getHealthCategory, getHealthLabel, calculateProgress } from '@/lib/utils'

describe('formatCurrency', () => {
  it('formats small numbers correctly', () => {
    expect(formatCurrency(1234)).toBe('$1K')
    expect(formatCurrency(999)).toBe('$999')
  })

  it('formats thousands correctly', () => {
    expect(formatCurrency(50000)).toBe('$50K')
    expect(formatCurrency(125000)).toBe('$125K')
  })

  it('formats millions correctly', () => {
    expect(formatCurrency(1500000)).toBe('$1.5M')
    expect(formatCurrency(12500000)).toBe('$12.5M')
  })

  it('handles zero', () => {
    expect(formatCurrency(0)).toBe('$0')
  })

  it('handles non-compact format', () => {
    expect(formatCurrency(50000, false)).toBe('$50,000')
    expect(formatCurrency(1500000, false)).toBe('$1,500,000')
  })
})

describe('formatPercent', () => {
  it('formats decimals as percentages', () => {
    expect(formatPercent(0.85)).toBe('85%')
    expect(formatPercent(0.5)).toBe('50%')
  })

  it('handles values over 100%', () => {
    expect(formatPercent(1.25)).toBe('125%')
  })

  it('rounds to whole numbers', () => {
    expect(formatPercent(0.8567)).toBe('86%')
  })

  it('shows sign when requested', () => {
    expect(formatPercent(0.15, true)).toBe('+15%')
    expect(formatPercent(-0.10, true)).toBe('-10%')
  })
})

describe('formatDelta', () => {
  it('formats positive deltas with plus sign', () => {
    expect(formatDelta(5)).toBe('+5')
    expect(formatDelta(5, 'd')).toBe('+5d')
  })

  it('formats negative deltas', () => {
    expect(formatDelta(-3)).toBe('-3')
    expect(formatDelta(-3, '%')).toBe('-3%')
  })

  it('formats zero with plus sign (>=0 behavior)', () => {
    expect(formatDelta(0)).toBe('+0')
  })
})

describe('cn (className merger)', () => {
  it('merges class names', () => {
    expect(cn('foo', 'bar')).toBe('foo bar')
  })

  it('handles conditional classes', () => {
    expect(cn('foo', true && 'bar', false && 'baz')).toBe('foo bar')
  })

  it('handles undefined and null', () => {
    expect(cn('foo', undefined, null, 'bar')).toBe('foo bar')
  })

  it('handles Tailwind class conflicts', () => {
    // tailwind-merge should handle conflicts
    expect(cn('p-4', 'p-6')).toBe('p-6')
  })
})

describe('getHealthCategory', () => {
  it('returns healthy for scores >= 80', () => {
    expect(getHealthCategory(80)).toBe('healthy')
    expect(getHealthCategory(100)).toBe('healthy')
  })

  it('returns monitor for scores 60-79', () => {
    expect(getHealthCategory(60)).toBe('monitor')
    expect(getHealthCategory(79)).toBe('monitor')
  })

  it('returns at-risk for scores 40-59', () => {
    expect(getHealthCategory(40)).toBe('at-risk')
    expect(getHealthCategory(59)).toBe('at-risk')
  })

  it('returns critical for scores < 40', () => {
    expect(getHealthCategory(39)).toBe('critical')
    expect(getHealthCategory(0)).toBe('critical')
  })
})

describe('getHealthLabel', () => {
  it('returns appropriate labels', () => {
    expect(getHealthLabel(85)).toBe('Healthy')
    expect(getHealthLabel(65)).toBe('Monitor')
    expect(getHealthLabel(45)).toBe('At Risk')
    expect(getHealthLabel(25)).toBe('Critical')
  })
})

describe('calculateProgress', () => {
  it('calculates percentage correctly', () => {
    expect(calculateProgress(50, 100)).toBe(50)
    expect(calculateProgress(75, 100)).toBe(75)
  })

  it('handles zero target', () => {
    expect(calculateProgress(50, 0)).toBe(0)
  })

  it('caps at 100%', () => {
    expect(calculateProgress(150, 100)).toBe(100)
  })

  it('floors at 0%', () => {
    expect(calculateProgress(-50, 100)).toBe(0)
  })
})
