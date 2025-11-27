/**
 * Sales Collateral Generator
 * Generate ROI reports and sales materials
 */

import type { TenantROI } from '../foundry/ops'

// =============================================================================
// TYPES
// =============================================================================

export interface ROIReportData {
  customer: {
    id: string
    name: string
    tier: string
    startDate: string
    monthsActive: number
  }
  baseline: {
    winRate: number
    avgCycleTime: number
    forecastAccuracy: number
    dataHygieneScore: number
  }
  current: {
    winRate: number
    avgCycleTime: number
    forecastAccuracy: number
    dataHygieneScore: number
  }
  improvements: {
    winRateImprovement: number
    cycleTimeReduction: number
    forecastAccuracyGain: number
    dataHygieneImprovement: number
  }
  valueCalculation: {
    revenueImpact: number
    timeSavings: number
    forecastingValue: number
    totalAnnualValue: number
  }
  benchmarks: {
    industryAvgWinRate: number
    industryAvgCycleTime: number
    topPerformerWinRate: number
    topPerformerCycleTime: number
  }
}

export interface SalesDeckSlide {
  type: 'title' | 'metrics' | 'comparison' | 'testimonial' | 'cta'
  title: string
  content: Record<string, unknown>
}

// =============================================================================
// REPORT GENERATOR
// =============================================================================

export class CollateralGenerator {
  /**
   * Generate detailed ROI report for a customer
   */
  generateROIReport(roi: TenantROI, baseline: Partial<ROIReportData['baseline']> = {}): ROIReportData {
    // Calculate baseline values (before RevOps)
    const baselineData: ROIReportData['baseline'] = {
      winRate: baseline.winRate ?? 22,
      avgCycleTime: baseline.avgCycleTime ?? 45,
      forecastAccuracy: baseline.forecastAccuracy ?? 65,
      dataHygieneScore: baseline.dataHygieneScore ?? 60,
    }

    // Calculate current values
    const currentData: ROIReportData['current'] = {
      winRate: baselineData.winRate + roi.winRateImprovement,
      avgCycleTime: baselineData.avgCycleTime * (1 - roi.cycleTimeReduction / 100),
      forecastAccuracy: baselineData.forecastAccuracy + roi.forecastAccuracyGain,
      dataHygieneScore: baselineData.dataHygieneScore + roi.dataHygieneImprovement,
    }

    // Value calculations
    const avgDealSize = roi.tier === 'enterprise' ? 150000 :
                        roi.tier === 'growth' ? 50000 : 20000
    const dealsPerYear = roi.tier === 'enterprise' ? 200 :
                         roi.tier === 'growth' ? 100 : 50

    const additionalWins = dealsPerYear * (roi.winRateImprovement / 100)
    const revenueImpact = additionalWins * avgDealSize

    const hoursSavedPerDeal = (baselineData.avgCycleTime - currentData.avgCycleTime) * 2 // 2 hours/day saved
    const timeSavings = hoursSavedPerDeal * dealsPerYear * 75 // $75/hour

    const forecastingValue = roi.forecastAccuracyGain * 5000 // $5K per accuracy point

    return {
      customer: {
        id: roi.customerId,
        name: roi.customerName,
        tier: roi.tier,
        startDate: roi.periodStart,
        monthsActive: 12, // Could be calculated from actual data
      },
      baseline: baselineData,
      current: currentData,
      improvements: {
        winRateImprovement: roi.winRateImprovement,
        cycleTimeReduction: roi.cycleTimeReduction,
        forecastAccuracyGain: roi.forecastAccuracyGain,
        dataHygieneImprovement: roi.dataHygieneImprovement,
      },
      valueCalculation: {
        revenueImpact,
        timeSavings,
        forecastingValue,
        totalAnnualValue: roi.estimatedAnnualValue,
      },
      benchmarks: {
        industryAvgWinRate: 25,
        industryAvgCycleTime: 42,
        topPerformerWinRate: 35,
        topPerformerCycleTime: 28,
      },
    }
  }

  /**
   * Generate HTML ROI report
   */
  generateHTMLReport(report: ROIReportData): string {
    return `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>ROI Report - ${report.customer.name}</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 40px; color: #1a1a1a; }
    .header { text-align: center; margin-bottom: 40px; }
    .logo { font-size: 24px; font-weight: bold; color: #2563eb; }
    h1 { font-size: 32px; margin: 20px 0 10px; }
    .subtitle { color: #6b7280; font-size: 18px; }
    .summary { background: linear-gradient(135deg, #2563eb 0%, #7c3aed 100%); color: white; padding: 30px; border-radius: 16px; margin-bottom: 40px; }
    .summary-title { font-size: 24px; margin-bottom: 20px; }
    .summary-value { font-size: 48px; font-weight: bold; }
    .metrics { display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px; margin-bottom: 40px; }
    .metric { background: #f9fafb; padding: 24px; border-radius: 12px; }
    .metric-label { color: #6b7280; font-size: 14px; margin-bottom: 8px; }
    .metric-value { font-size: 28px; font-weight: bold; }
    .metric-change { font-size: 14px; margin-top: 8px; }
    .positive { color: #059669; }
    .comparison { margin-bottom: 40px; }
    .comparison-row { display: flex; align-items: center; padding: 16px 0; border-bottom: 1px solid #e5e7eb; }
    .comparison-label { flex: 1; }
    .comparison-bar { flex: 2; display: flex; align-items: center; }
    .bar-baseline { height: 8px; background: #d1d5db; border-radius: 4px; }
    .bar-current { height: 8px; background: #2563eb; border-radius: 4px; margin-left: 8px; }
    .comparison-values { flex: 1; text-align: right; font-size: 14px; }
    .footer { text-align: center; color: #6b7280; font-size: 12px; margin-top: 40px; padding-top: 20px; border-top: 1px solid #e5e7eb; }
  </style>
</head>
<body>
  <div class="header">
    <div class="logo">RevOps Command Center</div>
    <h1>ROI Report</h1>
    <div class="subtitle">${report.customer.name} | ${report.customer.tier.charAt(0).toUpperCase() + report.customer.tier.slice(1)} Plan</div>
  </div>

  <div class="summary">
    <div class="summary-title">Total Annual Value Created</div>
    <div class="summary-value">$${report.valueCalculation.totalAnnualValue.toLocaleString()}</div>
  </div>

  <div class="metrics">
    <div class="metric">
      <div class="metric-label">Win Rate Improvement</div>
      <div class="metric-value">${report.improvements.winRateImprovement >= 0 ? '+' : ''}${report.improvements.winRateImprovement.toFixed(1)}%</div>
      <div class="metric-change positive">Now at ${report.current.winRate.toFixed(1)}% (was ${report.baseline.winRate}%)</div>
    </div>
    <div class="metric">
      <div class="metric-label">Cycle Time Reduction</div>
      <div class="metric-value">-${report.improvements.cycleTimeReduction.toFixed(0)}%</div>
      <div class="metric-change positive">Now ${report.current.avgCycleTime.toFixed(0)} days (was ${report.baseline.avgCycleTime} days)</div>
    </div>
    <div class="metric">
      <div class="metric-label">Forecast Accuracy Gain</div>
      <div class="metric-value">+${report.improvements.forecastAccuracyGain.toFixed(1)}%</div>
      <div class="metric-change positive">Now at ${report.current.forecastAccuracy.toFixed(1)}% (was ${report.baseline.forecastAccuracy}%)</div>
    </div>
    <div class="metric">
      <div class="metric-label">Data Hygiene Improvement</div>
      <div class="metric-value">+${report.improvements.dataHygieneImprovement.toFixed(1)}%</div>
      <div class="metric-change positive">Now at ${report.current.dataHygieneScore.toFixed(1)}% (was ${report.baseline.dataHygieneScore}%)</div>
    </div>
  </div>

  <h2>Value Breakdown</h2>
  <div class="metrics">
    <div class="metric">
      <div class="metric-label">Revenue Impact</div>
      <div class="metric-value">$${report.valueCalculation.revenueImpact.toLocaleString()}</div>
      <div class="metric-change">From additional won deals</div>
    </div>
    <div class="metric">
      <div class="metric-label">Time Savings</div>
      <div class="metric-value">$${report.valueCalculation.timeSavings.toLocaleString()}</div>
      <div class="metric-change">From faster sales cycles</div>
    </div>
  </div>

  <h2>Performance vs Benchmarks</h2>
  <div class="comparison">
    <div class="comparison-row">
      <div class="comparison-label">Win Rate</div>
      <div class="comparison-bar">
        <div class="bar-baseline" style="width: ${report.benchmarks.industryAvgWinRate * 3}px" title="Industry Avg"></div>
        <div class="bar-current" style="width: ${report.current.winRate * 3}px" title="Your Performance"></div>
      </div>
      <div class="comparison-values">
        <strong>${report.current.winRate.toFixed(1)}%</strong> vs ${report.benchmarks.industryAvgWinRate}% avg
      </div>
    </div>
    <div class="comparison-row">
      <div class="comparison-label">Cycle Time</div>
      <div class="comparison-bar">
        <div class="bar-baseline" style="width: ${report.benchmarks.industryAvgCycleTime * 2}px" title="Industry Avg"></div>
        <div class="bar-current" style="width: ${report.current.avgCycleTime * 2}px" title="Your Performance"></div>
      </div>
      <div class="comparison-values">
        <strong>${report.current.avgCycleTime.toFixed(0)} days</strong> vs ${report.benchmarks.industryAvgCycleTime} days avg
      </div>
    </div>
  </div>

  <div class="footer">
    Generated by RevOps Command Center | ${new Date().toLocaleDateString()}
  </div>
</body>
</html>
    `.trim()
  }

  /**
   * Generate sales deck slides
   */
  generateSalesDeck(aggregateROI: TenantROI[]): SalesDeckSlide[] {
    const avgWinRate = aggregateROI.reduce((sum, r) => sum + r.winRateImprovement, 0) / aggregateROI.length
    const avgCycleTime = aggregateROI.reduce((sum, r) => sum + r.cycleTimeReduction, 0) / aggregateROI.length
    const totalValue = aggregateROI.reduce((sum, r) => sum + r.estimatedAnnualValue, 0)
    const customerCount = aggregateROI.length

    return [
      {
        type: 'title',
        title: 'RevOps Command Center',
        content: {
          subtitle: 'AI-Powered Revenue Operations',
          tagline: 'Turn your CRM data into revenue insights',
        },
      },
      {
        type: 'metrics',
        title: 'Proven Results',
        content: {
          metrics: [
            { label: 'Avg Win Rate Improvement', value: `+${avgWinRate.toFixed(1)}%` },
            { label: 'Avg Cycle Time Reduction', value: `-${avgCycleTime.toFixed(0)}%` },
            { label: 'Total Value Created', value: `$${(totalValue / 1000000).toFixed(1)}M` },
            { label: 'Customers', value: customerCount.toString() },
          ],
        },
      },
      {
        type: 'comparison',
        title: 'Before vs After',
        content: {
          before: {
            title: 'Without RevOps',
            points: [
              'Manual forecast compilation',
              'Inconsistent deal data',
              'Reactive pipeline management',
              'Limited visibility into trends',
            ],
          },
          after: {
            title: 'With RevOps',
            points: [
              'AI-powered forecasting',
              'Automated data hygiene',
              'Proactive deal insights',
              'Real-time analytics',
            ],
          },
        },
      },
      {
        type: 'testimonial',
        title: 'Customer Success',
        content: {
          quote: "RevOps Command Center transformed how we manage our pipeline. We're now 5% more accurate on forecasts and closing deals 20% faster.",
          author: 'VP of Sales, Enterprise Customer',
          metrics: {
            winRateGain: '+6.2%',
            cycleTimeReduction: '-22%',
            annualValue: '$1.2M',
          },
        },
      },
      {
        type: 'cta',
        title: 'Get Started Today',
        content: {
          headline: 'See Your ROI Potential',
          subheadline: 'Schedule a demo to see how RevOps can transform your revenue operations',
          cta: 'Book a Demo',
          ctaUrl: 'https://revops.io/demo',
        },
      },
    ]
  }

  /**
   * Generate CSV export of ROI data
   */
  generateCSVExport(roiData: TenantROI[]): string {
    const headers = [
      'Customer ID',
      'Customer Name',
      'Tier',
      'Win Rate Improvement (%)',
      'Cycle Time Reduction (%)',
      'Forecast Accuracy Gain (%)',
      'Data Hygiene Improvement (%)',
      'Estimated Annual Value ($)',
      'Period Start',
      'Period End',
    ]

    const rows = roiData.map(roi => [
      roi.customerId,
      roi.customerName,
      roi.tier,
      roi.winRateImprovement.toFixed(2),
      roi.cycleTimeReduction.toFixed(2),
      roi.forecastAccuracyGain.toFixed(2),
      roi.dataHygieneImprovement.toFixed(2),
      roi.estimatedAnnualValue.toString(),
      roi.periodStart,
      roi.periodEnd,
    ])

    const csvContent = [
      headers.join(','),
      ...rows.map(row => row.map(cell => `"${cell}"`).join(',')),
    ].join('\n')

    return csvContent
  }
}

// =============================================================================
// SINGLETON
// =============================================================================

export const collateralGenerator = new CollateralGenerator()
