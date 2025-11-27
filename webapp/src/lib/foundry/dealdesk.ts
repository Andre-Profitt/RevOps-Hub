/**
 * Deal Desk data fetchers
 */

import { USE_MOCK_DATA, mock, executeFoundrySQL } from './core'
import type {
  DealDeskSummary,
  DealApproval,
  DiscountAnalysis,
} from '@/types'

export async function getDealDeskSummary(): Promise<DealDeskSummary> {
  if (USE_MOCK_DATA) {
    return mock('dealDeskSummary')
  }

  const results = await executeFoundrySQL<any>({
    query: `
      SELECT *
      FROM \`/RevOps/DealDesk/summary\`
      ORDER BY snapshot_date DESC
      LIMIT 1
    `,
  })

  if (results.length === 0) {
    throw new Error('No deal desk summary found')
  }

  const row = results[0]
  return {
    pendingApprovals: row.pending_approvals,
    pendingValue: row.pending_value,
    avgApprovalTime: row.avg_approval_time,
    slaBreachCount: row.sla_breach_count,
    approvedToday: row.approved_today,
    rejectedToday: row.rejected_today,
    avgDiscount: row.avg_discount,
    discountTrend: row.discount_trend,
    dealsByUrgency: row.deals_by_urgency || {},
  }
}

export async function getDealApprovals(): Promise<DealApproval[]> {
  if (USE_MOCK_DATA) {
    return mock('dealApprovals')
  }

  return executeFoundrySQL<DealApproval>({
    query: `
      SELECT
        deal_id as dealId,
        opportunity_name as opportunityName,
        account_name as accountName,
        rep_name as repName,
        deal_value as dealValue,
        discount_pct as discountPct,
        discount_amount as discountAmount,
        approval_status as approvalStatus,
        submitted_at as submittedAt,
        urgency,
        approver_name as approverName,
        approval_level as approvalLevel,
        time_in_queue as timeInQueue,
        sla_status as slaStatus,
        discount_reason as discountReason,
        competitor,
        close_date as closeDate
      FROM \`/RevOps/DealDesk/approvals\`
      ORDER BY submitted_at DESC
    `,
  })
}

export async function getDiscountAnalysis(): Promise<DiscountAnalysis[]> {
  if (USE_MOCK_DATA) {
    return mock('discountAnalysis')
  }

  return executeFoundrySQL<DiscountAnalysis>({
    query: `
      SELECT
        segment,
        deal_count as dealCount,
        total_value as totalValue,
        avg_discount as avgDiscount,
        median_discount as medianDiscount,
        max_discount as maxDiscount,
        discount_0_10 as discount0_10,
        discount_10_20 as discount10_20,
        discount_20_plus as discount20Plus,
        win_rate_with_discount as winRateWithDiscount,
        win_rate_without_discount as winRateWithoutDiscount,
        avg_cycle_with_discount as avgCycleWithDiscount,
        avg_cycle_without_discount as avgCycleWithoutDiscount,
        trend
      FROM \`/RevOps/DealDesk/discount_analysis\`
      ORDER BY total_value DESC
    `,
  })
}
