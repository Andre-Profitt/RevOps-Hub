/**
 * Staging & Sample Data fetchers
 * Data from HubSpot sync, sample/scenario generators
 */

import { USE_MOCK_DATA, executeFoundrySQL } from './core'

// ============================================================================
// HUBSPOT STAGING DATA
// ============================================================================

export async function getHubspotOpportunities(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        deal_id as dealId,
        deal_name as dealName,
        account_id as accountId,
        account_name as accountName,
        owner_id as ownerId,
        owner_name as ownerName,
        amount,
        stage_name as stageName,
        pipeline_name as pipelineName,
        close_date as closeDate,
        create_date as createDate,
        deal_type as dealType,
        lead_source as leadSource,
        hubspot_score as hubspotScore,
        last_activity_date as lastActivityDate,
        sync_timestamp as syncTimestamp
      FROM \`/RevOps/Staging/hubspot_opportunities\`
      ORDER BY amount DESC
    `,
  })
}

export async function getHubspotAccounts(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        company_id as companyId,
        company_name as companyName,
        domain,
        industry,
        employee_count as employeeCount,
        annual_revenue as annualRevenue,
        owner_id as ownerId,
        owner_name as ownerName,
        lifecycle_stage as lifecycleStage,
        lead_status as leadStatus,
        city,
        state,
        country,
        create_date as createDate,
        last_activity_date as lastActivityDate,
        sync_timestamp as syncTimestamp
      FROM \`/RevOps/Staging/hubspot_accounts\`
      ORDER BY annual_revenue DESC
    `,
  })
}

export async function getHubspotContacts(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        contact_id as contactId,
        email,
        first_name as firstName,
        last_name as lastName,
        company_id as companyId,
        company_name as companyName,
        job_title as jobTitle,
        phone,
        lifecycle_stage as lifecycleStage,
        lead_status as leadStatus,
        owner_id as ownerId,
        owner_name as ownerName,
        create_date as createDate,
        last_activity_date as lastActivityDate,
        sync_timestamp as syncTimestamp
      FROM \`/RevOps/Staging/hubspot_contacts\`
      ORDER BY last_activity_date DESC
    `,
  })
}

export async function getHubspotActivities(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        activity_id as activityId,
        activity_type as activityType,
        contact_id as contactId,
        company_id as companyId,
        deal_id as dealId,
        owner_id as ownerId,
        owner_name as ownerName,
        subject,
        body,
        activity_date as activityDate,
        duration_minutes as durationMinutes,
        outcome,
        sync_timestamp as syncTimestamp
      FROM \`/RevOps/Staging/hubspot_activities\`
      ORDER BY activity_date DESC
      LIMIT 1000
    `,
  })
}

// ============================================================================
// SAMPLE / SCENARIO DATA
// ============================================================================

export async function getSampleAccounts(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        account_id as accountId,
        account_name as accountName,
        industry,
        employee_count as employeeCount,
        annual_revenue as annualRevenue,
        segment,
        region,
        territory,
        owner_id as ownerId,
        owner_name as ownerName,
        created_date as createdDate,
        last_activity_date as lastActivityDate,
        health_score as healthScore
      FROM \`/RevOps/Sample/accounts\`
      ORDER BY annual_revenue DESC
    `,
  })
}

export async function getSampleActivities(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        activity_id as activityId,
        activity_type as activityType,
        account_id as accountId,
        opportunity_id as opportunityId,
        contact_id as contactId,
        owner_id as ownerId,
        owner_name as ownerName,
        subject,
        description,
        activity_date as activityDate,
        duration_minutes as durationMinutes,
        outcome,
        sentiment_score as sentimentScore
      FROM \`/RevOps/Sample/activities\`
      ORDER BY activity_date DESC
      LIMIT 1000
    `,
  })
}

export async function getScenarioAccounts(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        account_id as accountId,
        account_name as accountName,
        industry,
        employee_count as employeeCount,
        annual_revenue as annualRevenue,
        segment,
        region,
        territory,
        owner_id as ownerId,
        owner_name as ownerName,
        created_date as createdDate
      FROM \`/RevOps/Scenario/accounts\`
      ORDER BY annual_revenue DESC
    `,
  })
}

export async function getScenarioActivities(): Promise<any[]> {
  if (USE_MOCK_DATA) {
    return []
  }

  return executeFoundrySQL<any>({
    query: `
      SELECT
        activity_id as activityId,
        activity_type as activityType,
        account_id as accountId,
        opportunity_id as opportunityId,
        contact_id as contactId,
        owner_id as ownerId,
        owner_name as ownerName,
        subject,
        activity_date as activityDate,
        duration_minutes as durationMinutes,
        outcome
      FROM \`/RevOps/Scenario/activities\`
      ORDER BY activity_date DESC
      LIMIT 1000
    `,
  })
}
