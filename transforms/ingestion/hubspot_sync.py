# /transforms/ingestion/hubspot_sync.py
"""
HUBSPOT DATA SYNC
=================
Transforms HubSpot CRM and marketing data into RevOps canonical schema.

Input Sources:
- HubSpot Deals (opportunities)
- HubSpot Companies (accounts)
- HubSpot Contacts
- HubSpot Engagements (activities)
- HubSpot Marketing Events (campaigns, emails)

Output:
- /RevOps/Staging/opportunities (merged with Salesforce if both present)
- /RevOps/Staging/accounts
- /RevOps/Staging/contacts
- /RevOps/Staging/marketing_activities
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, TimestampType


# =============================================================================
# HUBSPOT FIELD MAPPINGS
# =============================================================================

HUBSPOT_DEAL_MAPPING = {
    "dealId": "opportunity_id",
    "dealname": "opportunity_name",
    "associatedCompanyIds": "account_id",
    "hubspot_owner_id": "owner_id",
    "amount": "amount",
    "dealstage": "stage_name",
    "closedate": "close_date",
    "createdate": "created_date",
    "pipeline": "pipeline",
    "deal_currency_code": "currency",
    "hs_lastmodifieddate": "last_modified_date",
}

HUBSPOT_COMPANY_MAPPING = {
    "companyId": "account_id",
    "name": "account_name",
    "industry": "industry",
    "annualrevenue": "annual_revenue",
    "numberofemployees": "employee_count",
    "country": "billing_country",
    "state": "billing_state",
    "website": "website",
    "hubspot_owner_id": "owner_id",
    "createdate": "created_date",
    "hs_lastmodifieddate": "last_modified_date",
}

# HubSpot stage to RevOps stage mapping
HUBSPOT_STAGE_MAPPING = {
    "appointmentscheduled": "Discovery",
    "qualifiedtobuy": "Solution Design",
    "presentationscheduled": "Proposal",
    "decisionmakerboughtin": "Negotiation",
    "contractsent": "Verbal Commit",
    "closedwon": "Closed Won",
    "closedlost": "Closed Lost",
}


@transform(
    hs_deals=Input("/HubSpot/Raw/Deals"),
    hs_companies=Input("/HubSpot/Raw/Companies"),
    hs_owners=Input("/HubSpot/Raw/Owners"),
    output=Output("/RevOps/Staging/hubspot_opportunities")
)
def sync_hubspot_deals(ctx, hs_deals, hs_companies, hs_owners, output):
    """Transform HubSpot Deals to RevOps opportunity schema."""

    deals = hs_deals.dataframe()
    companies = hs_companies.dataframe()
    owners = hs_owners.dataframe()

    # Apply field mappings
    mapped = deals.select(
        F.col("dealId").cast("string").alias("opportunity_id"),
        F.col("dealname").alias("opportunity_name"),
        F.col("associatedCompanyIds").getItem(0).cast("string").alias("account_id"),
        F.col("hubspot_owner_id").cast("string").alias("owner_id"),
        F.col("amount").cast("double").alias("amount"),
        F.col("dealstage").alias("hs_stage"),
        F.to_date(F.from_unixtime(F.col("closedate") / 1000)).alias("close_date"),
        F.to_date(F.from_unixtime(F.col("createdate") / 1000)).alias("created_date"),
        F.col("pipeline").alias("pipeline"),
        F.to_timestamp(F.from_unixtime(F.col("hs_lastmodifieddate") / 1000)).alias("last_modified_date"),
    )

    # Map HubSpot stages to RevOps stages
    stage_mapped = mapped.withColumn(
        "stage_name",
        F.when(F.col("hs_stage") == "appointmentscheduled", "Discovery")
        .when(F.col("hs_stage") == "qualifiedtobuy", "Solution Design")
        .when(F.col("hs_stage") == "presentationscheduled", "Proposal")
        .when(F.col("hs_stage") == "decisionmakerboughtin", "Negotiation")
        .when(F.col("hs_stage") == "contractsent", "Verbal Commit")
        .when(F.col("hs_stage") == "closedwon", "Closed Won")
        .when(F.col("hs_stage") == "closedlost", "Closed Lost")
        .otherwise("Prospecting")
    ).drop("hs_stage")

    # Enrich with company name
    company_names = companies.select(
        F.col("companyId").cast("string").alias("_comp_id"),
        F.col("name").alias("account_name")
    )
    with_company = stage_mapped.join(
        company_names,
        stage_mapped.account_id == company_names._comp_id,
        "left"
    ).drop("_comp_id")

    # Enrich with owner name
    owner_names = owners.select(
        F.col("ownerId").cast("string").alias("_owner_id"),
        F.concat(F.col("firstName"), F.lit(" "), F.col("lastName")).alias("owner_name")
    )
    with_owner = with_company.join(
        owner_names,
        with_company.owner_id == owner_names._owner_id,
        "left"
    ).drop("_owner_id")

    # Add standard RevOps fields
    enriched = with_owner.withColumn(
        "forecast_category",
        F.when(F.col("stage_name") == "Verbal Commit", "Commit")
        .when(F.col("stage_name").isin("Negotiation", "Proposal"), "Best Case")
        .when(F.col("stage_name") == "Closed Won", "Closed")
        .when(F.col("stage_name") == "Closed Lost", "Omitted")
        .otherwise("Pipeline")
    ).withColumn(
        "is_closed",
        F.col("stage_name").isin("Closed Won", "Closed Lost")
    ).withColumn(
        "is_won",
        F.col("stage_name") == "Closed Won"
    ).withColumn(
        "segment",
        F.when(F.col("amount") >= 500000, "Enterprise")
        .when(F.col("amount") >= 100000, "Mid-Market")
        .otherwise("SMB")
    ).withColumn(
        "source_system",
        F.lit("HubSpot")
    ).withColumn(
        "_synced_at",
        F.current_timestamp()
    )

    output.write_dataframe(enriched)


@transform(
    hs_companies=Input("/HubSpot/Raw/Companies"),
    hs_owners=Input("/HubSpot/Raw/Owners"),
    output=Output("/RevOps/Staging/hubspot_accounts")
)
def sync_hubspot_companies(ctx, hs_companies, hs_owners, output):
    """Transform HubSpot Companies to RevOps account schema."""

    companies = hs_companies.dataframe()
    owners = hs_owners.dataframe()

    mapped = companies.select(
        F.col("companyId").cast("string").alias("account_id"),
        F.col("name").alias("account_name"),
        F.col("industry").alias("industry"),
        F.col("annualrevenue").cast("double").alias("annual_revenue"),
        F.col("numberofemployees").cast("int").alias("employee_count"),
        F.col("country").alias("billing_country"),
        F.col("state").alias("billing_state"),
        F.col("hubspot_owner_id").cast("string").alias("owner_id"),
        F.col("website").alias("website"),
        F.to_date(F.from_unixtime(F.col("createdate") / 1000)).alias("created_date"),
        F.to_timestamp(F.from_unixtime(F.col("hs_lastmodifieddate") / 1000)).alias("last_modified_date"),
    )

    # Enrich with owner name
    owner_names = owners.select(
        F.col("ownerId").cast("string").alias("_owner_id"),
        F.concat(F.col("firstName"), F.lit(" "), F.col("lastName")).alias("owner_name")
    )
    with_owner = mapped.join(owner_names, mapped.owner_id == owner_names._owner_id, "left").drop("_owner_id")

    # Derive segment
    enriched = with_owner.withColumn(
        "segment",
        F.when(
            (F.col("annual_revenue") >= 100000000) | (F.col("employee_count") >= 1000),
            "Enterprise"
        ).when(
            (F.col("annual_revenue") >= 10000000) | (F.col("employee_count") >= 100),
            "Mid-Market"
        ).otherwise("SMB")
    ).withColumn(
        "source_system",
        F.lit("HubSpot")
    ).withColumn(
        "_synced_at",
        F.current_timestamp()
    )

    output.write_dataframe(enriched)


@transform(
    hs_engagements=Input("/HubSpot/Raw/Engagements"),
    hs_owners=Input("/HubSpot/Raw/Owners"),
    output=Output("/RevOps/Staging/hubspot_activities")
)
def sync_hubspot_engagements(ctx, hs_engagements, hs_owners, output):
    """Transform HubSpot Engagements to RevOps activity schema."""

    engagements = hs_engagements.dataframe()
    owners = hs_owners.dataframe()

    mapped = engagements.select(
        F.col("engagementId").cast("string").alias("activity_id"),
        F.when(F.col("type") == "CALL", "Call")
        .when(F.col("type") == "EMAIL", "Email")
        .when(F.col("type") == "MEETING", "Meeting")
        .when(F.col("type") == "NOTE", "Note")
        .otherwise("Task").alias("activity_type"),
        F.col("metadata.subject").alias("subject"),
        F.col("metadata.body").alias("description"),
        F.to_date(F.from_unixtime(F.col("timestamp") / 1000)).alias("activity_date"),
        F.col("metadata.durationMilliseconds").cast("int").alias("duration_minutes"),
        F.col("ownerId").cast("string").alias("owner_id"),
        F.col("associations.companyIds").getItem(0).cast("string").alias("account_id"),
        F.col("associations.dealIds").getItem(0).cast("string").alias("opportunity_id"),
        F.col("associations.contactIds").getItem(0).cast("string").alias("contact_id"),
        F.lit("Completed").alias("status"),
        F.lit(True).alias("is_completed"),
        F.to_timestamp(F.from_unixtime(F.col("timestamp") / 1000)).alias("created_date"),
    )

    # Convert duration from ms to minutes
    with_duration = mapped.withColumn(
        "duration_minutes",
        F.when(F.col("duration_minutes").isNotNull(),
               F.round(F.col("duration_minutes") / 60000, 0).cast("int"))
        .otherwise(F.lit(None))
    )

    # Enrich with owner name
    owner_names = owners.select(
        F.col("ownerId").cast("string").alias("_owner_id"),
        F.concat(F.col("firstName"), F.lit(" "), F.col("lastName")).alias("owner_name")
    )
    enriched = with_duration.join(owner_names, with_duration.owner_id == owner_names._owner_id, "left").drop("_owner_id")

    final = enriched.withColumn(
        "source_system",
        F.lit("HubSpot")
    ).withColumn(
        "_synced_at",
        F.current_timestamp()
    )

    output.write_dataframe(final)


# =============================================================================
# CONTACTS
# =============================================================================

@transform(
    hs_contacts=Input("/HubSpot/Raw/Contacts"),
    hs_companies=Input("/HubSpot/Raw/Companies"),
    hs_owners=Input("/HubSpot/Raw/Owners"),
    output=Output("/RevOps/Staging/hubspot_contacts")
)
def sync_hubspot_contacts(ctx, hs_contacts, hs_companies, hs_owners, output):
    """Transform HubSpot Contacts to RevOps contact schema."""

    contacts = hs_contacts.dataframe()
    companies = hs_companies.dataframe()
    owners = hs_owners.dataframe()

    mapped = contacts.select(
        F.col("vid").cast("string").alias("contact_id"),
        F.col("properties.email.value").alias("email"),
        F.col("properties.firstname.value").alias("first_name"),
        F.col("properties.lastname.value").alias("last_name"),
        F.col("properties.jobtitle.value").alias("job_title"),
        F.col("properties.phone.value").alias("phone"),
        F.col("properties.hubspot_owner_id.value").cast("string").alias("owner_id"),
        F.col("properties.lifecyclestage.value").alias("lifecycle_stage"),
        F.col("properties.hs_lead_status.value").alias("lead_status"),
        F.col("associated-company.companyId").cast("string").alias("company_id"),
        F.to_date(F.from_unixtime(F.col("addedAt") / 1000)).alias("created_date"),
        F.to_timestamp(F.from_unixtime(
            F.coalesce(F.col("properties.lastmodifieddate.value"), F.col("addedAt")) / 1000
        )).alias("last_modified_date"),
    )

    # Enrich with company name
    company_names = companies.select(
        F.col("companyId").cast("string").alias("_comp_id"),
        F.col("name").alias("company_name")
    )
    with_company = mapped.join(
        company_names,
        mapped.company_id == company_names._comp_id,
        "left"
    ).drop("_comp_id")

    # Enrich with owner name
    owner_names = owners.select(
        F.col("ownerId").cast("string").alias("_owner_id"),
        F.concat(F.col("firstName"), F.lit(" "), F.col("lastName")).alias("owner_name")
    )
    with_owner = with_company.join(
        owner_names,
        with_company.owner_id == owner_names._owner_id,
        "left"
    ).drop("_owner_id")

    enriched = with_owner.withColumn(
        "source_system",
        F.lit("HubSpot")
    ).withColumn(
        "_synced_at",
        F.current_timestamp()
    )

    output.write_dataframe(enriched)


# =============================================================================
# MARKETING DATA
# =============================================================================

@transform(
    hs_email_events=Input("/HubSpot/Raw/EmailEvents"),
    hs_campaigns=Input("/HubSpot/Raw/Campaigns"),
    output=Output("/RevOps/Staging/marketing_activities")
)
def sync_marketing_activities(ctx, hs_email_events, hs_campaigns, output):
    """Transform HubSpot marketing data for telemetry analysis."""

    emails = hs_email_events.dataframe()

    marketing = emails.select(
        F.col("id").cast("string").alias("event_id"),
        F.lit("Email Campaign").alias("activity_type"),
        F.col("emailCampaignId").cast("string").alias("campaign_id"),
        F.col("recipient").alias("contact_email"),
        F.col("type").alias("event_type"),  # SENT, DELIVERED, OPEN, CLICK, BOUNCE
        F.to_timestamp(F.from_unixtime(F.col("created") / 1000)).alias("event_timestamp"),
        F.col("portalId").cast("string").alias("portal_id"),
    ).withColumn(
        "is_engagement",
        F.col("event_type").isin("OPEN", "CLICK")
    ).withColumn(
        "source_system",
        F.lit("HubSpot")
    ).withColumn(
        "_synced_at",
        F.current_timestamp()
    )

    output.write_dataframe(marketing)
