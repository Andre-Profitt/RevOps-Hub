# /transforms/ingestion/salesforce_sync.py
"""
SALESFORCE DATA SYNC
====================
Transforms raw Salesforce exports into the RevOps canonical schema.
Supports both batch (daily/hourly) and incremental (CDC) sync patterns.

Input Sources:
- Salesforce Opportunity export (CSV, Parquet, or direct connector)
- Salesforce Account export
- Salesforce Contact export
- Salesforce Task/Event (activities)

Output:
- /RevOps/Staging/opportunities - Normalized opportunities
- /RevOps/Staging/accounts - Normalized accounts
- /RevOps/Staging/contacts - Normalized contacts
- /RevOps/Staging/activities - Normalized activities

Configuration:
- Set field mappings in /RevOps/Config/salesforce_field_map
- Set sync frequency via Foundry schedule
"""

from transforms.api import transform, Input, Output, configure
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, DateType, TimestampType, BooleanType
)


# =============================================================================
# CANONICAL SCHEMAS
# =============================================================================

OPPORTUNITY_SCHEMA = StructType([
    StructField("opportunity_id", StringType(), False),
    StructField("opportunity_name", StringType(), False),
    StructField("account_id", StringType(), True),
    StructField("account_name", StringType(), True),
    StructField("owner_id", StringType(), True),
    StructField("owner_name", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("stage_name", StringType(), True),
    StructField("probability", DoubleType(), True),
    StructField("close_date", DateType(), True),
    StructField("created_date", DateType(), True),
    StructField("forecast_category", StringType(), True),
    StructField("lead_source", StringType(), True),
    StructField("type", StringType(), True),
    StructField("description", StringType(), True),
    StructField("next_step", StringType(), True),
    StructField("competitor", StringType(), True),
    StructField("is_closed", BooleanType(), True),
    StructField("is_won", BooleanType(), True),
    StructField("last_activity_date", DateType(), True),
    StructField("last_modified_date", TimestampType(), True),
    # RevOps enrichment fields (calculated downstream)
    StructField("segment", StringType(), True),
    StructField("region", StringType(), True),
])

ACCOUNT_SCHEMA = StructType([
    StructField("account_id", StringType(), False),
    StructField("account_name", StringType(), False),
    StructField("industry", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("annual_revenue", DoubleType(), True),
    StructField("employee_count", IntegerType(), True),
    StructField("billing_country", StringType(), True),
    StructField("billing_state", StringType(), True),
    StructField("owner_id", StringType(), True),
    StructField("owner_name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("website", StringType(), True),
    StructField("created_date", DateType(), True),
    StructField("last_modified_date", TimestampType(), True),
])

CONTACT_SCHEMA = StructType([
    StructField("contact_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("title", StringType(), True),
    StructField("department", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("owner_id", StringType(), True),
    StructField("lead_source", StringType(), True),
    StructField("is_primary", BooleanType(), True),
    StructField("created_date", DateType(), True),
    StructField("last_modified_date", TimestampType(), True),
])

ACTIVITY_SCHEMA = StructType([
    StructField("activity_id", StringType(), False),
    StructField("activity_type", StringType(), False),  # Call, Email, Meeting, Task
    StructField("subject", StringType(), True),
    StructField("description", StringType(), True),
    StructField("activity_date", DateType(), True),
    StructField("duration_minutes", IntegerType(), True),
    StructField("owner_id", StringType(), True),
    StructField("owner_name", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("opportunity_id", StringType(), True),
    StructField("contact_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("priority", StringType(), True),
    StructField("is_completed", BooleanType(), True),
    StructField("created_date", TimestampType(), True),
])


# =============================================================================
# DEFAULT FIELD MAPPINGS (Salesforce -> RevOps)
# =============================================================================

DEFAULT_OPPORTUNITY_MAPPING = {
    "Id": "opportunity_id",
    "Name": "opportunity_name",
    "AccountId": "account_id",
    "Account.Name": "account_name",
    "OwnerId": "owner_id",
    "Owner.Name": "owner_name",
    "Amount": "amount",
    "StageName": "stage_name",
    "Probability": "probability",
    "CloseDate": "close_date",
    "CreatedDate": "created_date",
    "ForecastCategory": "forecast_category",
    "ForecastCategoryName": "forecast_category",  # Alternative field
    "LeadSource": "lead_source",
    "Type": "type",
    "Description": "description",
    "NextStep": "next_step",
    "Competitor__c": "competitor",  # Custom field example
    "IsClosed": "is_closed",
    "IsWon": "is_won",
    "LastActivityDate": "last_activity_date",
    "LastModifiedDate": "last_modified_date",
}

DEFAULT_ACCOUNT_MAPPING = {
    "Id": "account_id",
    "Name": "account_name",
    "Industry": "industry",
    "Type": "type",
    "AnnualRevenue": "annual_revenue",
    "NumberOfEmployees": "employee_count",
    "BillingCountry": "billing_country",
    "BillingState": "billing_state",
    "OwnerId": "owner_id",
    "Owner.Name": "owner_name",
    "Website": "website",
    "CreatedDate": "created_date",
    "LastModifiedDate": "last_modified_date",
}


# =============================================================================
# SYNC TRANSFORMS
# =============================================================================

@configure(profile=["DRIVER_MEMORY_LARGE"])
@transform(
    sf_opportunities=Input("/Salesforce/Raw/Opportunity"),
    sf_accounts=Input("/Salesforce/Raw/Account"),
    sf_users=Input("/Salesforce/Raw/User"),
    field_map=Input("/RevOps/Config/salesforce_field_map"),
    output=Output("/RevOps/Staging/opportunities")
)
def sync_opportunities(ctx, sf_opportunities, sf_accounts, sf_users, field_map, output):
    """
    Transform Salesforce Opportunity data to RevOps canonical schema.

    Handles:
    - Field mapping (configurable via field_map dataset)
    - Account name enrichment
    - Owner name enrichment
    - Segment derivation based on amount
    - Region derivation based on owner
    """

    opps = sf_opportunities.dataframe()
    accounts = sf_accounts.dataframe()
    users = sf_users.dataframe()

    # Try to load custom field mapping, fall back to defaults
    try:
        mapping_df = field_map.dataframe()
        custom_mapping = {row["sf_field"]: row["revops_field"] for row in mapping_df.collect()}
    except Exception:
        custom_mapping = DEFAULT_OPPORTUNITY_MAPPING

    # Apply field mappings
    mapped = opps
    for sf_field, revops_field in custom_mapping.items():
        if sf_field in opps.columns:
            mapped = mapped.withColumnRenamed(sf_field, revops_field)

    # Enrich with account name if not already present
    if "account_name" not in mapped.columns and "account_id" in mapped.columns:
        account_names = accounts.select(
            F.col("Id").alias("_acc_id"),
            F.col("Name").alias("account_name")
        )
        mapped = mapped.join(account_names, mapped.account_id == account_names._acc_id, "left").drop("_acc_id")

    # Enrich with owner name if not already present
    if "owner_name" not in mapped.columns and "owner_id" in mapped.columns:
        user_names = users.select(
            F.col("Id").alias("_user_id"),
            F.col("Name").alias("owner_name")
        )
        mapped = mapped.join(user_names, mapped.owner_id == user_names._user_id, "left").drop("_user_id")

    # Derive segment from amount
    enriched = mapped.withColumn(
        "segment",
        F.when(F.col("amount") >= 500000, "Enterprise")
        .when(F.col("amount") >= 100000, "Mid-Market")
        .when(F.col("amount") >= 25000, "SMB")
        .otherwise("Velocity")
    )

    # Derive region from owner (would use lookup table in production)
    enriched = enriched.withColumn(
        "region",
        F.when(F.col("owner_name").like("%West%"), "West")
        .when(F.col("owner_name").like("%East%"), "East")
        .otherwise("Central")
    )

    # Add sync metadata
    final = enriched.withColumn("_synced_at", F.current_timestamp())

    output.write_dataframe(final)


@transform(
    sf_accounts=Input("/Salesforce/Raw/Account"),
    sf_users=Input("/Salesforce/Raw/User"),
    output=Output("/RevOps/Staging/accounts")
)
def sync_accounts(ctx, sf_accounts, sf_users, output):
    """Transform Salesforce Account data to RevOps canonical schema."""

    accounts = sf_accounts.dataframe()
    users = sf_users.dataframe()

    # Apply default mappings
    mapped = accounts
    for sf_field, revops_field in DEFAULT_ACCOUNT_MAPPING.items():
        if sf_field in accounts.columns and sf_field != revops_field:
            mapped = mapped.withColumnRenamed(sf_field, revops_field)

    # Enrich with owner name
    if "owner_name" not in mapped.columns and "owner_id" in mapped.columns:
        user_names = users.select(
            F.col("Id").alias("_user_id"),
            F.col("Name").alias("owner_name")
        )
        mapped = mapped.join(user_names, mapped.owner_id == user_names._user_id, "left").drop("_user_id")

    # Derive segment from employee count and revenue
    enriched = mapped.withColumn(
        "segment",
        F.when(
            (F.col("annual_revenue") >= 100000000) | (F.col("employee_count") >= 1000),
            "Enterprise"
        ).when(
            (F.col("annual_revenue") >= 10000000) | (F.col("employee_count") >= 100),
            "Mid-Market"
        ).otherwise("SMB")
    )

    final = enriched.withColumn("_synced_at", F.current_timestamp())
    output.write_dataframe(final)


@transform(
    sf_contacts=Input("/Salesforce/Raw/Contact"),
    output=Output("/RevOps/Staging/contacts")
)
def sync_contacts(ctx, sf_contacts, output):
    """Transform Salesforce Contact data to RevOps canonical schema."""

    contacts = sf_contacts.dataframe()

    # Map standard fields
    mapped = contacts.select(
        F.col("Id").alias("contact_id"),
        F.col("FirstName").alias("first_name"),
        F.col("LastName").alias("last_name"),
        F.col("Email").alias("email"),
        F.col("Phone").alias("phone"),
        F.col("Title").alias("title"),
        F.col("Department").alias("department"),
        F.col("AccountId").alias("account_id"),
        F.col("OwnerId").alias("owner_id"),
        F.col("LeadSource").alias("lead_source"),
        F.lit(False).alias("is_primary"),  # Needs business logic
        F.col("CreatedDate").alias("created_date"),
        F.col("LastModifiedDate").alias("last_modified_date"),
    )

    final = mapped.withColumn("_synced_at", F.current_timestamp())
    output.write_dataframe(final)


@transform(
    sf_tasks=Input("/Salesforce/Raw/Task"),
    sf_events=Input("/Salesforce/Raw/Event"),
    sf_users=Input("/Salesforce/Raw/User"),
    output=Output("/RevOps/Staging/activities")
)
def sync_activities(ctx, sf_tasks, sf_events, sf_users, output):
    """
    Transform Salesforce Task and Event data to unified activities.

    Combines:
    - Tasks (calls, emails, to-dos)
    - Events (meetings, demos)
    """

    tasks = sf_tasks.dataframe()
    events = sf_events.dataframe()
    users = sf_users.dataframe()

    # Transform Tasks
    task_activities = tasks.select(
        F.col("Id").alias("activity_id"),
        F.when(F.col("TaskSubtype") == "Call", "Call")
        .when(F.col("TaskSubtype") == "Email", "Email")
        .otherwise("Task").alias("activity_type"),
        F.col("Subject").alias("subject"),
        F.col("Description").alias("description"),
        F.coalesce(F.col("ActivityDate"), F.col("CreatedDate")).alias("activity_date"),
        F.col("CallDurationInSeconds").cast("int").alias("duration_minutes"),
        F.col("OwnerId").alias("owner_id"),
        F.col("AccountId").alias("account_id"),
        F.col("WhatId").alias("opportunity_id"),  # WhatId is typically Opportunity
        F.col("WhoId").alias("contact_id"),
        F.col("Status").alias("status"),
        F.col("Priority").alias("priority"),
        F.when(F.col("Status") == "Completed", True).otherwise(False).alias("is_completed"),
        F.col("CreatedDate").alias("created_date"),
    )

    # Transform Events
    event_activities = events.select(
        F.col("Id").alias("activity_id"),
        F.when(F.col("Subject").like("%Demo%"), "Demo")
        .otherwise("Meeting").alias("activity_type"),
        F.col("Subject").alias("subject"),
        F.col("Description").alias("description"),
        F.col("ActivityDate").alias("activity_date"),
        F.col("DurationInMinutes").alias("duration_minutes"),
        F.col("OwnerId").alias("owner_id"),
        F.col("AccountId").alias("account_id"),
        F.col("WhatId").alias("opportunity_id"),
        F.col("WhoId").alias("contact_id"),
        F.lit("Completed").alias("status"),
        F.lit("Normal").alias("priority"),
        F.lit(True).alias("is_completed"),
        F.col("CreatedDate").alias("created_date"),
    )

    # Union tasks and events
    all_activities = task_activities.union(event_activities)

    # Enrich with owner name
    user_names = users.select(
        F.col("Id").alias("_user_id"),
        F.col("Name").alias("owner_name")
    )
    enriched = all_activities.join(user_names, all_activities.owner_id == user_names._user_id, "left").drop("_user_id")

    final = enriched.withColumn("_synced_at", F.current_timestamp())
    output.write_dataframe(final)


# =============================================================================
# INCREMENTAL SYNC (CDC PATTERN)
# =============================================================================

@transform(
    sf_opportunities=Input("/Salesforce/Raw/Opportunity"),
    existing_staging=Input("/RevOps/Staging/opportunities"),
    output=Output("/RevOps/Staging/opportunities")
)
def sync_opportunities_incremental(ctx, sf_opportunities, existing_staging, output):
    """
    Incremental sync using LastModifiedDate for CDC pattern.

    Only processes records modified since last sync.
    """

    new_data = sf_opportunities.dataframe()
    existing = existing_staging.dataframe()

    # Get last sync timestamp
    last_sync = existing.agg(F.max("_synced_at")).collect()[0][0]

    if last_sync:
        # Filter to only changed records
        incremental = new_data.filter(F.col("LastModifiedDate") > last_sync)
    else:
        incremental = new_data

    # Process incremental records (same mapping logic as full sync)
    # ... mapping code here ...

    # Merge: UPSERT pattern
    merged = existing.alias("e").join(
        incremental.alias("n"),
        F.col("e.opportunity_id") == F.col("n.opportunity_id"),
        "outer"
    ).select(
        F.coalesce(F.col("n.opportunity_id"), F.col("e.opportunity_id")).alias("opportunity_id"),
        # ... select coalesced fields, preferring new data ...
    )

    output.write_dataframe(merged)
