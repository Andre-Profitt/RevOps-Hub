# /transforms/config/salesforce_field_map.py
"""
SALESFORCE FIELD MAPPING CONFIGURATION
======================================
Defines the mapping between Salesforce fields and RevOps canonical schema.
Used by the Salesforce sync transform to normalize data.

Output:
- /RevOps/Config/salesforce_field_map
"""

from transforms.api import transform, Output
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType


# Standard field mappings for common Salesforce objects
OPPORTUNITY_MAPPINGS = [
    ("Id", "opportunity_id", "string", True),
    ("Name", "opportunity_name", "string", True),
    ("AccountId", "account_id", "string", True),
    ("OwnerId", "owner_id", "string", True),
    ("Amount", "amount", "double", True),
    ("StageName", "stage_name", "string", True),
    ("CloseDate", "close_date", "date", True),
    ("CreatedDate", "created_date", "timestamp", False),
    ("LastModifiedDate", "last_modified_date", "timestamp", False),
    ("ForecastCategory", "forecast_category", "string", False),
    ("Probability", "probability", "double", False),
    ("LeadSource", "lead_source", "string", False),
    ("Type", "opportunity_type", "string", False),
    ("IsClosed", "is_closed", "boolean", False),
    ("IsWon", "is_won", "boolean", False),
    ("NextStep", "next_step", "string", False),
    ("Description", "description", "string", False),
]

ACCOUNT_MAPPINGS = [
    ("Id", "account_id", "string", True),
    ("Name", "account_name", "string", True),
    ("Industry", "industry", "string", False),
    ("AnnualRevenue", "annual_revenue", "double", False),
    ("NumberOfEmployees", "employee_count", "integer", False),
    ("BillingCountry", "billing_country", "string", False),
    ("BillingState", "billing_state", "string", False),
    ("Website", "website", "string", False),
    ("OwnerId", "owner_id", "string", True),
    ("CreatedDate", "created_date", "timestamp", False),
    ("LastModifiedDate", "last_modified_date", "timestamp", False),
    ("Type", "account_type", "string", False),
]

CONTACT_MAPPINGS = [
    ("Id", "contact_id", "string", True),
    ("Email", "email", "string", True),
    ("FirstName", "first_name", "string", False),
    ("LastName", "last_name", "string", True),
    ("AccountId", "account_id", "string", True),
    ("Title", "job_title", "string", False),
    ("Phone", "phone", "string", False),
    ("OwnerId", "owner_id", "string", False),
    ("CreatedDate", "created_date", "timestamp", False),
    ("LastModifiedDate", "last_modified_date", "timestamp", False),
]

ACTIVITY_MAPPINGS = [
    ("Id", "activity_id", "string", True),
    ("Subject", "subject", "string", False),
    ("Description", "description", "string", False),
    ("ActivityDate", "activity_date", "date", True),
    ("OwnerId", "owner_id", "string", True),
    ("WhatId", "opportunity_id", "string", False),
    ("WhoId", "contact_id", "string", False),
    ("AccountId", "account_id", "string", False),
    ("Status", "status", "string", False),
    ("Type", "activity_type", "string", False),
    ("DurationInMinutes", "duration_minutes", "integer", False),
    ("CreatedDate", "created_date", "timestamp", False),
]


@transform(
    output=Output("/RevOps/Config/salesforce_field_map")
)
def generate_salesforce_field_map(ctx, output):
    """Generate Salesforce to RevOps field mapping configuration."""

    spark = SparkSession.builder.getOrCreate()

    # Combine all mappings
    all_mappings = []
    for sf_field, revops_field, data_type, required in OPPORTUNITY_MAPPINGS:
        all_mappings.append(("Opportunity", sf_field, revops_field, data_type, required))
    for sf_field, revops_field, data_type, required in ACCOUNT_MAPPINGS:
        all_mappings.append(("Account", sf_field, revops_field, data_type, required))
    for sf_field, revops_field, data_type, required in CONTACT_MAPPINGS:
        all_mappings.append(("Contact", sf_field, revops_field, data_type, required))
    for sf_field, revops_field, data_type, required in ACTIVITY_MAPPINGS:
        all_mappings.append(("Task", sf_field, revops_field, data_type, required))

    schema = StructType([
        StructField("object_type", StringType(), False),
        StructField("source_field", StringType(), False),
        StructField("target_field", StringType(), False),
        StructField("data_type", StringType(), False),
        StructField("is_required", BooleanType(), False),
    ])

    df = spark.createDataFrame(all_mappings, schema)

    output.write_dataframe(df)
