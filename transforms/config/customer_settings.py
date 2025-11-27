# /transforms/config/customer_settings.py
"""
CUSTOMER CONFIGURATION MANAGEMENT
=================================
Generates customer-specific configuration and settings.
Used by usage metering, alerting, and ops dashboard transforms.

Output:
- /RevOps/Config/customer_settings
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType


@transform(
    accounts=Input("/RevOps/Scenario/accounts"),
    output=Output("/RevOps/Config/customer_settings")
)
def generate_customer_settings(ctx, accounts, output):
    """Generate customer configuration settings based on account data."""

    accts = accounts.dataframe()

    # Derive customer settings from account attributes
    settings = accts.select(
        F.col("account_id").alias("customer_id"),
        F.col("account_name").alias("customer_name"),
    ).withColumn(
        # Derive tier from segment (simplified)
        "tier",
        F.when(F.rand() < 0.1, "Enterprise")
        .when(F.rand() < 0.4, "Growth")
        .otherwise("Starter")
    ).withColumn(
        # Billing period
        "billing_period",
        F.when(F.col("tier") == "Enterprise", "Annual")
        .when(F.col("tier") == "Growth", "Annual")
        .otherwise("Monthly")
    ).withColumn(
        # User limits by tier
        "user_limit",
        F.when(F.col("tier") == "Enterprise", F.lit(None).cast("int"))  # Unlimited
        .when(F.col("tier") == "Growth", 50)
        .otherwise(10)
    ).withColumn(
        # Opportunity limits
        "opportunity_limit",
        F.when(F.col("tier") == "Enterprise", F.lit(None).cast("int"))
        .when(F.col("tier") == "Growth", 5000)
        .otherwise(500)
    ).withColumn(
        # API call limits (per month)
        "api_call_limit",
        F.when(F.col("tier") == "Enterprise", F.lit(None).cast("int"))
        .when(F.col("tier") == "Growth", 100000)
        .otherwise(10000)
    ).withColumn(
        # Compute hour limits (per month)
        "compute_hour_limit",
        F.when(F.col("tier") == "Enterprise", F.lit(None).cast("int"))
        .when(F.col("tier") == "Growth", 100)
        .otherwise(10)
    ).withColumn(
        # Data refresh frequency (hours)
        "refresh_frequency_hours",
        F.when(F.col("tier") == "Enterprise", 0.25)  # 15 min
        .when(F.col("tier") == "Growth", 1)
        .otherwise(4)
    ).withColumn(
        # SLA target (%)
        "sla_target",
        F.when(F.col("tier") == "Enterprise", 99.9)
        .when(F.col("tier") == "Growth", 99.5)
        .otherwise(99.0)
    ).withColumn(
        # Features enabled
        "features_enabled",
        F.when(F.col("tier") == "Enterprise",
               F.array(F.lit("core"), F.lit("forecasting"), F.lit("ai_predictions"),
                      F.lit("custom_reports"), F.lit("api_access"), F.lit("sso")))
        .when(F.col("tier") == "Growth",
               F.array(F.lit("core"), F.lit("forecasting"), F.lit("ai_predictions")))
        .otherwise(F.array(F.lit("core"), F.lit("forecasting")))
    ).withColumn(
        "alert_notifications_enabled", F.lit(True)
    ).withColumn(
        "notification_email", F.lit(None).cast("string")
    ).withColumn(
        "contract_start_date",
        F.date_sub(F.current_date(), (F.rand() * 365).cast("int"))
    ).withColumn(
        "contract_end_date",
        F.date_add(F.col("contract_start_date"), 365)
    ).withColumn(
        "created_at", F.current_timestamp()
    ).withColumn(
        "updated_at", F.current_timestamp()
    )

    output.write_dataframe(settings)
