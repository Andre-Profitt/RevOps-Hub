# /transforms/commercial/billing_plans.py
"""
BILLING PLANS
=============
Defines billing plans and manages tenant plan assignments.

Plan tiers:
- Starter: Basic features, limited users/opportunities
- Growth: Advanced analytics, API access
- Enterprise: Full platform, unlimited, SSO, custom

Outputs:
- /RevOps/Commercial/billing_plans: Plan definitions
- /RevOps/Commercial/tenant_plans: Tenant plan assignments
"""

from transforms.api import transform, Input, Output
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    TimestampType, BooleanType, DoubleType, ArrayType
)
from datetime import datetime, timedelta


# =============================================================================
# PLAN DEFINITIONS
# =============================================================================

BILLING_PLANS = [
    {
        "plan_id": "starter",
        "name": "Starter",
        "description": "Essential pipeline visibility for small teams",
        "monthly_price": 499,
        "annual_price": 4990,
        "max_users": 10,
        "max_opportunities": 10000,
        "max_api_calls": 0,
        "max_webhooks": 0,
        "features": [
            "pipeline_health",
            "pipeline_hygiene",
            "forecast_summary",
            "basic_alerts",
        ],
        "support_level": "email",
        "data_retention_days": 365,
        "is_active": True,
    },
    {
        "plan_id": "growth",
        "name": "Growth",
        "description": "Advanced analytics and AI for scaling teams",
        "monthly_price": 1499,
        "annual_price": 14990,
        "max_users": 50,
        "max_opportunities": 50000,
        "max_api_calls": 100000,
        "max_webhooks": 5,
        "features": [
            "pipeline_health",
            "pipeline_hygiene",
            "forecast_summary",
            "deal_predictions",
            "coaching_insights",
            "rep_performance",
            "activity_analytics",
            "api_access",
            "webhooks",
            "slack_integration",
        ],
        "support_level": "priority",
        "data_retention_days": 730,
        "is_active": True,
    },
    {
        "plan_id": "enterprise",
        "name": "Enterprise",
        "description": "Full platform with unlimited scale and customization",
        "monthly_price": 4999,
        "annual_price": 49990,
        "max_users": 500,
        "max_opportunities": 500000,
        "max_api_calls": 1000000,
        "max_webhooks": 50,
        "features": [
            "pipeline_health",
            "pipeline_hygiene",
            "forecast_summary",
            "deal_predictions",
            "coaching_insights",
            "rep_performance",
            "activity_analytics",
            "scenario_modeling",
            "territory_planning",
            "custom_integrations",
            "api_access",
            "webhooks",
            "sso_enabled",
            "white_labeling",
            "data_export",
            "dedicated_support",
            "custom_training",
        ],
        "support_level": "dedicated",
        "data_retention_days": 1095,
        "is_active": True,
    },
]


@transform(
    output=Output("/RevOps/Commercial/billing_plans"),
)
def define_billing_plans(ctx, output):
    """
    Output current billing plan definitions.
    """
    spark = ctx.spark_session
    now = datetime.utcnow()

    # Convert to DataFrame
    plans_data = []
    for plan in BILLING_PLANS:
        plans_data.append({
            "plan_id": plan["plan_id"],
            "name": plan["name"],
            "description": plan["description"],
            "monthly_price": plan["monthly_price"],
            "annual_price": plan["annual_price"],
            "max_users": plan["max_users"],
            "max_opportunities": plan["max_opportunities"],
            "max_api_calls": plan["max_api_calls"],
            "max_webhooks": plan["max_webhooks"],
            "features": ",".join(plan["features"]),
            "support_level": plan["support_level"],
            "data_retention_days": plan["data_retention_days"],
            "is_active": plan["is_active"],
            "updated_at": now,
        })

    plans_df = spark.createDataFrame(plans_data)

    output.write_dataframe(plans_df)


# =============================================================================
# TENANT PLAN ASSIGNMENTS
# =============================================================================

@transform(
    customer_config=Input("/RevOps/Config/customer_settings"),
    billing_plans=Input("/RevOps/Commercial/billing_plans"),
    subscription_events=Input("/RevOps/Audit/subscription_events"),
    output=Output("/RevOps/Commercial/tenant_plans"),
)
def assign_tenant_plans(
    ctx,
    customer_config: DataFrame,
    billing_plans: DataFrame,
    subscription_events: DataFrame,
    output,
):
    """
    Track tenant plan assignments and subscription status.
    """
    spark = ctx.spark_session
    now = datetime.utcnow()

    tenant_plan_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("plan_id", StringType(), True),
        StructField("plan_name", StringType(), True),
        StructField("billing_cycle", StringType(), True),
        StructField("subscription_status", StringType(), True),
        StructField("started_at", TimestampType(), True),
        StructField("current_period_start", TimestampType(), True),
        StructField("current_period_end", TimestampType(), True),
        StructField("next_billing_date", TimestampType(), True),
        StructField("mrr", DoubleType(), True),
        StructField("arr", DoubleType(), True),
        StructField("is_trial", BooleanType(), True),
        StructField("trial_ends_at", TimestampType(), True),
        StructField("cancel_at_period_end", BooleanType(), True),
        StructField("updated_at", TimestampType(), True),
    ])

    # Get customers and their tiers
    customers = customer_config.select(
        "customer_id",
        "customer_name",
        F.col("tier").alias("plan_id"),
    )

    # Join with plan details
    tenant_plans = customers.join(
        billing_plans.select(
            "plan_id",
            F.col("name").alias("plan_name"),
            "monthly_price",
            "annual_price",
        ),
        "plan_id",
        "left"
    )

    # Get subscription details if available
    if subscription_events.count() > 0:
        # Get latest subscription event per customer
        from pyspark.sql.window import Window
        window = Window.partitionBy("customer_id").orderBy(F.desc("event_timestamp"))

        latest_sub = subscription_events.withColumn(
            "row_num", F.row_number().over(window)
        ).filter(F.col("row_num") == 1).drop("row_num")

        tenant_plans = tenant_plans.join(
            latest_sub.select(
                "customer_id",
                "billing_cycle",
                "subscription_status",
                "started_at",
                "current_period_start",
                "current_period_end",
                "is_trial",
                "trial_ends_at",
                "cancel_at_period_end",
            ),
            "customer_id",
            "left"
        )
    else:
        # Default values
        tenant_plans = tenant_plans.withColumn(
            "billing_cycle", F.lit("monthly")
        ).withColumn(
            "subscription_status", F.lit("active")
        ).withColumn(
            "started_at", F.lit(now - timedelta(days=30))
        ).withColumn(
            "current_period_start", F.lit(now.replace(day=1))
        ).withColumn(
            "current_period_end", F.lit((now.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1))
        ).withColumn(
            "is_trial", F.lit(False)
        ).withColumn(
            "trial_ends_at", F.lit(None).cast(TimestampType())
        ).withColumn(
            "cancel_at_period_end", F.lit(False)
        )

    # Calculate MRR/ARR
    tenant_plans = tenant_plans.withColumn(
        "mrr",
        F.when(F.col("billing_cycle") == "annual", F.col("annual_price") / 12)
        .otherwise(F.col("monthly_price"))
    ).withColumn(
        "arr",
        F.when(F.col("billing_cycle") == "annual", F.col("annual_price"))
        .otherwise(F.col("monthly_price") * 12)
    )

    # Next billing date
    tenant_plans = tenant_plans.withColumn(
        "next_billing_date",
        F.when(
            F.col("cancel_at_period_end") == True,
            F.lit(None).cast(TimestampType())
        ).otherwise(F.col("current_period_end") + F.expr("INTERVAL 1 DAY"))
    )

    # Add timestamp
    tenant_plans = tenant_plans.withColumn("updated_at", F.lit(now))

    result = tenant_plans.select(
        "customer_id",
        "customer_name",
        "plan_id",
        "plan_name",
        "billing_cycle",
        "subscription_status",
        "started_at",
        "current_period_start",
        "current_period_end",
        "next_billing_date",
        "mrr",
        "arr",
        "is_trial",
        "trial_ends_at",
        "cancel_at_period_end",
        "updated_at",
    )

    output.write_dataframe(result)


# =============================================================================
# BILLING SUMMARY
# =============================================================================

@transform(
    tenant_plans=Input("/RevOps/Commercial/tenant_plans"),
    output=Output("/RevOps/Commercial/billing_summary"),
)
def compute_billing_summary(ctx, tenant_plans: DataFrame, output):
    """
    Compute aggregate billing metrics for finance.
    """
    spark = ctx.spark_session
    now = datetime.utcnow()

    if tenant_plans.count() == 0:
        output.write_dataframe(spark.createDataFrame([], StructType([
            StructField("summary_date", TimestampType(), True),
            StructField("total_customers", IntegerType(), True),
            StructField("total_mrr", DoubleType(), True),
            StructField("total_arr", DoubleType(), True),
            StructField("customers_by_plan", StringType(), True),
            StructField("mrr_by_plan", StringType(), True),
            StructField("trial_customers", IntegerType(), True),
            StructField("churning_customers", IntegerType(), True),
        ])))
        return

    # Overall metrics
    total = tenant_plans.filter(
        F.col("subscription_status") == "active"
    ).agg(
        F.count("*").alias("total_customers"),
        F.sum("mrr").alias("total_mrr"),
        F.sum("arr").alias("total_arr"),
        F.sum(F.when(F.col("is_trial") == True, 1).otherwise(0)).alias("trial_customers"),
        F.sum(F.when(F.col("cancel_at_period_end") == True, 1).otherwise(0)).alias("churning_customers"),
    )

    # By plan breakdown
    by_plan = tenant_plans.filter(
        F.col("subscription_status") == "active"
    ).groupBy("plan_id").agg(
        F.count("*").alias("count"),
        F.sum("mrr").alias("mrr"),
    ).collect()

    customers_by_plan = {row["plan_id"]: row["count"] for row in by_plan}
    mrr_by_plan = {row["plan_id"]: row["mrr"] for row in by_plan}

    # Create summary row
    summary = total.withColumn(
        "summary_date", F.lit(now)
    ).withColumn(
        "customers_by_plan", F.lit(str(customers_by_plan))
    ).withColumn(
        "mrr_by_plan", F.lit(str(mrr_by_plan))
    )

    output.write_dataframe(summary)
