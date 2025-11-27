# /transforms/commercial/usage_metering.py
"""
USAGE METERING SYSTEM
=====================
Tracks platform usage for licensing enforcement and billing.

Metrics tracked:
- Active users (DAU/MAU)
- Opportunity count
- API calls
- Feature usage
- Compute consumption

Outputs:
- /RevOps/Commercial/usage_daily: Daily usage metrics
- /RevOps/Commercial/usage_summary: Monthly summary for billing
- /RevOps/Commercial/license_status: License compliance status
"""

from transforms.api import transform, Input, Output
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, BooleanType
from datetime import datetime, timedelta


# =============================================================================
# TIER LIMITS
# =============================================================================

TIER_LIMITS = {
    "starter": {
        "max_users": 10,
        "max_opportunities": 10000,
        "max_api_calls_per_month": 0,  # No API access
        "max_compute_hours_per_month": 10,
        "data_retention_days": 365,
    },
    "growth": {
        "max_users": 50,
        "max_opportunities": 50000,
        "max_api_calls_per_month": 100000,
        "max_compute_hours_per_month": 100,
        "data_retention_days": 730,
    },
    "enterprise": {
        "max_users": 500,
        "max_opportunities": 500000,
        "max_api_calls_per_month": 1000000,
        "max_compute_hours_per_month": 1000,
        "data_retention_days": 1095,
    },
}


# =============================================================================
# DAILY USAGE TRACKING
# =============================================================================

@transform(
    opportunities=Input("/RevOps/Staging/opportunities"),
    activities=Input("/RevOps/Staging/activities"),
    build_metrics=Input("/RevOps/Monitoring/build_metrics"),
    api_logs=Input("/RevOps/Audit/api_access_log"),
    feature_usage=Input("/RevOps/Telemetry/feature_usage"),
    customer_config=Input("/RevOps/Config/customer_settings"),
    output=Output("/RevOps/Commercial/usage_daily"),
)
def compute_daily_usage(
    ctx,
    opportunities: DataFrame,
    activities: DataFrame,
    build_metrics: DataFrame,
    api_logs: DataFrame,
    feature_usage: DataFrame,
    customer_config: DataFrame,
    output,
):
    """
    Compute daily usage metrics per customer.
    """
    spark = ctx.spark_session
    now = datetime.utcnow()
    today = now.date()

    # Get customer list
    customers = customer_config.select(
        "customer_id",
        "customer_name",
        "tier",
    )

    # ----- Active opportunities -----
    opp_counts = opportunities.filter(
        F.col("is_closed") == False
    ).groupBy("customer_id").agg(
        F.count("*").alias("active_opportunities"),
        F.sum("amount").alias("total_pipeline_value"),
    )

    # ----- Active users (from activities today) -----
    if activities.count() > 0:
        user_activity = activities.filter(
            F.col("activity_date") == F.lit(today)
        ).groupBy("customer_id").agg(
            F.countDistinct("owner_id").alias("active_users_today"),
        )
    else:
        user_activity = spark.createDataFrame([], StructType([
            StructField("customer_id", StringType(), True),
            StructField("active_users_today", IntegerType(), True),
        ]))

    # ----- Compute usage (from builds today) -----
    if build_metrics.count() > 0:
        compute_usage = build_metrics.filter(
            F.to_date(F.col("build_timestamp")) == F.lit(today)
        ).groupBy("customer_id").agg(
            F.count("*").alias("builds_today"),
            F.sum("duration_seconds").alias("compute_seconds_today"),
        )
    else:
        compute_usage = spark.createDataFrame([], StructType([
            StructField("customer_id", StringType(), True),
            StructField("builds_today", IntegerType(), True),
            StructField("compute_seconds_today", DoubleType(), True),
        ]))

    # ----- API usage -----
    if api_logs.count() > 0:
        api_usage = api_logs.filter(
            F.to_date(F.col("timestamp")) == F.lit(today)
        ).groupBy("customer_id").agg(
            F.count("*").alias("api_calls_today"),
        )
    else:
        api_usage = spark.createDataFrame([], StructType([
            StructField("customer_id", StringType(), True),
            StructField("api_calls_today", IntegerType(), True),
        ]))

    # ----- Feature usage -----
    if feature_usage.count() > 0:
        features_used = feature_usage.filter(
            F.to_date(F.col("usage_timestamp")) == F.lit(today)
        ).groupBy("customer_id").agg(
            F.countDistinct("feature_name").alias("features_used_today"),
            F.count("*").alias("feature_events_today"),
        )
    else:
        features_used = spark.createDataFrame([], StructType([
            StructField("customer_id", StringType(), True),
            StructField("features_used_today", IntegerType(), True),
            StructField("feature_events_today", IntegerType(), True),
        ]))

    # ----- Combine all metrics -----
    usage = customers.join(opp_counts, "customer_id", "left") \
        .join(user_activity, "customer_id", "left") \
        .join(compute_usage, "customer_id", "left") \
        .join(api_usage, "customer_id", "left") \
        .join(features_used, "customer_id", "left")

    # Fill nulls
    usage = usage.fillna({
        "active_opportunities": 0,
        "total_pipeline_value": 0,
        "active_users_today": 0,
        "builds_today": 0,
        "compute_seconds_today": 0,
        "api_calls_today": 0,
        "features_used_today": 0,
        "feature_events_today": 0,
    })

    # Add date and compute hours
    usage = usage.withColumn(
        "usage_date", F.lit(today)
    ).withColumn(
        "compute_hours_today",
        F.col("compute_seconds_today") / 3600.0
    )

    output.write_dataframe(usage)


# =============================================================================
# MONTHLY USAGE SUMMARY
# =============================================================================

@transform(
    daily_usage=Input("/RevOps/Commercial/usage_daily"),
    customer_config=Input("/RevOps/Config/customer_settings"),
    output=Output("/RevOps/Commercial/usage_summary"),
)
def compute_usage_summary(ctx, daily_usage: DataFrame, customer_config: DataFrame, output):
    """
    Compute monthly usage summary for billing.
    """
    spark = ctx.spark_session
    now = datetime.utcnow()

    # Get current month
    current_month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    # Aggregate monthly usage
    monthly_usage = daily_usage.filter(
        F.col("usage_date") >= F.lit(current_month_start)
    ).groupBy("customer_id", "tier").agg(
        # User metrics
        F.max("active_users_today").alias("peak_active_users"),
        F.avg("active_users_today").alias("avg_daily_active_users"),
        F.countDistinct("usage_date").alias("active_days"),

        # Opportunity metrics
        F.max("active_opportunities").alias("peak_opportunities"),
        F.avg("active_opportunities").alias("avg_opportunities"),
        F.max("total_pipeline_value").alias("peak_pipeline_value"),

        # Compute metrics
        F.sum("builds_today").alias("total_builds"),
        F.sum("compute_hours_today").alias("total_compute_hours"),

        # API metrics
        F.sum("api_calls_today").alias("total_api_calls"),

        # Feature metrics
        F.max("features_used_today").alias("max_features_used"),
        F.sum("feature_events_today").alias("total_feature_events"),
    )

    # Get tier limits
    tier_limits_rows = [
        (tier, limits["max_users"], limits["max_opportunities"],
         limits["max_api_calls_per_month"], limits["max_compute_hours_per_month"])
        for tier, limits in TIER_LIMITS.items()
    ]
    tier_limits_df = spark.createDataFrame(
        tier_limits_rows,
        ["tier", "limit_users", "limit_opportunities", "limit_api_calls", "limit_compute_hours"]
    )

    monthly_usage = monthly_usage.join(tier_limits_df, "tier", "left")

    # Calculate utilization percentages
    monthly_usage = monthly_usage.withColumn(
        "user_utilization",
        F.when(F.col("limit_users") > 0,
               F.col("peak_active_users") / F.col("limit_users"))
        .otherwise(0)
    ).withColumn(
        "opportunity_utilization",
        F.when(F.col("limit_opportunities") > 0,
               F.col("peak_opportunities") / F.col("limit_opportunities"))
        .otherwise(0)
    ).withColumn(
        "api_utilization",
        F.when(F.col("limit_api_calls") > 0,
               F.col("total_api_calls") / F.col("limit_api_calls"))
        .otherwise(0)
    ).withColumn(
        "compute_utilization",
        F.when(F.col("limit_compute_hours") > 0,
               F.col("total_compute_hours") / F.col("limit_compute_hours"))
        .otherwise(0)
    )

    # Calculate overage
    monthly_usage = monthly_usage.withColumn(
        "users_overage",
        F.greatest(F.col("peak_active_users") - F.col("limit_users"), F.lit(0))
    ).withColumn(
        "opportunities_overage",
        F.greatest(F.col("peak_opportunities") - F.col("limit_opportunities"), F.lit(0))
    ).withColumn(
        "api_overage",
        F.greatest(F.col("total_api_calls") - F.col("limit_api_calls"), F.lit(0))
    ).withColumn(
        "compute_overage",
        F.greatest(F.col("total_compute_hours") - F.col("limit_compute_hours"), F.lit(0))
    )

    # Overage flags
    monthly_usage = monthly_usage.withColumn(
        "has_overage",
        (F.col("users_overage") > 0) |
        (F.col("opportunities_overage") > 0) |
        (F.col("api_overage") > 0) |
        (F.col("compute_overage") > 0)
    )

    # Add billing period
    monthly_usage = monthly_usage.withColumn(
        "billing_period",
        F.lit(current_month_start.strftime("%Y-%m"))
    ).withColumn(
        "generated_at",
        F.lit(now)
    )

    output.write_dataframe(monthly_usage)


# =============================================================================
# LICENSE STATUS
# =============================================================================

@transform(
    usage_summary=Input("/RevOps/Commercial/usage_summary"),
    customer_config=Input("/RevOps/Config/customer_settings"),
    output=Output("/RevOps/Commercial/license_status"),
)
def compute_license_status(ctx, usage_summary: DataFrame, customer_config: DataFrame, output):
    """
    Compute license compliance status per customer.
    """
    spark = ctx.spark_session
    now = datetime.utcnow()

    # Get latest usage summary
    latest = usage_summary.filter(
        F.col("billing_period") == F.lit(now.strftime("%Y-%m"))
    )

    # Determine license status
    license_status = latest.withColumn(
        "license_status",
        F.when(F.col("has_overage"), "OVERAGE")
        .when(
            (F.col("user_utilization") > 0.9) |
            (F.col("opportunity_utilization") > 0.9) |
            (F.col("api_utilization") > 0.9),
            "WARNING"
        )
        .otherwise("COMPLIANT")
    )

    # Determine upgrade recommendation
    license_status = license_status.withColumn(
        "upgrade_recommended",
        F.when(
            (F.col("tier") == "starter") &
            ((F.col("user_utilization") > 0.8) | (F.col("opportunity_utilization") > 0.8)),
            True
        ).when(
            (F.col("tier") == "growth") &
            ((F.col("user_utilization") > 0.8) | (F.col("api_utilization") > 0.8)),
            True
        ).otherwise(False)
    )

    # Recommended tier
    license_status = license_status.withColumn(
        "recommended_tier",
        F.when(
            (F.col("tier") == "starter") & F.col("upgrade_recommended"),
            "growth"
        ).when(
            (F.col("tier") == "growth") & F.col("upgrade_recommended"),
            "enterprise"
        ).otherwise(F.col("tier"))
    )

    # Compliance message
    license_status = license_status.withColumn(
        "compliance_message",
        F.when(F.col("license_status") == "OVERAGE",
               F.concat(
                   F.lit("License limits exceeded. "),
                   F.when(F.col("users_overage") > 0,
                          F.concat(F.col("users_overage"), F.lit(" extra users. "))).otherwise(F.lit("")),
                   F.when(F.col("opportunities_overage") > 0,
                          F.concat(F.col("opportunities_overage"), F.lit(" extra opportunities. "))).otherwise(F.lit("")),
               ))
        .when(F.col("license_status") == "WARNING",
               "Approaching license limits. Consider upgrading.")
        .otherwise("License compliant.")
    )

    # Select output columns
    result = license_status.select(
        "customer_id",
        "tier",
        "billing_period",
        "license_status",
        "peak_active_users",
        "limit_users",
        "user_utilization",
        "peak_opportunities",
        "limit_opportunities",
        "opportunity_utilization",
        "total_api_calls",
        "limit_api_calls",
        "api_utilization",
        "total_compute_hours",
        "limit_compute_hours",
        "compute_utilization",
        "has_overage",
        "upgrade_recommended",
        "recommended_tier",
        "compliance_message",
        F.lit(now).alias("checked_at"),
    )

    output.write_dataframe(result)


# =============================================================================
# BILLING EVENTS
# =============================================================================

@transform(
    usage_summary=Input("/RevOps/Commercial/usage_summary"),
    previous_events=Input("/RevOps/Commercial/billing_events"),
    output=Output("/RevOps/Commercial/billing_events"),
)
def generate_billing_events(ctx, usage_summary: DataFrame, previous_events: DataFrame, output):
    """
    Generate billing events for overages.
    """
    spark = ctx.spark_session
    now = datetime.utcnow()

    # Overage pricing (per unit per month)
    OVERAGE_PRICING = {
        "user": 50,          # $50 per extra user
        "opportunity": 0.01,  # $0.01 per extra opportunity
        "api_call": 0.001,   # $0.001 per extra API call
        "compute_hour": 5,   # $5 per extra compute hour
    }

    # Get customers with overage
    overages = usage_summary.filter(F.col("has_overage") == True)

    new_events = []

    for row in overages.collect():
        base_event = {
            "customer_id": row["customer_id"],
            "billing_period": row["billing_period"],
            "tier": row["tier"],
            "event_timestamp": now,
        }

        # User overage
        if row["users_overage"] > 0:
            new_events.append({
                **base_event,
                "event_type": "user_overage",
                "quantity": row["users_overage"],
                "unit_price": OVERAGE_PRICING["user"],
                "total_amount": row["users_overage"] * OVERAGE_PRICING["user"],
            })

        # Opportunity overage
        if row["opportunities_overage"] > 0:
            new_events.append({
                **base_event,
                "event_type": "opportunity_overage",
                "quantity": row["opportunities_overage"],
                "unit_price": OVERAGE_PRICING["opportunity"],
                "total_amount": row["opportunities_overage"] * OVERAGE_PRICING["opportunity"],
            })

        # API overage
        if row["api_overage"] > 0:
            new_events.append({
                **base_event,
                "event_type": "api_overage",
                "quantity": row["api_overage"],
                "unit_price": OVERAGE_PRICING["api_call"],
                "total_amount": row["api_overage"] * OVERAGE_PRICING["api_call"],
            })

        # Compute overage
        if row["compute_overage"] > 0:
            new_events.append({
                **base_event,
                "event_type": "compute_overage",
                "quantity": row["compute_overage"],
                "unit_price": OVERAGE_PRICING["compute_hour"],
                "total_amount": row["compute_overage"] * OVERAGE_PRICING["compute_hour"],
            })

    # Create DataFrame
    if new_events:
        new_events_df = spark.createDataFrame(new_events)
        new_events_df = new_events_df.withColumn(
            "event_id",
            F.concat(
                F.col("customer_id"), F.lit("-"),
                F.col("billing_period"), F.lit("-"),
                F.col("event_type")
            )
        )
    else:
        schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("billing_period", StringType(), True),
            StructField("tier", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("quantity", DoubleType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("event_timestamp", TimestampType(), True),
        ])
        new_events_df = spark.createDataFrame([], schema)

    # Append to history (dedup by event_id)
    if previous_events.count() > 0:
        result = previous_events.unionByName(new_events_df, allowMissingColumns=True)
        result = result.dropDuplicates(["event_id"])
    else:
        result = new_events_df

    output.write_dataframe(result)
