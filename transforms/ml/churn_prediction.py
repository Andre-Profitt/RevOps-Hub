# /transforms/ml/churn_prediction.py
"""
CHURN PREDICTION MODEL
======================
ML-based customer churn prediction using engagement and health signals.

This module provides:
1. Customer health feature engineering
2. Churn risk scoring based on usage patterns
3. Early warning signals and intervention recommendations
4. Churn cohort analysis

Outputs:
- /RevOps/ML/churn_features: Engineered features for each customer
- /RevOps/ML/churn_scores: Predicted churn probability and risk factors
- /RevOps/ML/churn_cohorts: Customer cohort analysis for churn patterns
"""

from transforms.api import transform, Input, Output
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, ArrayType
from datetime import datetime, timedelta


# =============================================================================
# CHURN FEATURE ENGINEERING
# =============================================================================

@transform(
    accounts=Input("/RevOps/Staging/accounts"),
    opportunities=Input("/RevOps/Staging/opportunities"),
    activities=Input("/RevOps/Staging/activities"),
    product_usage=Input("/RevOps/Telemetry/product_usage"),
    support_tickets=Input("/RevOps/Telemetry/support_tickets"),
    output=Output("/RevOps/ML/churn_features"),
)
def engineer_churn_features(
    ctx,
    accounts: DataFrame,
    opportunities: DataFrame,
    activities: DataFrame,
    product_usage: DataFrame,
    support_tickets: DataFrame,
    output,
):
    """
    Engineer features for churn prediction.

    Features include:
    - Engagement metrics (login frequency, feature usage)
    - Support patterns (ticket volume, severity, sentiment)
    - Contract signals (renewal date proximity, expansion/contraction)
    - Relationship health (activity recency, stakeholder coverage)
    """
    spark = ctx.spark_session
    now = datetime.utcnow()
    thirty_days_ago = now - timedelta(days=30)
    ninety_days_ago = now - timedelta(days=90)

    # ----- Base account features -----
    customers = accounts.filter(
        F.col("account_type").isin(["Customer", "customer", "Active"])
    ).select(
        "account_id",
        "account_name",
        "segment",
        "industry",
        "owner_id",
        "created_date",
        F.col("annual_revenue").alias("arr"),
    )

    # Customer tenure
    customers = customers.withColumn(
        "tenure_days",
        F.datediff(F.lit(now), F.col("created_date"))
    ).withColumn(
        "tenure_months",
        F.col("tenure_days") / 30
    )

    # ----- Product usage features -----
    if product_usage.count() > 0:
        usage_30d = product_usage.filter(
            F.col("usage_date") >= F.lit(thirty_days_ago)
        ).groupBy("account_id").agg(
            F.avg("daily_active_users").alias("avg_dau_30d"),
            F.avg("total_events").alias("avg_events_30d"),
            F.avg("engagement_score").alias("avg_engagement_30d"),
            F.countDistinct("usage_date").alias("active_days_30d"),
            F.max("usage_date").alias("last_usage_date"),
        )

        usage_90d = product_usage.filter(
            F.col("usage_date") >= F.lit(ninety_days_ago)
        ).groupBy("account_id").agg(
            F.avg("daily_active_users").alias("avg_dau_90d"),
            F.avg("engagement_score").alias("avg_engagement_90d"),
        )

        # Usage trends
        usage_features = usage_30d.join(usage_90d, "account_id", "left")
        usage_features = usage_features.withColumn(
            "engagement_trend",
            F.when(
                F.col("avg_engagement_90d") > 0,
                (F.col("avg_engagement_30d") - F.col("avg_engagement_90d")) / F.col("avg_engagement_90d")
            ).otherwise(0)
        ).withColumn(
            "dau_trend",
            F.when(
                F.col("avg_dau_90d") > 0,
                (F.col("avg_dau_30d") - F.col("avg_dau_90d")) / F.col("avg_dau_90d")
            ).otherwise(0)
        ).withColumn(
            "days_since_last_usage",
            F.datediff(F.lit(now), F.col("last_usage_date"))
        ).withColumn(
            "usage_frequency",
            F.col("active_days_30d") / 30.0
        )

        customers = customers.join(usage_features, "account_id", "left")
    else:
        customers = customers.withColumn("avg_dau_30d", F.lit(None))
        customers = customers.withColumn("avg_engagement_30d", F.lit(None))
        customers = customers.withColumn("engagement_trend", F.lit(0))
        customers = customers.withColumn("dau_trend", F.lit(0))
        customers = customers.withColumn("days_since_last_usage", F.lit(None))
        customers = customers.withColumn("usage_frequency", F.lit(None))

    # ----- Support ticket features -----
    if support_tickets.count() > 0:
        ticket_30d = support_tickets.filter(
            F.col("created_date") >= F.lit(thirty_days_ago)
        ).groupBy("account_id").agg(
            F.count("*").alias("tickets_30d"),
            F.sum(F.when(F.col("severity") == "Critical", 1).otherwise(0)).alias("critical_tickets_30d"),
            F.sum(F.when(F.col("severity") == "High", 1).otherwise(0)).alias("high_tickets_30d"),
            F.avg("satisfaction_rating").alias("avg_csat_30d"),
            F.avg("resolution_hours").alias("avg_resolution_time_30d"),
        )

        ticket_90d = support_tickets.filter(
            F.col("created_date") >= F.lit(ninety_days_ago)
        ).groupBy("account_id").agg(
            F.count("*").alias("tickets_90d"),
            F.avg("satisfaction_rating").alias("avg_csat_90d"),
        )

        ticket_features = ticket_30d.join(ticket_90d, "account_id", "left")
        ticket_features = ticket_features.withColumn(
            "ticket_trend",
            F.when(
                F.col("tickets_90d") > 0,
                (F.col("tickets_30d") * 3 - F.col("tickets_90d")) / F.col("tickets_90d")
            ).otherwise(0)
        ).withColumn(
            "csat_trend",
            F.when(
                F.col("avg_csat_90d") > 0,
                (F.coalesce(F.col("avg_csat_30d"), F.lit(3.0)) - F.col("avg_csat_90d")) / F.col("avg_csat_90d")
            ).otherwise(0)
        )

        customers = customers.join(ticket_features, "account_id", "left")
    else:
        customers = customers.withColumn("tickets_30d", F.lit(0))
        customers = customers.withColumn("critical_tickets_30d", F.lit(0))
        customers = customers.withColumn("avg_csat_30d", F.lit(None))
        customers = customers.withColumn("ticket_trend", F.lit(0))
        customers = customers.withColumn("csat_trend", F.lit(0))

    # ----- Activity features (relationship health) -----
    activity_30d = activities.filter(
        (F.col("account_id").isNotNull()) &
        (F.col("activity_date") >= F.lit(thirty_days_ago))
    ).groupBy("account_id").agg(
        F.count("*").alias("activities_30d"),
        F.sum(F.when(F.col("activity_type") == "Meeting", 1).otherwise(0)).alias("meetings_30d"),
        F.countDistinct("contact_id").alias("contacts_engaged_30d"),
        F.max("activity_date").alias("last_activity_date"),
    )

    customers = customers.join(activity_30d, "account_id", "left").fillna({
        "activities_30d": 0,
        "meetings_30d": 0,
        "contacts_engaged_30d": 0,
    })

    customers = customers.withColumn(
        "days_since_contact",
        F.coalesce(
            F.datediff(F.lit(now), F.col("last_activity_date")),
            F.lit(999)
        )
    )

    # ----- Expansion/Contraction signals -----
    recent_opps = opportunities.filter(
        (F.col("created_date") >= F.lit(ninety_days_ago)) &
        (F.col("type").isin(["Expansion", "Renewal", "Upsell"]))
    ).groupBy("account_id").agg(
        F.sum(F.when(F.col("is_won") == True, F.col("amount")).otherwise(0)).alias("expansion_won_90d"),
        F.sum(F.when(F.col("is_won") == False, F.col("amount")).otherwise(0)).alias("expansion_lost_90d"),
        F.count(F.when(F.col("stage_name") == "Closed Lost", True)).alias("lost_opps_90d"),
    )

    customers = customers.join(recent_opps, "account_id", "left").fillna({
        "expansion_won_90d": 0,
        "expansion_lost_90d": 0,
        "lost_opps_90d": 0,
    })

    # Net expansion rate
    customers = customers.withColumn(
        "net_expansion_rate",
        F.when(
            F.col("arr") > 0,
            (F.col("expansion_won_90d") - F.col("expansion_lost_90d")) / F.col("arr")
        ).otherwise(0)
    )

    # ----- Fill nulls with defaults -----
    customers = customers.fillna({
        "avg_dau_30d": 0,
        "avg_engagement_30d": 0,
        "engagement_trend": 0,
        "dau_trend": 0,
        "days_since_last_usage": 30,
        "usage_frequency": 0,
        "tickets_30d": 0,
        "critical_tickets_30d": 0,
        "avg_csat_30d": 3.0,
        "ticket_trend": 0,
        "csat_trend": 0,
    })

    # Add timestamp
    result = customers.withColumn("feature_timestamp", F.lit(now))

    output.write_dataframe(result)


# =============================================================================
# CHURN SCORING
# =============================================================================

@transform(
    features=Input("/RevOps/ML/churn_features"),
    output=Output("/RevOps/ML/churn_scores"),
)
def score_churn_risk(ctx, features: DataFrame, output):
    """
    Score customer churn risk based on engineered features.
    """
    now = datetime.utcnow()

    # Feature weights (would come from trained model in production)
    # Negative weights indicate churn risk
    weights = {
        "engagement_trend": -0.20,      # Declining engagement = risk
        "dau_trend": -0.15,             # Declining usage = risk
        "usage_frequency": -0.15,       # Low frequency = risk
        "ticket_trend": 0.10,           # Increasing tickets = risk
        "csat_trend": -0.10,            # Declining satisfaction = risk
        "days_since_contact": 0.10,     # No contact = risk
        "days_since_last_usage": 0.10,  # No usage = risk
        "critical_tickets_30d": 0.05,   # Critical tickets = risk
        "net_expansion_rate": -0.10,    # Contraction = risk
    }

    # Normalize and score
    scores = features

    # Normalize features to 0-1 scale where higher = more risk
    scores = scores.withColumn(
        "engagement_risk",
        F.greatest(F.lit(0), -F.col("engagement_trend"))
    ).withColumn(
        "usage_risk",
        F.greatest(F.lit(0), -F.col("dau_trend"))
    ).withColumn(
        "frequency_risk",
        F.greatest(F.lit(0), 1.0 - F.coalesce(F.col("usage_frequency"), F.lit(0)))
    ).withColumn(
        "ticket_risk",
        F.least(F.lit(1), F.greatest(F.lit(0), F.col("ticket_trend")))
    ).withColumn(
        "satisfaction_risk",
        F.greatest(F.lit(0), -F.col("csat_trend"))
    ).withColumn(
        "contact_risk",
        F.least(F.col("days_since_contact") / 60.0, F.lit(1.0))
    ).withColumn(
        "inactivity_risk",
        F.least(F.coalesce(F.col("days_since_last_usage"), F.lit(30)) / 30.0, F.lit(1.0))
    ).withColumn(
        "support_risk",
        F.least(F.col("critical_tickets_30d") / 3.0, F.lit(1.0))
    ).withColumn(
        "expansion_risk",
        F.greatest(F.lit(0), -F.col("net_expansion_rate"))
    )

    # Calculate weighted risk score
    scores = scores.withColumn(
        "raw_churn_score",
        (
            F.col("engagement_risk") * 0.20 +
            F.col("usage_risk") * 0.15 +
            F.col("frequency_risk") * 0.15 +
            F.col("ticket_risk") * 0.10 +
            F.col("satisfaction_risk") * 0.10 +
            F.col("contact_risk") * 0.10 +
            F.col("inactivity_risk") * 0.10 +
            F.col("support_risk") * 0.05 +
            F.col("expansion_risk") * 0.05
        )
    )

    # Calibrate to probability
    scores = scores.withColumn(
        "churn_probability",
        F.least(F.greatest(F.col("raw_churn_score"), F.lit(0.01)), F.lit(0.99))
    )

    # Risk tier
    scores = scores.withColumn(
        "churn_risk_tier",
        F.when(F.col("churn_probability") >= 0.7, "Critical")
        .when(F.col("churn_probability") >= 0.4, "High")
        .when(F.col("churn_probability") >= 0.2, "Medium")
        .otherwise("Low")
    )

    # Identify top risk factors
    scores = scores.withColumn(
        "risk_factors",
        F.array_distinct(F.filter(
            F.array(
                F.when(F.col("engagement_risk") > 0.3, F.lit("Declining engagement")).otherwise(F.lit(None)),
                F.when(F.col("usage_risk") > 0.3, F.lit("Declining usage")).otherwise(F.lit(None)),
                F.when(F.col("frequency_risk") > 0.5, F.lit("Infrequent usage")).otherwise(F.lit(None)),
                F.when(F.col("contact_risk") > 0.5, F.lit("No recent contact")).otherwise(F.lit(None)),
                F.when(F.col("inactivity_risk") > 0.5, F.lit("Product inactivity")).otherwise(F.lit(None)),
                F.when(F.col("support_risk") > 0.3, F.lit("Critical support issues")).otherwise(F.lit(None)),
                F.when(F.col("satisfaction_risk") > 0.2, F.lit("Declining satisfaction")).otherwise(F.lit(None)),
                F.when(F.col("expansion_risk") > 0.1, F.lit("Contract contraction")).otherwise(F.lit(None)),
            ),
            lambda x: x.isNotNull()
        ))
    )

    # Recommended interventions based on risk factors
    scores = scores.withColumn(
        "recommended_action",
        F.when(F.col("churn_probability") >= 0.7, "Executive escalation - schedule QBR immediately")
        .when(F.col("contact_risk") > 0.5, "Schedule customer check-in call")
        .when(F.col("inactivity_risk") > 0.5, "Trigger product adoption campaign")
        .when(F.col("support_risk") > 0.3, "Escalate open support tickets")
        .when(F.col("satisfaction_risk") > 0.2, "Request customer feedback")
        .otherwise("Monitor - no immediate action required")
    )

    # Select output columns
    result = scores.select(
        "account_id",
        "account_name",
        "segment",
        "owner_id",
        F.col("arr").alias("annual_revenue"),
        "tenure_months",
        "churn_probability",
        "churn_risk_tier",
        "risk_factors",
        "recommended_action",
        # Component scores for transparency
        "engagement_risk",
        "usage_risk",
        "contact_risk",
        "support_risk",
        F.lit(now).alias("scored_at"),
    )

    output.write_dataframe(result)


# =============================================================================
# CHURN COHORT ANALYSIS
# =============================================================================

@transform(
    churn_scores=Input("/RevOps/ML/churn_scores"),
    historical_churn=Input("/RevOps/Analytics/customer_health_details"),
    output=Output("/RevOps/ML/churn_cohorts"),
)
def analyze_churn_cohorts(ctx, churn_scores: DataFrame, historical_churn: DataFrame, output):
    """
    Analyze churn patterns by customer cohort.
    """
    now = datetime.utcnow()

    # Group by segment and risk tier
    cohort_summary = churn_scores.groupBy("segment", "churn_risk_tier").agg(
        F.count("*").alias("customer_count"),
        F.sum("annual_revenue").alias("total_arr"),
        F.avg("churn_probability").alias("avg_churn_prob"),
        F.avg("tenure_months").alias("avg_tenure_months"),
    )

    # Calculate ARR at risk by tier
    arr_at_risk = churn_scores.groupBy("churn_risk_tier").agg(
        F.sum("annual_revenue").alias("arr_at_risk"),
        F.count("*").alias("customers_at_risk"),
    )

    # Pivot to get segment breakdown
    segment_risk = churn_scores.groupBy("segment").agg(
        F.sum(F.when(F.col("churn_risk_tier") == "Critical", F.col("annual_revenue")).otherwise(0)).alias("critical_arr"),
        F.sum(F.when(F.col("churn_risk_tier") == "High", F.col("annual_revenue")).otherwise(0)).alias("high_arr"),
        F.sum(F.when(F.col("churn_risk_tier") == "Medium", F.col("annual_revenue")).otherwise(0)).alias("medium_arr"),
        F.sum(F.when(F.col("churn_risk_tier") == "Low", F.col("annual_revenue")).otherwise(0)).alias("low_arr"),
        F.count("*").alias("total_customers"),
    )

    segment_risk = segment_risk.withColumn(
        "total_arr",
        F.col("critical_arr") + F.col("high_arr") + F.col("medium_arr") + F.col("low_arr")
    ).withColumn(
        "at_risk_pct",
        (F.col("critical_arr") + F.col("high_arr")) / F.col("total_arr")
    )

    # Add timestamp
    result = segment_risk.withColumn("snapshot_time", F.lit(now))

    output.write_dataframe(result)
