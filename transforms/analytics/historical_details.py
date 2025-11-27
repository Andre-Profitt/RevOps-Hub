# /transforms/analytics/historical_details.py
"""
HISTORICAL ANALYTICS DETAILS
============================
Generates historical detail records used by ML transforms
for training and feature engineering.

Outputs:
- /RevOps/Analytics/customer_health_details - Historical customer health snapshots
- /RevOps/Analytics/win_loss_details - Historical deal outcomes for ML training
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@transform(
    accounts=Input("/RevOps/Enriched/accounts"),
    opportunities=Input("/RevOps/Scenario/opportunities"),
    output=Output("/RevOps/Analytics/customer_health_details")
)
def compute_customer_health_details(ctx, accounts, opportunities, output):
    """
    Generate historical customer health detail records.
    Used by churn prediction model for training.
    """

    accts = accounts.dataframe()
    opps = opportunities.dataframe()

    # Get customer accounts
    customers = accts.filter(F.col("is_customer") == True)

    # Calculate health metrics per customer
    customer_opps = opps.groupBy("account_id").agg(
        F.sum(F.when(F.col("is_won"), F.col("amount")).otherwise(0)).alias("lifetime_value"),
        F.count(F.when(F.col("is_won"), 1)).alias("purchases"),
        F.avg(F.when(F.col("is_won"), F.col("amount"))).alias("avg_purchase"),
        F.max(F.when(F.col("is_won"), F.col("close_date"))).alias("last_purchase_date"),
        F.min(F.when(F.col("is_won"), F.col("close_date"))).alias("first_purchase_date"),
    )

    # Join and enrich
    enriched = customers.join(customer_opps, "account_id", "left")

    # Calculate health indicators
    health_details = enriched.withColumn(
        "days_since_last_purchase",
        F.datediff(F.current_date(), F.col("last_purchase_date"))
    ).withColumn(
        "tenure_days",
        F.datediff(F.current_date(), F.col("first_purchase_date"))
    ).withColumn(
        "purchase_frequency",
        F.when(
            F.col("tenure_days") > 0,
            F.round(F.col("purchases") / (F.col("tenure_days") / 365.0), 2)
        ).otherwise(0)
    ).withColumn(
        "recency_score",
        F.when(F.col("days_since_last_purchase") <= 30, 100)
        .when(F.col("days_since_last_purchase") <= 90, 75)
        .when(F.col("days_since_last_purchase") <= 180, 50)
        .when(F.col("days_since_last_purchase") <= 365, 25)
        .otherwise(10)
    ).withColumn(
        "frequency_score",
        F.when(F.col("purchase_frequency") >= 4, 100)
        .when(F.col("purchase_frequency") >= 2, 75)
        .when(F.col("purchase_frequency") >= 1, 50)
        .otherwise(25)
    ).withColumn(
        "monetary_score",
        F.when(F.col("lifetime_value") >= 500000, 100)
        .when(F.col("lifetime_value") >= 100000, 75)
        .when(F.col("lifetime_value") >= 50000, 50)
        .otherwise(25)
    ).withColumn(
        "health_score",
        F.round(
            F.col("recency_score") * 0.4 +
            F.col("frequency_score") * 0.3 +
            F.col("monetary_score") * 0.3,
            0
        )
    ).withColumn(
        "health_status",
        F.when(F.col("health_score") >= 80, "Healthy")
        .when(F.col("health_score") >= 60, "Monitor")
        .when(F.col("health_score") >= 40, "At Risk")
        .otherwise("Critical")
    ).withColumn(
        "churned",
        F.col("days_since_last_purchase") > 365
    ).withColumn(
        "snapshot_date",
        F.current_date()
    )

    output.write_dataframe(health_details)


@transform(
    opportunities=Input("/RevOps/Scenario/opportunities"),
    activities=Input("/RevOps/Scenario/activities"),
    output=Output("/RevOps/Analytics/win_loss_details")
)
def compute_win_loss_details(ctx, opportunities, activities, output):
    """
    Generate historical win/loss detail records.
    Used by deal scoring model for training.
    """

    opps = opportunities.dataframe()
    acts = activities.dataframe()

    # Get closed deals for training
    closed_deals = opps.filter(F.col("is_closed") == True)

    # Calculate activity metrics per deal
    deal_activities = acts.groupBy("opportunity_id").agg(
        F.count("*").alias("total_activities"),
        F.countDistinct("activity_type").alias("activity_types"),
        F.sum(F.when(F.col("activity_type") == "Call", 1).otherwise(0)).alias("calls"),
        F.sum(F.when(F.col("activity_type") == "Email", 1).otherwise(0)).alias("emails"),
        F.sum(F.when(F.col("activity_type") == "Meeting", 1).otherwise(0)).alias("meetings"),
        F.sum(F.when(F.col("activity_type") == "Demo", 1).otherwise(0)).alias("demos"),
        F.max("activity_date").alias("last_activity_date"),
        F.min("activity_date").alias("first_activity_date"),
    )

    # Join
    enriched = closed_deals.join(deal_activities, "opportunity_id", "left")

    # Calculate features used for ML
    win_loss_details = enriched.withColumn(
        "activity_density",
        F.when(
            F.col("days_to_close") > 0,
            F.round(F.col("total_activities") / F.col("days_to_close") * 30, 2)
        ).otherwise(0)
    ).withColumn(
        "meeting_ratio",
        F.when(
            F.col("total_activities") > 0,
            F.round(F.col("meetings") / F.col("total_activities") * 100, 1)
        ).otherwise(0)
    ).withColumn(
        "demo_conducted",
        F.col("demos") > 0
    ).withColumn(
        "multi_touch",
        F.col("activity_types") >= 3
    ).withColumn(
        "velocity_score",
        F.when(F.col("days_to_close") <= 30, 100)
        .when(F.col("days_to_close") <= 60, 75)
        .when(F.col("days_to_close") <= 90, 50)
        .when(F.col("days_to_close") <= 120, 25)
        .otherwise(10)
    ).withColumn(
        "engagement_score",
        F.when(F.col("activity_density") >= 5, 100)
        .when(F.col("activity_density") >= 3, 75)
        .when(F.col("activity_density") >= 1, 50)
        .otherwise(25)
    ).withColumn(
        "stakeholder_score",
        F.when(F.col("stakeholder_count") >= 5, 100)
        .when(F.col("stakeholder_count") >= 3, 75)
        .when(F.col("stakeholder_count") >= 2, 50)
        .otherwise(25)
    ).withColumn(
        "deal_size_bucket",
        F.when(F.col("amount") >= 500000, "Enterprise")
        .when(F.col("amount") >= 100000, "Mid-Market")
        .otherwise("SMB")
    ).withColumn(
        "outcome",
        F.when(F.col("is_won"), "Won").otherwise("Lost")
    ).withColumn(
        "close_quarter",
        F.concat(F.lit("Q"), F.quarter(F.col("close_date")), F.lit(" "), F.year(F.col("close_date")))
    ).withColumn(
        "snapshot_date",
        F.current_date()
    )

    output.write_dataframe(win_loss_details)
