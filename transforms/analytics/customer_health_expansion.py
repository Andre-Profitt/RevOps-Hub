"""
Customer Health & Expansion Analytics

Transforms for customer health scoring, churn risk prediction,
and expansion opportunity identification.
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@transform(
    accounts=Input("/RevOps/Enriched/accounts"),
    opportunities=Input("/RevOps/Enriched/deal_health_scores"),
    activities=Input("/RevOps/Raw/activities"),
    output=Output("/RevOps/Analytics/customer_health_scores"),
)
def compute_customer_health_scores(accounts, opportunities, activities, output):
    """
    Compute customer health scores for existing customers.

    Health score components:
    - Usage/engagement signals (30%)
    - Relationship strength (25%)
    - Financial health (25%)
    - Support sentiment (20%)
    """
    accounts_df = accounts.dataframe()
    opps_df = opportunities.dataframe()
    activities_df = activities.dataframe()

    # Filter to customers only (has ARR)
    customers = accounts_df.filter(F.col("current_arr") > 0)

    # Activity metrics per account
    activity_metrics = activities_df.groupBy("account_id").agg(
        F.count("*").alias("total_activities"),
        F.sum(F.when(F.col("activity_type") == "Meeting", 1).otherwise(0)).alias("meeting_count"),
        F.max("activity_date").alias("last_activity_date"),
        F.countDistinct("contact_id").alias("contacts_engaged"),
    )

    # Opportunity metrics
    opp_metrics = opps_df.groupBy("account_id").agg(
        F.sum(F.when(F.col("stage_name") == "Closed Won", F.col("amount")).otherwise(0)).alias("total_won"),
        F.count(F.when(F.col("is_closed") == False, 1)).alias("open_opps"),
        F.sum(F.when(F.col("is_closed") == False, F.col("amount")).otherwise(0)).alias("open_pipeline"),
    )

    # Join all data
    health_data = customers.join(
        activity_metrics, customers["account_id"] == activity_metrics["account_id"], "left"
    ).join(
        opp_metrics, customers["account_id"] == opp_metrics["account_id"], "left"
    )

    # Calculate component scores
    health_data = health_data.withColumn(
        "days_since_activity",
        F.datediff(F.current_date(), F.col("last_activity_date"))
    ).withColumn(
        "engagement_score",
        F.least(
            F.round(
                (F.coalesce(F.col("meeting_count"), F.lit(0)) * 10 +
                 F.coalesce(F.col("contacts_engaged"), F.lit(0)) * 15) / 2,
                0
            ),
            F.lit(100)
        )
    ).withColumn(
        "relationship_score",
        F.when(F.col("days_since_activity") <= 14, 100)
         .when(F.col("days_since_activity") <= 30, 80)
         .when(F.col("days_since_activity") <= 60, 60)
         .when(F.col("days_since_activity") <= 90, 40)
         .otherwise(20)
    ).withColumn(
        "financial_score",
        F.when(F.col("open_pipeline") > F.col("current_arr") * 0.5, 100)
         .when(F.col("open_pipeline") > F.col("current_arr") * 0.25, 80)
         .when(F.col("open_pipeline") > 0, 60)
         .otherwise(40)
    ).withColumn(
        "support_score",
        F.lit(70)  # Placeholder - would come from support data
    )

    # Calculate overall health score
    health_data = health_data.withColumn(
        "health_score",
        F.round(
            F.col("engagement_score") * 0.30 +
            F.col("relationship_score") * 0.25 +
            F.col("financial_score") * 0.25 +
            F.col("support_score") * 0.20,
            1
        )
    ).withColumn(
        "health_tier",
        F.when(F.col("health_score") >= 80, "Healthy")
         .when(F.col("health_score") >= 60, "Monitor")
         .when(F.col("health_score") >= 40, "At Risk")
         .otherwise("Critical")
    ).withColumn(
        "churn_risk",
        F.when(F.col("health_score") >= 80, "Low")
         .when(F.col("health_score") >= 60, "Medium")
         .when(F.col("health_score") >= 40, "High")
         .otherwise("Critical")
    ).withColumn(
        "renewal_date",
        F.date_add(F.current_date(), F.floor(F.rand() * 365).cast("int"))  # Simulated
    ).withColumn(
        "days_to_renewal",
        F.datediff(F.col("renewal_date"), F.current_date())
    )

    # Select output columns
    result = health_data.select(
        "account_id",
        "account_name",
        "industry",
        "segment",
        "current_arr",
        "health_score",
        "health_tier",
        "engagement_score",
        "relationship_score",
        "financial_score",
        "support_score",
        "churn_risk",
        "renewal_date",
        "days_to_renewal",
        F.coalesce(F.col("days_since_activity"), F.lit(999)).alias("days_since_activity"),
        F.coalesce(F.col("contacts_engaged"), F.lit(0)).alias("contacts_engaged"),
        F.coalesce(F.col("open_pipeline"), F.lit(0)).alias("open_pipeline"),
    )

    output.write_dataframe(result)


@transform(
    customer_health=Input("/RevOps/Analytics/customer_health_scores"),
    output=Output("/RevOps/Analytics/expansion_opportunities"),
)
def compute_expansion_opportunities(customer_health, output):
    """
    Identify expansion opportunities based on health and usage patterns.
    """
    health_df = customer_health.dataframe()

    # Score expansion potential
    expansion = health_df.withColumn(
        "expansion_score",
        F.round(
            (F.col("health_score") * 0.40) +
            (F.when(F.col("open_pipeline") > 0, 30).otherwise(0)) +
            (F.when(F.col("contacts_engaged") >= 3, 20).otherwise(F.col("contacts_engaged") * 7)) +
            (F.when(F.col("days_since_activity") <= 14, 10).otherwise(0)),
            1
        )
    ).withColumn(
        "expansion_potential",
        F.round(F.col("current_arr") * F.col("expansion_score") / 100, 0)
    ).withColumn(
        "expansion_tier",
        F.when(F.col("expansion_score") >= 80, "High")
         .when(F.col("expansion_score") >= 60, "Medium")
         .when(F.col("expansion_score") >= 40, "Low")
         .otherwise("None")
    ).withColumn(
        "recommended_play",
        F.when(
            (F.col("health_score") >= 80) & (F.col("open_pipeline") == 0),
            "Initiate expansion conversation - healthy account with no active deals"
        ).when(
            (F.col("health_score") >= 60) & (F.col("open_pipeline") > 0),
            "Accelerate existing opportunity - good health, deal in progress"
        ).when(
            (F.col("health_score") < 60) & (F.col("churn_risk").isin("High", "Critical")),
            "Focus on retention before expansion - address churn risk first"
        ).otherwise("Monitor and nurture - build relationship before expansion")
    ).withColumn(
        "priority_rank",
        F.row_number().over(
            Window.orderBy(
                F.col("expansion_score").desc(),
                F.col("current_arr").desc()
            )
        )
    )

    output.write_dataframe(expansion)


@transform(
    customer_health=Input("/RevOps/Analytics/customer_health_scores"),
    output=Output("/RevOps/Analytics/churn_risk_analysis"),
)
def compute_churn_risk_analysis(customer_health, output):
    """
    Analyze churn risk patterns and identify at-risk customers.
    """
    health_df = customer_health.dataframe()

    # Filter to at-risk customers
    at_risk = health_df.filter(F.col("churn_risk").isin("High", "Critical"))

    # Add risk factors and recommendations
    at_risk = at_risk.withColumn(
        "risk_factors",
        F.concat_ws(", ",
            F.when(F.col("days_since_activity") > 30, F.lit("No recent engagement")).otherwise(None),
            F.when(F.col("contacts_engaged") < 2, F.lit("Single-threaded")).otherwise(None),
            F.when(F.col("engagement_score") < 50, F.lit("Low engagement")).otherwise(None),
            F.when(F.col("days_to_renewal") < 90, F.lit("Renewal approaching")).otherwise(None),
        )
    ).withColumn(
        "arr_at_risk",
        F.when(F.col("churn_risk") == "Critical", F.col("current_arr"))
         .when(F.col("churn_risk") == "High", F.col("current_arr") * 0.7)
         .otherwise(F.col("current_arr") * 0.3)
    ).withColumn(
        "save_probability",
        F.when(F.col("churn_risk") == "Critical", 0.3)
         .when(F.col("churn_risk") == "High", 0.5)
         .otherwise(0.7)
    ).withColumn(
        "recommended_action",
        F.when(F.col("churn_risk") == "Critical",
            "Immediate executive outreach required"
        ).when(
            (F.col("churn_risk") == "High") & (F.col("days_to_renewal") < 90),
            "Schedule urgent renewal review meeting"
        ).when(
            F.col("days_since_activity") > 60,
            "Re-engagement campaign - customer going dark"
        ).otherwise("Increase touchpoint frequency")
    ).withColumn(
        "urgency",
        F.when(
            (F.col("churn_risk") == "Critical") | (F.col("days_to_renewal") < 30),
            "Immediate"
        ).when(
            (F.col("churn_risk") == "High") | (F.col("days_to_renewal") < 60),
            "This Week"
        ).otherwise("This Month")
    ).withColumn(
        "priority_rank",
        F.row_number().over(
            Window.orderBy(
                F.when(F.col("churn_risk") == "Critical", 1).otherwise(2),
                F.col("arr_at_risk").desc()
            )
        )
    )

    output.write_dataframe(at_risk)


@transform(
    customer_health=Input("/RevOps/Analytics/customer_health_scores"),
    expansion=Input("/RevOps/Analytics/expansion_opportunities"),
    churn_risk=Input("/RevOps/Analytics/churn_risk_analysis"),
    output=Output("/RevOps/Analytics/customer_health_summary"),
)
def compute_customer_health_summary(customer_health, expansion, churn_risk, output):
    """
    Executive summary of customer health metrics.
    """
    health_df = customer_health.dataframe()
    expansion_df = expansion.dataframe()
    churn_df = churn_risk.dataframe()

    # Overall metrics
    summary = health_df.agg(
        F.count("*").alias("total_customers"),
        F.sum("current_arr").alias("total_arr"),
        F.avg("health_score").alias("avg_health_score"),
        F.sum(F.when(F.col("health_tier") == "Healthy", 1).otherwise(0)).alias("healthy_count"),
        F.sum(F.when(F.col("health_tier") == "Monitor", 1).otherwise(0)).alias("monitor_count"),
        F.sum(F.when(F.col("health_tier") == "At Risk", 1).otherwise(0)).alias("at_risk_count"),
        F.sum(F.when(F.col("health_tier") == "Critical", 1).otherwise(0)).alias("critical_count"),
        F.sum(F.when(F.col("health_tier") == "Healthy", F.col("current_arr")).otherwise(0)).alias("healthy_arr"),
        F.sum(F.when(F.col("health_tier").isin("At Risk", "Critical"), F.col("current_arr")).otherwise(0)).alias("at_risk_arr"),
        F.sum(F.when(F.col("days_to_renewal") <= 90, 1).otherwise(0)).alias("renewals_90d"),
        F.sum(F.when(F.col("days_to_renewal") <= 90, F.col("current_arr")).otherwise(0)).alias("renewal_arr_90d"),
    )

    # Expansion metrics
    expansion_metrics = expansion_df.agg(
        F.sum("expansion_potential").alias("total_expansion_potential"),
        F.sum(F.when(F.col("expansion_tier") == "High", 1).otherwise(0)).alias("high_expansion_count"),
        F.sum(F.when(F.col("expansion_tier") == "High", F.col("expansion_potential")).otherwise(0)).alias("high_expansion_value"),
    ).first()

    # Churn metrics
    churn_metrics = churn_df.agg(
        F.count("*").alias("churn_risk_count"),
        F.sum("arr_at_risk").alias("total_arr_at_risk"),
    ).first()

    # Combine
    summary = summary.withColumn("total_expansion_potential", F.lit(expansion_metrics["total_expansion_potential"]))
    summary = summary.withColumn("high_expansion_count", F.lit(expansion_metrics["high_expansion_count"]))
    summary = summary.withColumn("high_expansion_value", F.lit(expansion_metrics["high_expansion_value"]))
    summary = summary.withColumn("churn_risk_count", F.lit(churn_metrics["churn_risk_count"]))
    summary = summary.withColumn("total_arr_at_risk", F.lit(churn_metrics["total_arr_at_risk"]))
    summary = summary.withColumn("net_retention_forecast",
        F.round((F.col("total_arr") + F.col("total_expansion_potential") - F.col("total_arr_at_risk")) / F.col("total_arr"), 3)
    )
    summary = summary.withColumn("snapshot_date", F.current_date())

    output.write_dataframe(summary)
