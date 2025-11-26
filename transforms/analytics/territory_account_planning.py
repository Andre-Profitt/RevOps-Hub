"""
Territory & Account Planning Analytics

Provides account scoring, territory balancing, and white space analysis
for strategic planning and resource allocation.

Outputs:
- /RevOps/Analytics/account_scores: Propensity-to-buy scoring for all accounts
- /RevOps/Analytics/territory_balance: Territory health and balance metrics
- /RevOps/Analytics/white_space_analysis: Expansion opportunities by account
- /RevOps/Analytics/territory_summary: Executive summary of territory health
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime


# =============================================================================
# 1. ACCOUNT SCORING MODEL
# =============================================================================

@transform(
    accounts=Input("/RevOps/Scenario/accounts"),
    opportunities=Input("/RevOps/Scenario/opportunities"),
    activities=Input("/RevOps/Scenario/activities"),
    output=Output("/RevOps/Analytics/account_scores")
)
def compute_account_scores(ctx, accounts, opportunities, activities, output):
    """
    Score accounts based on propensity to buy using firmographic,
    behavioral, and engagement signals.
    """
    accounts_df = accounts.dataframe()
    opps_df = opportunities.dataframe()
    activities_df = activities.dataframe()

    # Aggregate opportunity history per account
    opp_metrics = opps_df.groupBy("account_id").agg(
        F.count("*").alias("total_opportunities"),
        F.sum(F.when(F.col("stage_name") == "Closed Won", 1).otherwise(0)).alias("won_count"),
        F.sum(F.when(F.col("stage_name") == "Closed Lost", 1).otherwise(0)).alias("lost_count"),
        F.sum(F.when(F.col("stage_name") == "Closed Won", F.col("amount")).otherwise(0)).alias("total_won_revenue"),
        F.sum(F.when(~F.col("stage_name").isin(["Closed Won", "Closed Lost"]), F.col("amount")).otherwise(0)).alias("open_pipeline"),
        F.max("close_date").alias("last_close_date"),
        F.avg("amount").alias("avg_deal_size"),
    )

    # Aggregate engagement per account
    engagement_metrics = activities_df.groupBy("account_id").agg(
        F.count("*").alias("total_activities"),
        F.sum(F.when(F.col("activity_type") == "Meeting", 1).otherwise(0)).alias("meeting_count"),
        F.sum(F.when(F.col("activity_type") == "Email", 1).otherwise(0)).alias("email_count"),
        F.countDistinct("contact_id").alias("contacts_engaged"),
        F.max("activity_date").alias("last_activity_date"),
        F.datediff(F.current_date(), F.max("activity_date")).alias("days_since_activity"),
    )

    # Join with accounts
    account_data = accounts_df.join(
        opp_metrics, "account_id", "left"
    ).join(
        engagement_metrics, "account_id", "left"
    )

    # Calculate component scores (0-100 scale)
    scored = account_data.withColumn(
        # Firmographic score (industry, size, tech stack fit)
        "firmographic_score",
        F.when(F.col("industry").isin(["Technology", "Financial Services", "Healthcare"]), 25)
         .when(F.col("industry").isin(["Manufacturing", "Retail"]), 20)
         .otherwise(15) +
        F.when(F.col("employee_count") >= 5000, 25)
         .when(F.col("employee_count") >= 1000, 20)
         .when(F.col("employee_count") >= 500, 15)
         .otherwise(10) +
        F.when(F.col("annual_revenue") >= 1000000000, 25)
         .when(F.col("annual_revenue") >= 500000000, 20)
         .when(F.col("annual_revenue") >= 100000000, 15)
         .otherwise(10)
    ).withColumn(
        # Behavioral score (past purchases, engagement)
        "behavioral_score",
        F.least(
            F.lit(100),
            F.coalesce(F.col("won_count"), F.lit(0)) * 15 +
            F.when(F.col("total_won_revenue") >= 500000, 30)
             .when(F.col("total_won_revenue") >= 100000, 20)
             .otherwise(F.coalesce(F.col("total_won_revenue"), F.lit(0)) / 10000) +
            F.when(F.col("days_since_activity") <= 30, 25)
             .when(F.col("days_since_activity") <= 90, 15)
             .otherwise(5)
        )
    ).withColumn(
        # Engagement score (activity volume, breadth)
        "engagement_score",
        F.least(
            F.lit(100),
            F.coalesce(F.col("meeting_count"), F.lit(0)) * 5 +
            F.coalesce(F.col("email_count"), F.lit(0)) * 2 +
            F.coalesce(F.col("contacts_engaged"), F.lit(0)) * 10
        )
    ).withColumn(
        # Pipeline score (current opportunity value)
        "pipeline_score",
        F.when(F.col("open_pipeline") >= 500000, 100)
         .when(F.col("open_pipeline") >= 200000, 75)
         .when(F.col("open_pipeline") >= 50000, 50)
         .when(F.col("open_pipeline") > 0, 25)
         .otherwise(0)
    )

    # Calculate overall propensity score (weighted average)
    result = scored.withColumn(
        "propensity_score",
        F.round(
            F.col("firmographic_score") * 0.30 +
            F.col("behavioral_score") * 0.25 +
            F.col("engagement_score") * 0.25 +
            F.col("pipeline_score") * 0.20,
            1
        )
    ).withColumn(
        "propensity_tier",
        F.when(F.col("propensity_score") >= 70, "High")
         .when(F.col("propensity_score") >= 50, "Medium")
         .when(F.col("propensity_score") >= 30, "Low")
         .otherwise("Very Low")
    ).withColumn(
        "recommended_action",
        F.when(F.col("propensity_tier") == "High",
               F.when(F.col("open_pipeline") > 0, "Accelerate existing deals")
                .otherwise("Initiate executive outreach"))
         .when(F.col("propensity_tier") == "Medium",
               F.when(F.col("days_since_activity") > 60, "Re-engage with value content")
                .otherwise("Continue nurture sequence"))
         .when(F.col("propensity_tier") == "Low", "Add to awareness campaigns")
         .otherwise("Deprioritize or archive")
    ).withColumn(
        "snapshot_date",
        F.lit(str(datetime.now().date()))
    ).select(
        "account_id",
        "account_name",
        "industry",
        "employee_count",
        "annual_revenue",
        F.col("owner_id").alias("territory_owner_id"),
        F.col("region").alias("territory"),
        "firmographic_score",
        "behavioral_score",
        "engagement_score",
        "pipeline_score",
        "propensity_score",
        "propensity_tier",
        F.coalesce(F.col("total_won_revenue"), F.lit(0)).alias("lifetime_value"),
        F.coalesce(F.col("open_pipeline"), F.lit(0)).alias("open_pipeline"),
        F.coalesce(F.col("total_activities"), F.lit(0)).alias("total_activities"),
        F.coalesce(F.col("days_since_activity"), F.lit(999)).alias("days_since_activity"),
        "recommended_action",
        "snapshot_date",
    )

    output.write_dataframe(result)


# =============================================================================
# 2. TERRITORY BALANCE ANALYSIS
# =============================================================================

@transform(
    account_scores=Input("/RevOps/Analytics/account_scores"),
    opportunities=Input("/RevOps/Scenario/opportunities"),
    reps=Input("/RevOps/Reference/sales_reps"),
    quota=Input("/RevOps/Reference/quota_targets"),
    output=Output("/RevOps/Analytics/territory_balance")
)
def compute_territory_balance(ctx, account_scores, opportunities, reps, quota, output):
    """
    Analyze territory balance across reps and regions.
    """
    scores_df = account_scores.dataframe()
    opps_df = opportunities.dataframe()
    reps_df = reps.dataframe()
    quota_df = quota.dataframe()

    # Aggregate by territory owner
    territory_metrics = scores_df.groupBy("territory_owner_id", "territory").agg(
        F.count("*").alias("account_count"),
        F.sum(F.when(F.col("propensity_tier") == "High", 1).otherwise(0)).alias("high_propensity_accounts"),
        F.sum(F.when(F.col("propensity_tier") == "Medium", 1).otherwise(0)).alias("medium_propensity_accounts"),
        F.sum("lifetime_value").alias("total_lifetime_value"),
        F.sum("open_pipeline").alias("total_pipeline"),
        F.avg("propensity_score").alias("avg_propensity_score"),
        F.sum("annual_revenue").alias("total_addressable_market"),
    )

    # Get quota for each rep
    territory_with_quota = territory_metrics.join(
        quota_df.select("rep_id", "quota_amount"),
        territory_metrics.territory_owner_id == quota_df.rep_id,
        "left"
    ).drop("rep_id")

    # Get rep names
    territory_with_names = territory_with_quota.join(
        reps_df.select("rep_id", "rep_name", "segment"),
        territory_with_quota.territory_owner_id == reps_df.rep_id,
        "left"
    ).drop("rep_id")

    # Calculate balance metrics
    # First, get global averages
    global_stats = territory_metrics.agg(
        F.avg("account_count").alias("avg_accounts"),
        F.avg("high_propensity_accounts").alias("avg_high_propensity"),
        F.avg("total_pipeline").alias("avg_pipeline"),
        F.avg("total_addressable_market").alias("avg_tam"),
    ).collect()[0]

    result = territory_with_names.withColumn(
        "accounts_vs_avg",
        F.round((F.col("account_count") - global_stats["avg_accounts"]) / F.greatest(global_stats["avg_accounts"], F.lit(1)) * 100, 1)
    ).withColumn(
        "pipeline_vs_avg",
        F.round((F.col("total_pipeline") - global_stats["avg_pipeline"]) / F.greatest(global_stats["avg_pipeline"], F.lit(1)) * 100, 1)
    ).withColumn(
        "coverage_ratio",
        F.when(F.col("quota_amount") > 0,
               F.round(F.col("total_pipeline") / F.col("quota_amount"), 2)).otherwise(0)
    ).withColumn(
        "tam_per_account",
        F.when(F.col("account_count") > 0,
               F.round(F.col("total_addressable_market") / F.col("account_count"), 0)).otherwise(0)
    ).withColumn(
        "territory_health",
        F.when(
            (F.col("coverage_ratio") >= 3) & (F.col("high_propensity_accounts") >= 5), "Strong"
        ).when(
            (F.col("coverage_ratio") >= 2) & (F.col("high_propensity_accounts") >= 3), "Healthy"
        ).when(
            (F.col("coverage_ratio") >= 1) | (F.col("high_propensity_accounts") >= 2), "At Risk"
        ).otherwise("Critical")
    ).withColumn(
        "rebalance_recommendation",
        F.when(F.col("accounts_vs_avg") > 30, "Consider redistributing accounts - overloaded")
         .when(F.col("accounts_vs_avg") < -30, "Add accounts - capacity available")
         .when(F.col("coverage_ratio") < 2, "Focus on pipeline generation")
         .otherwise("Territory balanced")
    ).withColumn(
        "snapshot_date",
        F.lit(str(datetime.now().date()))
    ).select(
        F.col("territory_owner_id").alias("rep_id"),
        F.col("rep_name"),
        "territory",
        "segment",
        "account_count",
        "high_propensity_accounts",
        "medium_propensity_accounts",
        F.coalesce(F.col("quota_amount"), F.lit(0)).alias("quota"),
        "total_pipeline",
        "coverage_ratio",
        "total_lifetime_value",
        "total_addressable_market",
        "tam_per_account",
        "avg_propensity_score",
        "accounts_vs_avg",
        "pipeline_vs_avg",
        "territory_health",
        "rebalance_recommendation",
        "snapshot_date",
    )

    output.write_dataframe(result)


# =============================================================================
# 3. WHITE SPACE ANALYSIS
# =============================================================================

@transform(
    accounts=Input("/RevOps/Scenario/accounts"),
    opportunities=Input("/RevOps/Scenario/opportunities"),
    account_scores=Input("/RevOps/Analytics/account_scores"),
    output=Output("/RevOps/Analytics/white_space_analysis")
)
def compute_white_space(ctx, accounts, opportunities, account_scores, output):
    """
    Identify expansion opportunities and white space in accounts.
    """
    accounts_df = accounts.dataframe()
    opps_df = opportunities.dataframe()
    scores_df = account_scores.dataframe()

    # Define product lines/use cases (simulated)
    product_lines = ["Platform", "Analytics", "Integration", "Security", "Support"]

    # Get products already purchased per account
    won_products = opps_df.filter(
        F.col("stage_name") == "Closed Won"
    ).groupBy("account_id").agg(
        F.collect_set("product_line").alias("purchased_products"),
        F.count("*").alias("products_purchased_count"),
        F.sum("amount").alias("total_spend"),
    )

    # Join with account scores
    account_products = scores_df.join(
        won_products, "account_id", "left"
    )

    # Calculate white space
    result = account_products.withColumn(
        "purchased_products",
        F.coalesce(F.col("purchased_products"), F.array())
    ).withColumn(
        "products_purchased_count",
        F.coalesce(F.col("products_purchased_count"), F.lit(0))
    ).withColumn(
        "total_spend",
        F.coalesce(F.col("total_spend"), F.lit(0))
    ).withColumn(
        # Calculate potential based on company size
        "estimated_potential",
        F.when(F.col("annual_revenue") >= 1000000000, 2000000)
         .when(F.col("annual_revenue") >= 500000000, 1000000)
         .when(F.col("annual_revenue") >= 100000000, 500000)
         .otherwise(200000)
    ).withColumn(
        "white_space_value",
        F.greatest(F.col("estimated_potential") - F.col("total_spend"), F.lit(0))
    ).withColumn(
        "wallet_share",
        F.when(F.col("estimated_potential") > 0,
               F.round(F.col("total_spend") / F.col("estimated_potential") * 100, 1)).otherwise(0)
    ).withColumn(
        "expansion_opportunity",
        F.when(F.col("wallet_share") < 20, "High - Greenfield")
         .when(F.col("wallet_share") < 50, "Medium - Expand")
         .when(F.col("wallet_share") < 80, "Low - Optimize")
         .otherwise("Saturated")
    ).withColumn(
        "products_available",
        F.lit(len(product_lines)) - F.col("products_purchased_count")
    ).withColumn(
        "cross_sell_potential",
        F.when(F.col("products_available") >= 4, "High")
         .when(F.col("products_available") >= 2, "Medium")
         .when(F.col("products_available") >= 1, "Low")
         .otherwise("None")
    ).withColumn(
        "upsell_potential",
        F.when((F.col("propensity_score") >= 60) & (F.col("total_spend") > 0), "High")
         .when((F.col("propensity_score") >= 40) & (F.col("total_spend") > 0), "Medium")
         .when(F.col("total_spend") > 0, "Low")
         .otherwise("N/A - No purchases")
    ).withColumn(
        "priority_rank",
        F.when(
            (F.col("expansion_opportunity") == "High - Greenfield") &
            (F.col("propensity_tier") == "High"), 1
        ).when(
            (F.col("expansion_opportunity").isin(["High - Greenfield", "Medium - Expand"])) &
            (F.col("propensity_tier").isin(["High", "Medium"])), 2
        ).when(
            F.col("propensity_tier") == "High", 3
        ).otherwise(4)
    ).withColumn(
        "recommended_play",
        F.when(F.col("priority_rank") == 1, "Executive sponsorship + multi-product pitch")
         .when(F.col("priority_rank") == 2, "Solution expansion workshop")
         .when(F.col("priority_rank") == 3, "Use case discovery session")
         .otherwise("Nurture with thought leadership")
    ).withColumn(
        "snapshot_date",
        F.lit(str(datetime.now().date()))
    ).select(
        "account_id",
        "account_name",
        "industry",
        "territory",
        "propensity_score",
        "propensity_tier",
        "total_spend",
        "estimated_potential",
        "white_space_value",
        "wallet_share",
        "expansion_opportunity",
        "products_purchased_count",
        "products_available",
        "cross_sell_potential",
        "upsell_potential",
        "priority_rank",
        "recommended_play",
        "open_pipeline",
        "snapshot_date",
    )

    output.write_dataframe(result)


# =============================================================================
# 4. TERRITORY SUMMARY
# =============================================================================

@transform(
    territory_balance=Input("/RevOps/Analytics/territory_balance"),
    white_space=Input("/RevOps/Analytics/white_space_analysis"),
    account_scores=Input("/RevOps/Analytics/account_scores"),
    output=Output("/RevOps/Analytics/territory_summary")
)
def compute_territory_summary(ctx, territory_balance, white_space, account_scores, output):
    """
    Executive summary of territory and account planning metrics.
    """
    balance_df = territory_balance.dataframe()
    white_space_df = white_space.dataframe()
    scores_df = account_scores.dataframe()

    # Territory aggregates
    territory_agg = balance_df.agg(
        F.count("*").alias("total_territories"),
        F.sum("account_count").alias("total_accounts"),
        F.sum("high_propensity_accounts").alias("total_high_propensity"),
        F.sum("total_pipeline").alias("total_pipeline"),
        F.sum("quota").alias("total_quota"),
        F.avg("coverage_ratio").alias("avg_coverage"),
        F.sum(F.when(F.col("territory_health") == "Strong", 1).otherwise(0)).alias("strong_territories"),
        F.sum(F.when(F.col("territory_health") == "Critical", 1).otherwise(0)).alias("critical_territories"),
    ).collect()[0]

    # White space aggregates
    ws_agg = white_space_df.agg(
        F.sum("white_space_value").alias("total_white_space"),
        F.sum(F.when(F.col("expansion_opportunity") == "High - Greenfield", F.col("white_space_value")).otherwise(0)).alias("greenfield_value"),
        F.sum(F.when(F.col("priority_rank") == 1, 1).otherwise(0)).alias("priority_1_accounts"),
        F.avg("wallet_share").alias("avg_wallet_share"),
    ).collect()[0]

    # Account score distribution
    score_dist = scores_df.agg(
        F.avg("propensity_score").alias("avg_propensity"),
        F.sum(F.when(F.col("propensity_tier") == "High", 1).otherwise(0)).alias("high_tier_count"),
        F.sum(F.when(F.col("propensity_tier") == "Medium", 1).otherwise(0)).alias("medium_tier_count"),
        F.sum(F.when(F.col("propensity_tier") == "Low", 1).otherwise(0)).alias("low_tier_count"),
    ).collect()[0]

    # Determine overall health
    coverage = territory_agg["avg_coverage"] or 0
    critical_pct = (territory_agg["critical_territories"] or 0) / max(territory_agg["total_territories"] or 1, 1)

    if coverage >= 3 and critical_pct < 0.1:
        overall_health = "Strong"
        health_grade = "A"
    elif coverage >= 2 and critical_pct < 0.2:
        overall_health = "Healthy"
        health_grade = "B"
    elif coverage >= 1.5:
        overall_health = "At Risk"
        health_grade = "C"
    else:
        overall_health = "Critical"
        health_grade = "D"

    # Build summary
    result = ctx.spark_session.createDataFrame([{
        # Territory metrics
        "total_territories": territory_agg["total_territories"] or 0,
        "total_accounts": territory_agg["total_accounts"] or 0,
        "total_pipeline": territory_agg["total_pipeline"] or 0,
        "total_quota": territory_agg["total_quota"] or 0,
        "avg_coverage_ratio": round(coverage, 2),
        "strong_territories": territory_agg["strong_territories"] or 0,
        "critical_territories": territory_agg["critical_territories"] or 0,

        # Account scoring
        "high_propensity_accounts": score_dist["high_tier_count"] or 0,
        "medium_propensity_accounts": score_dist["medium_tier_count"] or 0,
        "low_propensity_accounts": score_dist["low_tier_count"] or 0,
        "avg_propensity_score": round(score_dist["avg_propensity"] or 0, 1),

        # White space
        "total_white_space_value": ws_agg["total_white_space"] or 0,
        "greenfield_opportunity": ws_agg["greenfield_value"] or 0,
        "priority_accounts": ws_agg["priority_1_accounts"] or 0,
        "avg_wallet_share": round(ws_agg["avg_wallet_share"] or 0, 1),

        # Health
        "overall_health": overall_health,
        "health_grade": health_grade,

        # Insights
        "insight_1": f"Total addressable white space of ${(ws_agg['total_white_space'] or 0)/1000000:.1f}M across {territory_agg['total_accounts'] or 0} accounts.",
        "insight_2": f"{score_dist['high_tier_count'] or 0} high-propensity accounts identified for prioritized outreach.",
        "insight_3": f"{territory_agg['critical_territories'] or 0} territories need immediate attention due to low coverage.",

        # Recommendations
        "recommendation_1": "Focus expansion efforts on priority 1 accounts with high propensity and low wallet share." if (ws_agg["priority_1_accounts"] or 0) > 0 else "Build pipeline in underpenetrated accounts.",
        "recommendation_2": "Rebalance territories with >30% deviation from average account load." if critical_pct > 0.15 else "Maintain current territory assignments.",
        "recommendation_3": "Accelerate deals in territories with <2x coverage to reduce risk." if coverage < 2 else "Continue pipeline generation across all territories.",

        "snapshot_date": str(datetime.now().date()),
        "generated_at": str(datetime.now()),
    }])

    output.write_dataframe(result)
