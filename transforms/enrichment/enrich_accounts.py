# /transforms/enrichment/enrich_accounts.py
"""
ACCOUNT ENRICHMENT
==================
Enriches account data with derived fields for analytics.
Used by customer health and expansion analytics.

Output:
- /RevOps/Enriched/accounts
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F


@transform(
    accounts=Input("/RevOps/Scenario/accounts"),
    opportunities=Input("/RevOps/Scenario/opportunities"),
    output=Output("/RevOps/Enriched/accounts")
)
def enrich_accounts(ctx, accounts, opportunities, output):
    """Enrich accounts with derived metrics."""

    accts = accounts.dataframe()
    opps = opportunities.dataframe()

    # Calculate account-level opportunity metrics
    opp_metrics = opps.groupBy("account_id").agg(
        F.count("*").alias("total_opportunities"),
        F.sum(F.when(F.col("is_won"), F.col("amount")).otherwise(0)).alias("closed_won_amount"),
        F.sum(F.when(~F.col("is_closed"), F.col("amount")).otherwise(0)).alias("open_pipeline"),
        F.avg(F.when(F.col("is_won"), F.col("amount"))).alias("avg_won_deal_size"),
        F.sum(F.when(F.col("is_won"), 1).otherwise(0)).alias("won_deals"),
        F.sum(F.when(F.col("is_closed") & ~F.col("is_won"), 1).otherwise(0)).alias("lost_deals"),
        F.max("close_date").alias("last_close_date"),
    )

    # Calculate win rate per account
    opp_metrics_enriched = opp_metrics.withColumn(
        "account_win_rate",
        F.when(
            (F.col("won_deals") + F.col("lost_deals")) > 0,
            F.round(F.col("won_deals") / (F.col("won_deals") + F.col("lost_deals")) * 100, 1)
        ).otherwise(F.lit(None))
    )

    # Join with accounts
    enriched = accts.join(
        opp_metrics_enriched,
        "account_id",
        "left"
    )

    # Add derived fields
    final = enriched.withColumn(
        "customer_since",
        F.when(F.col("closed_won_amount") > 0, F.col("created_date")).otherwise(F.lit(None))
    ).withColumn(
        "is_customer",
        F.col("closed_won_amount") > 0
    ).withColumn(
        "account_tier",
        F.when(F.col("closed_won_amount") >= 500000, "Strategic")
        .when(F.col("closed_won_amount") >= 100000, "Key")
        .when(F.col("closed_won_amount") > 0, "Standard")
        .otherwise("Prospect")
    ).withColumn(
        "expansion_potential_score",
        F.when(
            (F.col("is_customer")) &
            (F.coalesce(F.col("account_win_rate"), F.lit(0)) >= 50) &
            (F.coalesce(F.col("open_pipeline"), F.lit(0)) > 0),
            "High"
        ).when(
            F.col("is_customer"),
            "Medium"
        ).otherwise("Low")
    ).withColumn(
        "_enriched_at",
        F.current_timestamp()
    )

    output.write_dataframe(final)
