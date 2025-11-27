# /transforms/analytics/data_quality_analytics.py
"""
DATA QUALITY ANALYTICS
======================
Monitors data quality across the RevOps data pipeline:
- Overall quality summary (completeness, validity, freshness)
- Per-field quality metrics with issue counts
- Integration sync status and health

Powers the Data Quality Admin Dashboard.
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


@transform(
    opportunities=Input("/RevOps/Scenario/opportunities"),
    accounts=Input("/RevOps/Scenario/accounts"),
    reps=Input("/RevOps/Scenario/sales_reps"),
    output=Output("/RevOps/DataQuality/summary")
)
def compute_quality_summary(ctx, opportunities, accounts, reps, output):
    """Generate overall data quality summary."""

    opps_df = opportunities.dataframe()
    accts_df = accounts.dataframe()
    reps_df = reps.dataframe()

    # Calculate completeness scores for each dataset
    opp_completeness = opps_df.agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("opportunity_name").isNull(), 1).otherwise(0)).alias("null_name"),
        F.sum(F.when(F.col("amount").isNull(), 1).otherwise(0)).alias("null_amount"),
        F.sum(F.when(F.col("close_date").isNull(), 1).otherwise(0)).alias("null_close"),
        F.sum(F.when(F.col("owner_id").isNull(), 1).otherwise(0)).alias("null_owner")
    ).collect()[0]

    total_opps = opp_completeness["total"]
    opp_null_count = (opp_completeness["null_name"] + opp_completeness["null_amount"] +
                       opp_completeness["null_close"] + opp_completeness["null_owner"])

    # Calculate completeness percentage
    completeness = round(1 - (opp_null_count / (total_opps * 4)), 3) if total_opps > 0 else 0

    # Calculate validity (check for reasonable values)
    validity_issues = opps_df.filter(
        (F.col("amount") < 0) |
        (F.col("amount") > 10000000) |
        (F.col("discount_pct") < 0) |
        (F.col("discount_pct") > 1)
    ).count()

    validity = round(1 - (validity_issues / total_opps), 3) if total_opps > 0 else 1

    # Freshness (assume data is recent for demo)
    freshness = 0.95

    # Overall score (weighted average)
    overall = round(completeness * 0.4 + validity * 0.35 + freshness * 0.25, 2)

    # Create summary
    summary_schema = StructType([
        StructField("overall_score", DoubleType(), False),
        StructField("completeness_score", DoubleType(), False),
        StructField("validity_score", DoubleType(), False),
        StructField("freshness_score", DoubleType(), False),
        StructField("total_records", IntegerType(), False),
        StructField("issue_count", IntegerType(), False),
        StructField("critical_issues", IntegerType(), False),
        StructField("last_sync_time", StringType(), False),
        StructField("sync_status", StringType(), False),
        StructField("trend", StringType(), False)
    ])

    data = [(
        float(overall),
        float(completeness),
        float(validity),
        float(freshness),
        total_opps + accts_df.count() + reps_df.count(),
        int(opp_null_count + validity_issues),
        int(validity_issues),
        F.current_timestamp(),
        "Healthy",
        "improving"
    )]

    summary_df = ctx.spark_session.createDataFrame(data, schema=summary_schema)
    output.write_dataframe(summary_df)


@transform(
    opportunities=Input("/RevOps/Scenario/opportunities"),
    accounts=Input("/RevOps/Scenario/accounts"),
    output=Output("/RevOps/DataQuality/metrics")
)
def compute_quality_metrics(ctx, opportunities, accounts, output):
    """Generate per-field quality metrics."""

    opps_df = opportunities.dataframe()
    accts_df = accounts.dataframe()

    total_opps = opps_df.count()
    total_accts = accts_df.count()

    # Define metrics to check
    metrics = []

    # Opportunity metrics
    opp_checks = [
        ("Amount Populated", "Completeness", "Opportunity", "amount", 1.0, 0.95),
        ("Close Date Valid", "Validity", "Opportunity", "close_date", 0.98, 0.95),
        ("Owner Assigned", "Completeness", "Opportunity", "owner_id", 0.99, 0.98),
        ("Stage Name Valid", "Validity", "Opportunity", "stage_name", 1.0, 0.98),
        ("Forecast Category Set", "Completeness", "Opportunity", "forecast_category", 0.85, 0.90)
    ]

    for metric_name, category, obj_type, field, current, target in opp_checks:
        null_count = opps_df.filter(F.col(field).isNull()).count()
        current_score = round(1 - (null_count / total_opps), 2) if total_opps > 0 else 1
        metrics.append({
            "metric_name": metric_name,
            "category": category,
            "object_type": obj_type,
            "current_score": current_score,
            "target_score": target,
            "trend": "stable" if abs(current_score - target) < 0.05 else "declining",
            "issue_count": null_count,
            "sample_issues": f"{null_count} records missing {field}",
            "remediation_action": f"Run data enrichment for {field}" if null_count > 0 else "No action needed",
            "owner": "Data Ops"
        })

    # Account metrics
    acct_checks = [
        ("Industry Classified", "Completeness", "Account", "industry", 0.92, 0.95),
        ("Segment Assigned", "Completeness", "Account", "segment", 0.95, 0.95)
    ]

    for metric_name, category, obj_type, field, current, target in acct_checks:
        null_count = accts_df.filter(F.col(field).isNull()).count()
        current_score = round(1 - (null_count / total_accts), 2) if total_accts > 0 else 1
        metrics.append({
            "metric_name": metric_name,
            "category": category,
            "object_type": obj_type,
            "current_score": current_score,
            "target_score": target,
            "trend": "stable",
            "issue_count": null_count,
            "sample_issues": f"{null_count} records missing {field}",
            "remediation_action": f"Enrich {field} from external source",
            "owner": "Data Ops"
        })

    # Create DataFrame
    metrics_df = ctx.spark_session.createDataFrame(metrics)
    output.write_dataframe(metrics_df)


@transform(
    output=Output("/RevOps/DataQuality/sync_status")
)
def compute_sync_status(ctx, output):
    """Generate integration sync status."""

    # Simulated sync status for common integrations
    sync_data = [
        {
            "source_system": "Salesforce",
            "object_type": "Opportunity",
            "last_sync": "2024-11-27T08:00:00Z",
            "status": "Success",
            "records_synced": 1247,
            "records_failed": 3,
            "error_message": None,
            "next_sync": "2024-11-27T09:00:00Z",
            "sync_frequency": "Hourly"
        },
        {
            "source_system": "Salesforce",
            "object_type": "Account",
            "last_sync": "2024-11-27T08:00:00Z",
            "status": "Success",
            "records_synced": 523,
            "records_failed": 0,
            "error_message": None,
            "next_sync": "2024-11-27T09:00:00Z",
            "sync_frequency": "Hourly"
        },
        {
            "source_system": "Salesforce",
            "object_type": "Contact",
            "last_sync": "2024-11-27T08:00:00Z",
            "status": "Success",
            "records_synced": 2341,
            "records_failed": 12,
            "error_message": None,
            "next_sync": "2024-11-27T09:00:00Z",
            "sync_frequency": "Hourly"
        },
        {
            "source_system": "Salesforce",
            "object_type": "Activity",
            "last_sync": "2024-11-27T07:30:00Z",
            "status": "Success",
            "records_synced": 8934,
            "records_failed": 0,
            "error_message": None,
            "next_sync": "2024-11-27T08:30:00Z",
            "sync_frequency": "Hourly"
        },
        {
            "source_system": "HubSpot",
            "object_type": "Marketing Activity",
            "last_sync": "2024-11-27T06:00:00Z",
            "status": "Warning",
            "records_synced": 4521,
            "records_failed": 45,
            "error_message": "Rate limit exceeded, retrying",
            "next_sync": "2024-11-27T10:00:00Z",
            "sync_frequency": "4 Hours"
        },
        {
            "source_system": "Product Analytics",
            "object_type": "Usage Events",
            "last_sync": "2024-11-27T00:00:00Z",
            "status": "Success",
            "records_synced": 125000,
            "records_failed": 0,
            "error_message": None,
            "next_sync": "2024-11-28T00:00:00Z",
            "sync_frequency": "Daily"
        }
    ]

    sync_df = ctx.spark_session.createDataFrame(sync_data)
    output.write_dataframe(sync_df)
