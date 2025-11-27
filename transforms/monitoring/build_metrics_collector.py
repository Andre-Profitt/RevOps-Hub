"""
BUILD METRICS COLLECTOR
=======================
Collects build metrics (timestamps, record counts, schema snapshots, latency)
from all datasets and writes to monitoring datasets for freshness tracking
and AIOps telemetry.

Run after each build cycle to update the metrics.

Enhanced for Phase 1 AIOps: Instrumentation & Telemetry
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType,
    ArrayType, MapType, DoubleType, BooleanType
)
from datetime import datetime
import json
import traceback
import time
import uuid


# List of all datasets to monitor
MONITORED_DATASETS = [
    # Dashboard
    "/RevOps/Dashboard/kpis",
    "/RevOps/Dashboard/next_best_actions",

    # Analytics
    "/RevOps/Analytics/pipeline_health_summary",
    "/RevOps/Analytics/pipeline_hygiene_summary",
    "/RevOps/Analytics/pipeline_hygiene_alerts",
    "/RevOps/Analytics/rep_performance",
    "/RevOps/Analytics/deal_predictions",
    "/RevOps/Analytics/leading_indicators_summary",
    "/RevOps/Analytics/telemetry_summary",
    "/RevOps/Analytics/funnel_handoff_metrics",
    "/RevOps/Analytics/process_bottlenecks",
    "/RevOps/Analytics/forecast_summary",
    "/RevOps/Analytics/forecast_by_segment",
    "/RevOps/Analytics/forecast_history",
    "/RevOps/Analytics/forecast_accuracy",
    "/RevOps/Analytics/win_loss_summary",
    "/RevOps/Analytics/loss_reasons",
    "/RevOps/Analytics/win_factors",
    "/RevOps/Analytics/competitive_battles",
    "/RevOps/Analytics/territory_summary",
    "/RevOps/Analytics/account_scores",
    "/RevOps/Analytics/white_space_analysis",
    "/RevOps/Analytics/customer_health_summary",
    "/RevOps/Analytics/customer_health_scores",
    "/RevOps/Analytics/churn_risk_analysis",
    "/RevOps/Analytics/expansion_opportunities",
    "/RevOps/Analytics/scenario_summary",
    "/RevOps/Analytics/qbr_executive_summary",
    "/RevOps/Analytics/capacity_planning_summary",
    "/RevOps/Analytics/rep_capacity_model",
    "/RevOps/Analytics/team_capacity_summary",

    # Enriched
    "/RevOps/Enriched/deal_health_scores",
]


def _extract_schema_snapshot(df) -> dict:
    """Extract schema information as a serializable dict."""
    columns = []
    for field in df.schema.fields:
        columns.append({
            "name": field.name,
            "type": str(field.dataType),
            "nullable": field.nullable
        })
    return {
        "column_count": len(columns),
        "columns": columns
    }


def _compute_data_stats(df) -> dict:
    """Compute basic data statistics for quality monitoring."""
    stats = {"null_counts": {}, "distinct_counts": {}}
    try:
        # Sample for performance - compute on subset
        sample_df = df.limit(10000)
        for field in df.schema.fields[:10]:  # Limit to first 10 columns
            col_name = field.name
            null_count = sample_df.filter(F.col(col_name).isNull()).count()
            stats["null_counts"][col_name] = null_count
    except Exception:
        pass
    return stats


@transform(
    output=Output("/RevOps/Monitoring/build_metrics")
)
def collect_build_metrics(ctx, output):
    """
    Collect enhanced build metrics from all monitored datasets.

    For each dataset, records:
    - dataset_path: The dataset path
    - last_build_ts: Timestamp when this metrics collection ran
    - record_count: Number of rows in the dataset
    - status: 'ok', 'empty', or 'error'
    - schema_snapshot: JSON of column names and types
    - read_latency_ms: Time taken to read the dataset
    - error_details: Full error message and stack trace if failed
    - column_count: Number of columns in the dataset
    - sample_null_pct: Percentage of null values in sample
    """

    spark = ctx.spark_session
    metrics = []
    collection_time = datetime.now()
    run_id = str(uuid.uuid4())[:8]

    for dataset_path in MONITORED_DATASETS:
        start_time = time.time()
        schema_json = "{}"
        error_details = None
        column_count = 0
        sample_null_pct = 0.0

        try:
            # Try to read the dataset with timing
            df = spark.read.format("foundry").load(dataset_path)
            read_latency_ms = (time.time() - start_time) * 1000

            # Get schema snapshot
            schema_snapshot = _extract_schema_snapshot(df)
            schema_json = json.dumps(schema_snapshot)
            column_count = schema_snapshot["column_count"]

            # Get count with timing
            count_start = time.time()
            count = df.count()
            count_latency_ms = (time.time() - count_start) * 1000
            total_latency_ms = read_latency_ms + count_latency_ms

            # Compute data stats for quality monitoring
            if count > 0:
                data_stats = _compute_data_stats(df)
                total_nulls = sum(data_stats["null_counts"].values())
                total_cells = count * min(10, column_count)
                sample_null_pct = (total_nulls / total_cells * 100) if total_cells > 0 else 0.0

            metrics.append({
                "dataset_path": dataset_path,
                "last_build_ts": collection_time,
                "record_count": count,
                "status": "ok" if count > 0 else "empty",
                "schema_snapshot": schema_json,
                "read_latency_ms": total_latency_ms,
                "error_details": None,
                "column_count": column_count,
                "sample_null_pct": round(sample_null_pct, 2),
                "run_id": run_id
            })
        except Exception as e:
            read_latency_ms = (time.time() - start_time) * 1000
            error_details = json.dumps({
                "error_type": type(e).__name__,
                "error_message": str(e),
                "stack_trace": traceback.format_exc()[-500:]  # Last 500 chars
            })

            # Dataset doesn't exist or can't be read
            metrics.append({
                "dataset_path": dataset_path,
                "last_build_ts": collection_time,
                "record_count": 0,
                "status": "error",
                "schema_snapshot": schema_json,
                "read_latency_ms": read_latency_ms,
                "error_details": error_details,
                "column_count": 0,
                "sample_null_pct": 0.0,
                "run_id": run_id
            })

    # Enhanced schema with telemetry fields
    schema = StructType([
        StructField("dataset_path", StringType(), False),
        StructField("last_build_ts", TimestampType(), False),
        StructField("record_count", LongType(), False),
        StructField("status", StringType(), False),
        StructField("schema_snapshot", StringType(), True),
        StructField("read_latency_ms", DoubleType(), True),
        StructField("error_details", StringType(), True),
        StructField("column_count", LongType(), True),
        StructField("sample_null_pct", DoubleType(), True),
        StructField("run_id", StringType(), False),
    ])

    metrics_df = spark.createDataFrame(metrics, schema)

    # Add derived columns
    metrics_df = metrics_df.withColumn(
        "domain",
        F.split(F.col("dataset_path"), "/").getItem(2)
    ).withColumn(
        "is_healthy",
        F.when(F.col("status") == "ok", True).otherwise(False)
    ).withColumn(
        "is_slow",
        F.when(F.col("read_latency_ms") > 5000, True).otherwise(False)  # > 5 seconds
    ).withColumn(
        "has_quality_issues",
        F.when(F.col("sample_null_pct") > 20.0, True).otherwise(False)  # > 20% nulls
    )

    output.write_dataframe(metrics_df)


@transform(
    metrics=Input("/RevOps/Monitoring/build_metrics"),
    output=Output("/RevOps/Monitoring/stale_datasets")
)
def identify_stale_datasets(ctx, metrics, output):
    """
    Identify datasets that are stale (not updated recently).

    Flags datasets where last_build_ts is more than 2 hours old.
    """

    df = metrics.dataframe()
    current_time = F.current_timestamp()

    stale = df.withColumn(
        "age_hours",
        (F.unix_timestamp(current_time) - F.unix_timestamp(F.col("last_build_ts"))) / 3600
    ).filter(
        (F.col("age_hours") > 2) | (F.col("status") != "ok")
    ).withColumn(
        "alert_level",
        F.when(F.col("status") != "ok", "critical")
         .when(F.col("age_hours") > 24, "critical")
         .when(F.col("age_hours") > 6, "warning")
         .otherwise("info")
    )

    output.write_dataframe(stale)


@transform(
    metrics=Input("/RevOps/Monitoring/build_metrics"),
    output=Output("/RevOps/Monitoring/build_summary")
)
def build_summary(ctx, metrics, output):
    """
    Generate enhanced summary metrics for the build health dashboard.

    Includes:
    - Dataset counts by domain
    - Health status aggregations
    - Performance metrics (avg latency, slow datasets)
    - Quality metrics (high null percentage datasets)
    """

    df = metrics.dataframe()

    summary = df.groupBy("domain").agg(
        F.count("*").alias("total_datasets"),
        F.sum(F.when(F.col("is_healthy"), 1).otherwise(0)).alias("healthy_datasets"),
        F.sum(F.when(F.col("status") == "empty", 1).otherwise(0)).alias("empty_datasets"),
        F.sum(F.when(F.col("status") == "error", 1).otherwise(0)).alias("error_datasets"),
        F.sum("record_count").alias("total_records"),
        F.max("last_build_ts").alias("latest_build"),
        # Performance metrics
        F.avg("read_latency_ms").alias("avg_latency_ms"),
        F.max("read_latency_ms").alias("max_latency_ms"),
        F.sum(F.when(F.col("is_slow"), 1).otherwise(0)).alias("slow_datasets"),
        # Quality metrics
        F.avg("sample_null_pct").alias("avg_null_pct"),
        F.sum(F.when(F.col("has_quality_issues"), 1).otherwise(0)).alias("quality_issue_datasets"),
        F.sum("column_count").alias("total_columns"),
        F.max("run_id").alias("latest_run_id")
    ).withColumn(
        "health_pct",
        F.round(F.col("healthy_datasets") * 100 / F.col("total_datasets"), 1)
    ).withColumn(
        "performance_score",
        F.round(
            100 - (F.col("slow_datasets") * 10) - (F.col("avg_latency_ms") / 100),
            1
        )
    ).withColumn(
        "quality_score",
        F.round(
            100 - (F.col("quality_issue_datasets") * 10) - F.col("avg_null_pct"),
            1
        )
    )

    output.write_dataframe(summary)


# =============================================================================
# PIPELINE EVENTS - Build Event Streaming for AIOps
# =============================================================================

@transform(
    metrics=Input("/RevOps/Monitoring/build_metrics"),
    stale=Input("/RevOps/Monitoring/stale_datasets"),
    output=Output("/RevOps/Monitoring/pipeline_events")
)
def generate_pipeline_events(ctx, metrics, stale, output):
    """
    Generate pipeline events for AIOps agent consumption.

    Creates a stream of events that can be consumed by:
    - Ops Dashboard for real-time monitoring
    - AIOps Agent for automated remediation
    - Alert systems for notifications

    Event types:
    - BUILD_COMPLETE: Successful dataset build
    - BUILD_EMPTY: Build completed but no data
    - BUILD_ERROR: Build failed with error
    - DATASET_STALE: Dataset not refreshed within SLA
    - PERFORMANCE_DEGRADED: Build took longer than threshold
    - QUALITY_ALERT: Data quality issues detected
    """

    spark = ctx.spark_session
    collection_time = datetime.now()
    run_id = str(uuid.uuid4())[:8]

    events = []
    metrics_df = metrics.dataframe()
    stale_df = stale.dataframe()

    # Collect metrics as list for iteration
    metrics_rows = metrics_df.collect()

    for row in metrics_rows:
        base_event = {
            "event_id": str(uuid.uuid4()),
            "event_ts": collection_time,
            "run_id": row["run_id"],
            "dataset_path": row["dataset_path"],
            "domain": row["domain"],
        }

        # BUILD_COMPLETE / BUILD_EMPTY / BUILD_ERROR
        if row["status"] == "ok":
            events.append({
                **base_event,
                "event_type": "BUILD_COMPLETE",
                "severity": "info",
                "message": f"Dataset built successfully with {row['record_count']:,} records",
                "details": json.dumps({
                    "record_count": row["record_count"],
                    "column_count": row["column_count"],
                    "latency_ms": row["read_latency_ms"]
                }),
                "requires_action": False,
                "suggested_action": None
            })
        elif row["status"] == "empty":
            events.append({
                **base_event,
                "event_type": "BUILD_EMPTY",
                "severity": "warning",
                "message": f"Dataset built but contains no records",
                "details": json.dumps({
                    "column_count": row["column_count"],
                    "latency_ms": row["read_latency_ms"]
                }),
                "requires_action": True,
                "suggested_action": "CHECK_UPSTREAM_DATA"
            })
        else:  # error
            events.append({
                **base_event,
                "event_type": "BUILD_ERROR",
                "severity": "critical",
                "message": f"Dataset build failed",
                "details": row["error_details"],
                "requires_action": True,
                "suggested_action": "TRIGGER_REBUILD"
            })

        # PERFORMANCE_DEGRADED
        if row["is_slow"]:
            events.append({
                **base_event,
                "event_type": "PERFORMANCE_DEGRADED",
                "severity": "warning",
                "message": f"Build took {row['read_latency_ms']:.0f}ms (threshold: 5000ms)",
                "details": json.dumps({
                    "latency_ms": row["read_latency_ms"],
                    "threshold_ms": 5000
                }),
                "requires_action": True,
                "suggested_action": "OPTIMIZE_TRANSFORM"
            })

        # QUALITY_ALERT
        if row["has_quality_issues"]:
            events.append({
                **base_event,
                "event_type": "QUALITY_ALERT",
                "severity": "warning",
                "message": f"High null percentage detected: {row['sample_null_pct']:.1f}%",
                "details": json.dumps({
                    "null_pct": row["sample_null_pct"],
                    "threshold_pct": 20.0
                }),
                "requires_action": True,
                "suggested_action": "REVIEW_DATA_QUALITY"
            })

    # DATASET_STALE events from stale datasets
    stale_rows = stale_df.collect()
    for row in stale_rows:
        events.append({
            "event_id": str(uuid.uuid4()),
            "event_ts": collection_time,
            "run_id": run_id,
            "dataset_path": row["dataset_path"],
            "domain": row["domain"],
            "event_type": "DATASET_STALE",
            "severity": row["alert_level"],
            "message": f"Dataset is {row['age_hours']:.1f} hours old",
            "details": json.dumps({
                "age_hours": float(row["age_hours"]),
                "alert_level": row["alert_level"]
            }),
            "requires_action": True,
            "suggested_action": "TRIGGER_REBUILD"
        })

    # Create events DataFrame
    events_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("event_ts", TimestampType(), False),
        StructField("run_id", StringType(), False),
        StructField("dataset_path", StringType(), False),
        StructField("domain", StringType(), True),
        StructField("event_type", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("message", StringType(), False),
        StructField("details", StringType(), True),
        StructField("requires_action", BooleanType(), False),
        StructField("suggested_action", StringType(), True),
    ])

    events_df = spark.createDataFrame(events, events_schema)

    # Add priority score for agent consumption
    events_df = events_df.withColumn(
        "priority_score",
        F.when(F.col("severity") == "critical", 100)
         .when(F.col("severity") == "warning", 50)
         .when(F.col("severity") == "info", 10)
         .otherwise(0)
    ).withColumn(
        "action_status",
        F.lit("pending")  # pending, in_progress, completed, skipped
    )

    output.write_dataframe(events_df)


@transform(
    events=Input("/RevOps/Monitoring/pipeline_events"),
    output=Output("/RevOps/Monitoring/ops_agent_queue")
)
def build_ops_agent_queue(ctx, events, output):
    """
    Build a prioritized queue of actions for the Ops Agent.

    Filters to actionable events and ranks by priority for
    automated remediation processing.
    """

    df = events.dataframe()

    # Filter to events requiring action, sort by priority
    queue = df.filter(
        F.col("requires_action") == True
    ).select(
        "event_id",
        "event_ts",
        "dataset_path",
        "domain",
        "event_type",
        "severity",
        "message",
        "details",
        "suggested_action",
        "priority_score",
        "action_status"
    ).orderBy(
        F.col("priority_score").desc(),
        F.col("event_ts").desc()
    ).withColumn(
        "queue_position",
        F.row_number().over(
            F.Window.orderBy(F.col("priority_score").desc(), F.col("event_ts").desc())
        )
    ).withColumn(
        "estimated_resolution_min",
        F.when(F.col("suggested_action") == "TRIGGER_REBUILD", 15)
         .when(F.col("suggested_action") == "CHECK_UPSTREAM_DATA", 30)
         .when(F.col("suggested_action") == "OPTIMIZE_TRANSFORM", 60)
         .when(F.col("suggested_action") == "REVIEW_DATA_QUALITY", 45)
         .otherwise(30)
    )

    output.write_dataframe(queue)
