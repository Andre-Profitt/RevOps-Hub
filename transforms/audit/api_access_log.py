# /transforms/audit/api_access_log.py
"""
API ACCESS LOG GENERATOR
========================
Generates synthetic API access logs for usage metering.
In production, this would be replaced by actual API gateway logs.

Output:
- /RevOps/Audit/api_access_log
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


@transform(
    accounts=Input("/RevOps/Scenario/accounts"),
    output=Output("/RevOps/Audit/api_access_log")
)
def generate_api_access_log(ctx, accounts, output):
    """Generate synthetic API access logs for metering."""

    accts = accounts.dataframe()

    # Create base API log entries
    api_endpoints = ["query", "search", "export", "sync", "webhook", "report"]

    # Generate multiple log entries per customer
    base = accts.select(
        F.col("account_id").alias("customer_id")
    ).distinct()

    # Explode to create multiple entries
    logs = base.withColumn(
        "entry_count",
        (F.rand() * 100 + 10).cast("int")
    ).withColumn(
        "entries",
        F.expr("sequence(1, entry_count)")
    ).withColumn(
        "entry",
        F.explode("entries")
    ).drop("entry_count", "entries")

    # Add log fields
    enriched = logs.withColumn(
        "log_id",
        F.concat(F.col("customer_id"), F.lit("-"), F.monotonically_increasing_id())
    ).withColumn(
        "endpoint",
        F.element_at(
            F.array([F.lit(e) for e in api_endpoints]),
            (F.rand() * len(api_endpoints)).cast("int") + 1
        )
    ).withColumn(
        "method",
        F.when(F.col("endpoint").isin("query", "search", "export", "report"), "GET")
        .otherwise("POST")
    ).withColumn(
        "status_code",
        F.when(F.rand() < 0.95, 200)
        .when(F.rand() < 0.97, 400)
        .when(F.rand() < 0.99, 429)  # Rate limited
        .otherwise(500)
    ).withColumn(
        "response_time_ms",
        (F.rand() * 500 + 50).cast("int")
    ).withColumn(
        "request_size_bytes",
        (F.rand() * 10000 + 100).cast("int")
    ).withColumn(
        "response_size_bytes",
        (F.rand() * 100000 + 1000).cast("int")
    ).withColumn(
        "timestamp",
        F.current_timestamp() - F.expr("make_interval(0, 0, 0, 0, 0, cast(rand() * 86400 * 30 as int))")
    ).withColumn(
        "user_agent",
        F.when(F.rand() < 0.6, "RevOps-SDK/1.0")
        .when(F.rand() < 0.8, "Python-requests/2.28")
        .otherwise("curl/7.79")
    ).withColumn(
        "ip_address",
        F.concat(
            (F.rand() * 200 + 10).cast("int"),
            F.lit("."),
            (F.rand() * 255).cast("int"),
            F.lit("."),
            (F.rand() * 255).cast("int"),
            F.lit("."),
            (F.rand() * 255).cast("int")
        )
    ).drop("entry")

    output.write_dataframe(enriched)
