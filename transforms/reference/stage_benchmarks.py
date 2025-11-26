# /transforms/reference/stage_benchmarks.py
"""
STAGE BENCHMARKS REFERENCE DATA
===============================
Defines expected durations and conversion rates for each sales stage.
Used by health score calculations and process mining analysis.
"""

from transforms.api import transform, Output
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
)


@transform(
    output=Output("/RevOps/Reference/stage_benchmarks")
)
def compute(ctx, output):
    """Generate stage benchmark reference data."""

    spark = ctx.spark_session

    BENCHMARKS = [
        # stage_name, expected_days, max_acceptable_days, conversion_rate, stage_order
        ("Prospecting", 7, 14, 0.80, 1),
        ("Discovery", 14, 21, 0.70, 2),
        ("Solution Design", 21, 35, 0.65, 3),
        ("Proposal", 10, 18, 0.60, 4),
        ("Negotiation", 14, 28, 0.75, 5),
        ("Verbal Commit", 5, 10, 0.90, 6),
        ("Legal Review", 5, 10, 0.95, 7),
        ("Security Review", 3, 7, 0.98, 8),
        ("Procurement", 7, 14, 0.95, 9),
    ]

    rows = [
        Row(
            stage_name=b[0],
            expected_days=b[1],
            max_acceptable_days=b[2],
            historical_conversion_rate=b[3],
            stage_order=b[4],
            is_sales_stage=b[4] <= 6,
            is_admin_stage=b[4] > 6,
            stall_threshold_days=int(b[1] * 1.5),
            critical_threshold_days=b[1] * 2
        )
        for b in BENCHMARKS
    ]

    schema = StructType([
        StructField("stage_name", StringType(), False),
        StructField("expected_days", IntegerType(), True),
        StructField("max_acceptable_days", IntegerType(), True),
        StructField("historical_conversion_rate", DoubleType(), True),
        StructField("stage_order", IntegerType(), True),
        StructField("is_sales_stage", BooleanType(), True),
        StructField("is_admin_stage", BooleanType(), True),
        StructField("stall_threshold_days", IntegerType(), True),
        StructField("critical_threshold_days", IntegerType(), True),
    ])

    df = spark.createDataFrame(rows, schema)
    output.write_dataframe(df)
