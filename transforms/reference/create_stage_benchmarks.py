# /transforms/reference/create_stage_benchmarks.py
"""
REFERENCE DATA: Stage Benchmarks
=================================
Defines the expected duration for each sales stage.
Used by health scoring to determine if deals are on track.
"""

from transforms.api import transform, Output
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)


@transform(
    output=Output("/RevOps/Reference/stage_benchmarks")
)
def compute(ctx, output):
    """
    Create stage duration benchmarks.

    These benchmarks represent the expected number of days a deal
    should spend in each stage before progressing.

    In production, these would be calculated from historical data
    and segmented by deal size, industry, etc.
    """

    spark = ctx.spark_session

    benchmarks = [
        Row(
            stage="Prospecting",
            benchmark_days=14,
            max_healthy_days=21,
            typical_exit_rate=0.30,
            description="Initial outreach and qualification"
        ),
        Row(
            stage="Discovery",
            benchmark_days=21,
            max_healthy_days=35,
            typical_exit_rate=0.50,
            description="Understanding customer needs and pain points"
        ),
        Row(
            stage="Solution Design",
            benchmark_days=28,
            max_healthy_days=45,
            typical_exit_rate=0.60,
            description="Crafting the solution and building the business case"
        ),
        Row(
            stage="Proposal",
            benchmark_days=14,
            max_healthy_days=25,
            typical_exit_rate=0.70,
            description="Formal proposal and pricing delivery"
        ),
        Row(
            stage="Negotiation",
            benchmark_days=21,
            max_healthy_days=35,
            typical_exit_rate=0.80,
            description="Contract negotiation and legal review"
        ),
        Row(
            stage="Verbal Commit",
            benchmark_days=7,
            max_healthy_days=14,
            typical_exit_rate=0.95,
            description="Awaiting signature and final approval"
        ),
    ]

    schema = StructType([
        StructField("stage", StringType(), False),
        StructField("benchmark_days", IntegerType(), True),
        StructField("max_healthy_days", IntegerType(), True),
        StructField("typical_exit_rate", DoubleType(), True),
        StructField("description", StringType(), True),
    ])

    df = spark.createDataFrame(benchmarks, schema)
    output.write_dataframe(df)
