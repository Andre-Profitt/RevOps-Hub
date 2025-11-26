# /transforms/scenario/generate_process_events.py
"""
PROCESS MINING: Event Log Generator
====================================
Creates event logs that show the sales process with discoverable patterns:
- Happy path vs deviations
- Bottlenecks (Legal review taking too long)
- Rework loops (Proposal → Discovery → Proposal)
- Stage skip anomalies

This data enables process mining visualizations and bottleneck detection.
"""

from transforms.api import transform, Input, Output
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, IntegerType, BooleanType
)
import random
from datetime import datetime, timedelta


@transform(
    opportunities=Input("/RevOps/Scenario/opportunities"),
    output=Output("/RevOps/Scenario/process_events")
)
def compute(ctx, opportunities, output):
    """Generate process mining event logs for each opportunity."""

    spark = ctx.spark_session
    random.seed(42)

    opps_df = opportunities.dataframe().collect()

    # ===========================================
    # PROCESS DEFINITION
    # ===========================================

    # Standard sales stages in order
    STAGES = [
        "Lead Created",
        "Qualified",
        "Discovery",
        "Solution Design",
        "Proposal",
        "Negotiation",
        "Legal Review",
        "Security Review",  # For enterprise deals
        "Procurement",      # Sometimes added
        "Closed Won",
        "Closed Lost"
    ]

    # Benchmark durations (days)
    STAGE_BENCHMARKS = {
        "Lead Created": 1,
        "Qualified": 3,
        "Discovery": 14,
        "Solution Design": 21,
        "Proposal": 10,
        "Negotiation": 14,
        "Legal Review": 5,      # Target
        "Security Review": 3,   # Target
        "Procurement": 7,
    }

    # Actual durations (showing bottlenecks)
    STAGE_ACTUALS = {
        "Lead Created": (1, 2),
        "Qualified": (2, 5),
        "Discovery": (10, 21),
        "Solution Design": (14, 35),
        "Proposal": (7, 18),
        "Negotiation": (10, 28),
        "Legal Review": (8, 18),      # BOTTLENECK: Taking longer!
        "Security Review": (5, 12),   # BOTTLENECK
        "Procurement": (7, 21),
    }

    events = []
    event_id = 1

    for opp in opps_df:
        case_id = opp.opportunity_id
        amount = opp.amount
        is_enterprise = opp.segment == "Enterprise"

        # Determine the path based on outcome
        if opp.stage_name == "Closed Won":
            # Full happy path
            path = ["Lead Created", "Qualified", "Discovery", "Solution Design",
                    "Proposal", "Negotiation"]
            if is_enterprise:
                path.extend(["Legal Review", "Security Review"])
            else:
                path.append("Legal Review")

            # Add procurement for big deals
            if amount >= 500000 and random.random() < 0.6:
                path.append("Procurement")

            path.append("Closed Won")

            # Add rework for some deals (realistic)
            if random.random() < 0.2:
                # Proposal rework
                idx = path.index("Proposal")
                path.insert(idx + 1, "Discovery")  # Went back to discovery
                path.insert(idx + 2, "Proposal")   # Then back to proposal

        elif opp.stage_name == "Closed Lost":
            # Partial path - died at various stages
            death_stage = random.choice([
                "Discovery", "Discovery", "Solution Design", "Solution Design",
                "Proposal", "Proposal", "Negotiation"
            ])
            path = ["Lead Created", "Qualified"]

            stage_idx = STAGES.index(death_stage)
            for stage in STAGES[2:stage_idx + 1]:
                if stage not in ["Legal Review", "Security Review", "Procurement", "Closed Won", "Closed Lost"]:
                    path.append(stage)

            path.append("Closed Lost")

        else:
            # Open deal - path up to current stage
            current_stage_map = {
                "Prospecting": "Qualified",
                "Discovery": "Discovery",
                "Solution Design": "Solution Design",
                "Proposal": "Proposal",
                "Negotiation": "Negotiation",
                "Verbal Commit": "Negotiation",
            }
            end_stage = current_stage_map.get(opp.stage_name, "Discovery")

            path = ["Lead Created", "Qualified"]
            for stage in ["Discovery", "Solution Design", "Proposal", "Negotiation"]:
                path.append(stage)
                if stage == end_stage:
                    break

        # ===========================================
        # GENERATE EVENTS WITH TIMESTAMPS
        # ===========================================

        # Start from created date
        current_time = datetime.combine(opp.created_date, datetime.min.time())

        prev_stage = None
        for stage in path:
            # Duration in this stage
            if stage in STAGE_ACTUALS:
                min_days, max_days = STAGE_ACTUALS[stage]

                # Add extra time for bottleneck stages (Legal, Security)
                if stage == "Legal Review" and random.random() < 0.4:
                    # 40% of deals hit legal bottleneck
                    duration = random.randint(12, 18)  # Much longer than target
                    is_bottleneck = True
                elif stage == "Security Review" and is_enterprise and random.random() < 0.3:
                    duration = random.randint(8, 14)
                    is_bottleneck = True
                else:
                    duration = random.randint(min_days, max_days)
                    is_bottleneck = False
            else:
                duration = random.randint(1, 3)
                is_bottleneck = False

            # Record event
            events.append(Row(
                event_id=f"EVT-{event_id:08d}",
                case_id=case_id,
                activity=stage,
                timestamp=current_time,
                resource=opp.owner_name,
                resource_id=opp.owner_id,
                account_name=opp.account_name,
                amount=opp.amount,
                segment=opp.segment,
                duration_days=duration,
                is_bottleneck=is_bottleneck,
                is_rework=stage == prev_stage,  # Went back to same stage
                previous_activity=prev_stage,
                benchmark_days=STAGE_BENCHMARKS.get(stage, 0),
                deviation_days=duration - STAGE_BENCHMARKS.get(stage, 0) if stage in STAGE_BENCHMARKS else 0,
            ))

            event_id += 1
            current_time += timedelta(days=duration)
            prev_stage = stage

    # ===========================================
    # CREATE DATAFRAME
    # ===========================================

    schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("case_id", StringType(), True),
        StructField("activity", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("resource", StringType(), True),
        StructField("resource_id", StringType(), True),
        StructField("account_name", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("segment", StringType(), True),
        StructField("duration_days", IntegerType(), True),
        StructField("is_bottleneck", BooleanType(), True),
        StructField("is_rework", BooleanType(), True),
        StructField("previous_activity", StringType(), True),
        StructField("benchmark_days", IntegerType(), True),
        StructField("deviation_days", IntegerType(), True),
    ])

    df = spark.createDataFrame(events, schema)
    output.write_dataframe(df)
