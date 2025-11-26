# /transforms/scenario/generate_scenario_activities.py
"""
SCENARIO DATA GENERATOR: Activities
====================================
Generates sales activity data (calls, emails, meetings) to support
the Q4 scenario narrative and demonstrate activity-based insights.
"""

from transforms.api import transform, Input, Output
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    IntegerType, BooleanType
)
import random
from datetime import datetime, timedelta


@transform(
    opportunities=Input("/RevOps/Scenario/opportunities"),
    output=Output("/RevOps/Scenario/activities")
)
def compute(ctx, opportunities, output):
    """Generate activity data based on opportunity patterns."""

    spark = ctx.spark_session
    random.seed(42)

    opps_df = opportunities.dataframe()
    opps_list = opps_df.collect()

    activities = []
    activity_counter = 1

    SCENARIO_DATE = datetime(2024, 11, 15).date()

    ACTIVITY_TYPES = ["Call", "Email", "Meeting", "Demo"]
    OUTCOMES = ["Completed", "No Answer", "Left Voicemail", "Rescheduled"]
    SUBJECTS = {
        "Call": ["Discovery call", "Follow-up call", "Check-in call", "Pricing discussion", "Technical Q&A"],
        "Email": ["Introduction email", "Proposal follow-up", "Meeting recap", "Contract review", "ROI summary"],
        "Meeting": ["Discovery meeting", "Demo session", "Executive briefing", "Technical deep-dive", "Contract negotiation"],
        "Demo": ["Product demo", "Technical demo", "Security walkthrough", "Integration demo"],
    }

    for opp in opps_list:
        opp_id = opp["opportunity_id"]
        account_id = opp["account_id"]
        owner_id = opp["owner_id"]
        is_closed = opp["is_closed"]
        health_score = opp["health_score_override"] or 70

        # Number of activities based on deal health and status
        if is_closed:
            # Closed deals have historical activities
            num_activities = random.randint(8, 20)
            days_range = 90
        elif health_score >= 80:
            # Healthy deals have high activity
            num_activities = random.randint(10, 15)
            days_range = 45
        elif health_score >= 60:
            # Monitor deals have moderate activity
            num_activities = random.randint(5, 10)
            days_range = 60
        else:
            # At-risk deals have low recent activity
            num_activities = random.randint(3, 6)
            days_range = 75

        for i in range(num_activities):
            activity_id = f"ACT-{activity_counter:06d}"
            activity_type = random.choices(
                ACTIVITY_TYPES,
                weights=[0.35, 0.35, 0.20, 0.10]
            )[0]

            # Activity date - spread over time
            if is_closed:
                days_ago = random.randint(10, days_range)
            elif health_score < 60 and i < num_activities // 2:
                # At-risk deals have gap in recent activity
                days_ago = random.randint(15, days_range)
            else:
                days_ago = random.randint(0, days_range)

            activity_date = SCENARIO_DATE - timedelta(days=days_ago)

            # Duration based on type
            if activity_type == "Call":
                duration = random.randint(10, 45)
            elif activity_type == "Email":
                duration = random.randint(5, 15)
            elif activity_type == "Meeting":
                duration = random.randint(30, 90)
            else:  # Demo
                duration = random.randint(45, 120)

            activities.append(Row(
                activity_id=activity_id,
                activity_type=activity_type,
                subject=random.choice(SUBJECTS[activity_type]),
                opportunity_id=opp_id,
                account_id=account_id,
                owner_id=owner_id,
                activity_date=activity_date,
                duration_minutes=duration,
                outcome=random.choice(OUTCOMES) if activity_type in ["Call", "Meeting"] else "Completed",
                is_completed=True,
            ))
            activity_counter += 1

    # ===========================================
    # CREATE DATAFRAME
    # ===========================================

    schema = StructType([
        StructField("activity_id", StringType(), False),
        StructField("activity_type", StringType(), True),
        StructField("subject", StringType(), True),
        StructField("opportunity_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("owner_id", StringType(), True),
        StructField("activity_date", DateType(), True),
        StructField("duration_minutes", IntegerType(), True),
        StructField("outcome", StringType(), True),
        StructField("is_completed", BooleanType(), True),
    ])

    df = spark.createDataFrame(activities, schema)
    output.write_dataframe(df)
