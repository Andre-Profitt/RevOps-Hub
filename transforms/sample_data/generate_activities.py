# /transforms/sample_data/generate_activities.py
"""
SAMPLE DATA GENERATOR: Activities
==================================
Generates sales activity data (calls, emails, meetings).
"""

from transforms.api import transform, Input, Output
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    IntegerType, BooleanType, TimestampType
)
import random
from datetime import datetime, timedelta


@transform(
    opportunities=Input("/RevOps/Sample/opportunities"),
    output=Output("/RevOps/Sample/activities")
)
def compute(ctx, opportunities, output):
    """Generate activities for each opportunity."""

    spark = ctx.spark_session
    random.seed(43)

    opps_df = opportunities.dataframe().collect()

    ACTIVITY_TYPES = [
        ("Call", ["Discovery call", "Follow-up call", "Demo call", "Check-in call", "Negotiation call"]),
        ("Email", ["Introduction email", "Follow-up email", "Proposal sent", "Contract sent", "Thank you email"]),
        ("Meeting", ["On-site meeting", "Video meeting", "Lunch meeting", "Executive briefing", "Technical deep dive"]),
        ("Demo", ["Product demo", "Technical demo", "Executive demo", "POC review"]),
        ("Task", ["Prepare proposal", "Internal review", "Update CRM", "Send contract", "Schedule meeting"]),
    ]

    OUTCOMES = ["Completed", "Completed", "Completed", "No Show", "Rescheduled", "Left Voicemail"]

    activities = []
    activity_id = 1
    today = datetime.now()

    for opp in opps_df:
        # Number of activities based on stage
        if opp.stage_name == "Closed Won":
            num_activities = random.randint(15, 30)
        elif opp.stage_name == "Closed Lost":
            num_activities = random.randint(5, 15)
        elif opp.probability >= 0.6:
            num_activities = random.randint(10, 20)
        elif opp.probability >= 0.3:
            num_activities = random.randint(5, 12)
        else:
            num_activities = random.randint(2, 8)

        # Generate activities spread over the deal lifetime
        deal_start = datetime.combine(opp.created_date, datetime.min.time())
        if opp.is_closed:
            deal_end = datetime.combine(opp.close_date, datetime.min.time())
        else:
            deal_end = today

        deal_duration = (deal_end - deal_start).days
        if deal_duration < 1:
            deal_duration = 1

        for _ in range(num_activities):
            activity_type, subjects = random.choice(ACTIVITY_TYPES)
            subject = random.choice(subjects)

            # Random date within deal duration
            days_offset = random.randint(0, deal_duration)
            activity_date = deal_start + timedelta(days=days_offset)

            # Duration based on type
            if activity_type == "Call":
                duration_minutes = random.choice([15, 30, 30, 45, 60])
            elif activity_type == "Meeting":
                duration_minutes = random.choice([30, 60, 60, 90, 120])
            elif activity_type == "Demo":
                duration_minutes = random.choice([45, 60, 60, 90])
            else:
                duration_minutes = random.choice([15, 30])

            outcome = random.choice(OUTCOMES) if activity_type in ("Call", "Meeting", "Demo") else "Completed"

            activities.append(Row(
                activity_id=f"ACT-{activity_id:06d}",
                activity_type=activity_type,
                subject=subject,
                opportunity_id=opp.opportunity_id,
                account_id=opp.account_id,
                account_name=opp.account_name,
                owner_id=opp.owner_id,
                owner_name=opp.owner_name,
                activity_date=activity_date.date(),
                activity_timestamp=activity_date,
                duration_minutes=duration_minutes,
                outcome=outcome,
                is_completed=outcome in ("Completed", "Left Voicemail"),
                notes=f"Activity regarding {opp.opportunity_name}",
                created_date=activity_date.date()
            ))

            activity_id += 1

    schema = StructType([
        StructField("activity_id", StringType(), False),
        StructField("activity_type", StringType(), True),
        StructField("subject", StringType(), True),
        StructField("opportunity_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("account_name", StringType(), True),
        StructField("owner_id", StringType(), True),
        StructField("owner_name", StringType(), True),
        StructField("activity_date", DateType(), True),
        StructField("activity_timestamp", TimestampType(), True),
        StructField("duration_minutes", IntegerType(), True),
        StructField("outcome", StringType(), True),
        StructField("is_completed", BooleanType(), True),
        StructField("notes", StringType(), True),
        StructField("created_date", DateType(), True),
    ])

    df = spark.createDataFrame(activities, schema)
    output.write_dataframe(df)
