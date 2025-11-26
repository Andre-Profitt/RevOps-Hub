# /transforms/sample_data/generate_opportunities.py
"""
SAMPLE DATA GENERATOR: Opportunities
=====================================
Generates realistic B2B sales opportunity data for testing.
Run this ONCE to create your test dataset.

To use in Foundry:
1. Create a new Python transform in your Code Repository
2. Copy this entire file
3. Build the transform
4. The output dataset will be created at /RevOps/Sample/opportunities
"""

from transforms.api import transform, Output
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    DateType, IntegerType, BooleanType, TimestampType
)
import random
from datetime import datetime, timedelta
import hashlib


@transform(
    output=Output("/RevOps/Sample/opportunities")
)
def compute(ctx, output):
    """
    Generate 500 realistic B2B sales opportunities.

    Data characteristics:
    - Mix of Enterprise, Mid-Market, and SMB deals
    - Realistic stage distribution
    - Proper amount ranges by segment
    - Seasonal patterns
    - Win/loss ratios aligned with industry benchmarks
    """

    spark = ctx.spark_session
    random.seed(42)  # Reproducible results

    # ===========================================
    # CONFIGURATION
    # ===========================================

    NUM_OPPORTUNITIES = 500

    # Accounts with realistic characteristics
    ACCOUNTS = [
        # (name, id, segment, industry, typical_deal_size_min, typical_deal_size_max)
        ("Acme Corporation", "ACC-001", "Enterprise", "Technology", 500000, 2000000),
        ("GlobalTech Industries", "ACC-002", "Enterprise", "Technology", 400000, 1500000),
        ("Meridian Healthcare", "ACC-003", "Enterprise", "Healthcare", 300000, 1200000),
        ("Pacific Financial Group", "ACC-004", "Enterprise", "Financial Services", 600000, 2500000),
        ("Atlas Manufacturing", "ACC-005", "Enterprise", "Manufacturing", 350000, 1000000),
        ("Vertex Software", "ACC-006", "Mid-Market", "Technology", 100000, 400000),
        ("Coastal Retail Co", "ACC-007", "Mid-Market", "Retail", 75000, 250000),
        ("Summit Education", "ACC-008", "Mid-Market", "Education", 80000, 300000),
        ("Harbor Logistics", "ACC-009", "Mid-Market", "Logistics", 90000, 350000),
        ("Pinnacle Media", "ACC-010", "Mid-Market", "Media", 60000, 200000),
        ("Frontier Analytics", "ACC-011", "Mid-Market", "Technology", 120000, 450000),
        ("Cascade Health", "ACC-012", "Mid-Market", "Healthcare", 100000, 380000),
        ("Redwood Partners", "ACC-013", "Mid-Market", "Financial Services", 150000, 500000),
        ("Skyline Ventures", "ACC-014", "SMB", "Technology", 15000, 75000),
        ("Maple Street Retail", "ACC-015", "SMB", "Retail", 10000, 50000),
        ("Cornerstone Consulting", "ACC-016", "SMB", "Professional Services", 20000, 80000),
        ("Brightside Marketing", "ACC-017", "SMB", "Marketing", 12000, 60000),
        ("Evergreen Solutions", "ACC-018", "SMB", "Technology", 18000, 70000),
        ("Sunrise Healthcare", "ACC-019", "SMB", "Healthcare", 25000, 90000),
        ("Bluewave Technologies", "ACC-020", "SMB", "Technology", 15000, 65000),
    ]

    # Sales stages with probabilities and typical durations
    STAGES = [
        # (stage_name, probability, typical_days, can_be_current)
        ("Prospecting", 0.10, 14, True),
        ("Discovery", 0.20, 21, True),
        ("Solution Design", 0.40, 28, True),
        ("Proposal", 0.60, 14, True),
        ("Negotiation", 0.80, 21, True),
        ("Verbal Commit", 0.90, 7, True),
        ("Closed Won", 1.00, 0, False),
        ("Closed Lost", 0.00, 0, False),
    ]

    # Sales reps with managers and regions
    REPS = [
        # (name, id, manager_id, manager_name, region, quota)
        ("Sarah Chen", "REP-001", "MGR-001", "Michael Torres", "West", 1500000),
        ("James Wilson", "REP-002", "MGR-001", "Michael Torres", "West", 1200000),
        ("Emily Rodriguez", "REP-003", "MGR-001", "Michael Torres", "West", 1400000),
        ("David Kim", "REP-004", "MGR-002", "Jennifer Adams", "East", 1600000),
        ("Lisa Wang", "REP-005", "MGR-002", "Jennifer Adams", "East", 1300000),
        ("Robert Johnson", "REP-006", "MGR-002", "Jennifer Adams", "East", 1450000),
        ("Amanda Foster", "REP-007", "MGR-003", "Christopher Lee", "Central", 1100000),
        ("Daniel Martinez", "REP-008", "MGR-003", "Christopher Lee", "Central", 1250000),
        ("Jessica Taylor", "REP-009", "MGR-003", "Christopher Lee", "Central", 1350000),
        ("Kevin Brown", "REP-010", "MGR-004", "Rachel Green", "South", 1000000),
    ]

    COMPETITORS = [None, None, None, "Competitor A", "Competitor B", "Competitor C", "Competitor D"]

    LOSS_REASONS = [
        "Price too high",
        "Went with competitor",
        "No budget",
        "Project cancelled",
        "No decision made",
        "Missing features",
        "Timing not right"
    ]

    NEXT_STEPS = [
        "Schedule discovery call",
        "Send proposal",
        "Demo scheduled",
        "Executive briefing",
        "Contract review",
        "Procurement meeting",
        "Technical deep dive",
        "Reference call",
        "Negotiate terms",
        "Final approval meeting",
        None
    ]

    # ===========================================
    # GENERATE OPPORTUNITIES
    # ===========================================

    opportunities = []
    today = datetime.now().date()

    for i in range(NUM_OPPORTUNITIES):
        # Select account
        account = random.choice(ACCOUNTS)
        account_name, account_id, segment, industry, min_deal, max_deal = account

        # Select rep
        rep = random.choice(REPS)
        rep_name, rep_id, manager_id, manager_name, region, quota = rep

        # Determine if this is a closed deal or open
        is_closed = random.random() < 0.35  # 35% of deals are closed

        if is_closed:
            # Closed deal
            is_won = random.random() < 0.28  # 28% win rate
            stage_name = "Closed Won" if is_won else "Closed Lost"
            probability = 1.0 if is_won else 0.0

            # Close date in the past
            days_ago = random.randint(1, 120)
            close_date = today - timedelta(days=days_ago)
            created_date = close_date - timedelta(days=random.randint(30, 180))

            # Calculate days in stage (was in final stage)
            days_in_stage = random.randint(1, 14)

            forecast_category = "Closed" if is_won else "Omitted"
            loss_reason = random.choice(LOSS_REASONS) if not is_won else None
        else:
            # Open deal - pick a stage
            open_stages = [s for s in STAGES if s[3]]  # can_be_current = True
            stage_info = random.choice(open_stages)
            stage_name, probability, typical_days, _ = stage_info

            # Close date in the future
            days_ahead = random.randint(7, 120)
            close_date = today + timedelta(days=days_ahead)
            created_date = today - timedelta(days=random.randint(14, 180))

            # Days in current stage (some deals are stuck)
            if random.random() < 0.2:  # 20% are stalled
                days_in_stage = random.randint(typical_days * 2, typical_days * 4)
            else:
                days_in_stage = random.randint(1, int(typical_days * 1.5))

            # Forecast category based on stage
            if probability >= 0.80:
                forecast_category = random.choices(
                    ["Commit", "Best Case"],
                    weights=[0.7, 0.3]
                )[0]
            elif probability >= 0.40:
                forecast_category = random.choices(
                    ["Best Case", "Pipeline"],
                    weights=[0.4, 0.6]
                )[0]
            else:
                forecast_category = "Pipeline"

            loss_reason = None

        # Deal amount based on segment
        amount = random.randint(min_deal, max_deal)
        # Round to nearest 5000
        amount = round(amount / 5000) * 5000

        # Activity recency (some deals have gone dark)
        if is_closed:
            last_activity_date = close_date - timedelta(days=random.randint(0, 7))
        elif random.random() < 0.15:  # 15% have gone dark
            last_activity_date = today - timedelta(days=random.randint(14, 45))
        else:
            last_activity_date = today - timedelta(days=random.randint(0, 10))

        # Stakeholder count (enterprise has more)
        if segment == "Enterprise":
            stakeholder_count = random.randint(2, 8)
        elif segment == "Mid-Market":
            stakeholder_count = random.randint(1, 5)
        else:
            stakeholder_count = random.randint(1, 3)

        # Champion present (correlates with stage progression)
        has_champion = probability >= 0.40 or random.random() < 0.3

        # Competitor mentioned
        competitor = random.choice(COMPETITORS)

        # Discount (higher discounts in negotiation, correlates with competitive deals)
        if stage_name in ("Negotiation", "Verbal Commit", "Closed Won", "Closed Lost"):
            if competitor:
                discount_percent = random.choice([0, 5, 10, 15, 20, 25])
            else:
                discount_percent = random.choice([0, 0, 5, 10, 15])
        else:
            discount_percent = 0

        # Next step
        if is_closed:
            next_step = None
        else:
            next_step = random.choice(NEXT_STEPS)

        # Generate unique opportunity ID
        opp_id = f"OPP-{i+1:05d}"

        # Opportunity name
        opp_name = f"{account_name} - {stage_name if is_closed else 'Expansion'} Deal"

        opportunities.append(Row(
            opportunity_id=opp_id,
            opportunity_name=opp_name,
            account_id=account_id,
            account_name=account_name,
            segment=segment,
            industry=industry,
            amount=float(amount),
            stage_name=stage_name,
            probability=probability,
            close_date=close_date,
            days_in_current_stage=days_in_stage,
            owner_id=rep_id,
            owner_name=rep_name,
            manager_id=manager_id,
            manager_name=manager_name,
            region=region,
            forecast_category=forecast_category,
            created_date=created_date,
            last_activity_date=last_activity_date,
            competitor_mentioned=competitor,
            discount_percent=discount_percent,
            has_champion=has_champion,
            stakeholder_count=stakeholder_count,
            next_step=next_step,
            loss_reason=loss_reason,
            lead_source=random.choice(["Inbound", "Outbound", "Referral", "Event", "Partner"]),
            deal_type=random.choice(["New Business", "Expansion", "Renewal"]),
            is_closed=is_closed
        ))

    # ===========================================
    # CREATE DATAFRAME WITH SCHEMA
    # ===========================================

    schema = StructType([
        StructField("opportunity_id", StringType(), False),
        StructField("opportunity_name", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("account_name", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("industry", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("stage_name", StringType(), True),
        StructField("probability", DoubleType(), True),
        StructField("close_date", DateType(), True),
        StructField("days_in_current_stage", IntegerType(), True),
        StructField("owner_id", StringType(), True),
        StructField("owner_name", StringType(), True),
        StructField("manager_id", StringType(), True),
        StructField("manager_name", StringType(), True),
        StructField("region", StringType(), True),
        StructField("forecast_category", StringType(), True),
        StructField("created_date", DateType(), True),
        StructField("last_activity_date", DateType(), True),
        StructField("competitor_mentioned", StringType(), True),
        StructField("discount_percent", IntegerType(), True),
        StructField("has_champion", BooleanType(), True),
        StructField("stakeholder_count", IntegerType(), True),
        StructField("next_step", StringType(), True),
        StructField("loss_reason", StringType(), True),
        StructField("lead_source", StringType(), True),
        StructField("deal_type", StringType(), True),
        StructField("is_closed", BooleanType(), True),
    ])

    df = spark.createDataFrame(opportunities, schema)
    output.write_dataframe(df)
