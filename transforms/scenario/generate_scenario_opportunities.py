# /transforms/scenario/generate_scenario_opportunities.py
"""
SCENARIO DATA GENERATOR: Q4 Crunch Time
=========================================
This generator creates STORY-DRIVEN data, not random data.
Every opportunity serves the narrative of a Q4 sales push.

THE STORY:
- It's November 15th, Q4 is heating up
- Company needs $13M, has $8.2M closed, $4.8M to go
- Key deals at risk, top performers shining, struggling reps need help
- Competitive pressure from TechRival
- Process bottlenecks in Legal

KEY DEALS (Named, Important):
- Acme Corporation: $1.2M - AT RISK (champion went dark)
- GlobalTech: $850K - HEALTHY (multi-threaded, exec engaged)
- Pacific Financial: $950K - MONITOR (competitor involved)
- Meridian Healthcare: $720K - CLOSING (verbal commit)
"""

from transforms.api import transform, Output
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    DateType, IntegerType, BooleanType, ArrayType
)
import random
from datetime import datetime, timedelta


@transform(
    output=Output("/RevOps/Scenario/opportunities")
)
def compute(ctx, output):
    """Generate story-driven opportunity data."""

    spark = ctx.spark_session
    random.seed(42)  # Reproducible

    # ===========================================
    # SCENARIO DATE CONTEXT
    # ===========================================
    # We're simulating November 15th, Q4

    SCENARIO_DATE = datetime(2024, 11, 15).date()
    Q4_START = datetime(2024, 10, 1).date()
    Q4_END = datetime(2024, 12, 31).date()
    DAYS_LEFT_IN_Q = (Q4_END - SCENARIO_DATE).days  # 46 days

    opportunities = []
    opp_counter = 1

    # ===========================================
    # PART 1: HERO DEALS (Named, Critical to Story)
    # ===========================================

    HERO_DEALS = [
        # THE CRISIS DEAL - Acme Corporation
        {
            "account_name": "Acme Corporation",
            "account_id": "ACC-001",
            "amount": 1200000,
            "stage": "Negotiation",
            "forecast": "Commit",  # Was commit, now at risk
            "health_score": 48,  # CRASHED from 78
            "health_category": "At Risk",
            "owner": ("James Wilson", "REP-002", "MGR-001"),
            "days_in_stage": 18,  # Stuck
            "last_activity_days_ago": 8,  # GONE DARK
            "stakeholder_count": 2,  # Single-threaded
            "has_champion": True,  # Champion went dark
            "competitor": None,
            "close_date_offset": 25,  # Dec 10
            "risk_flags": ["Gone dark", "Stalled in stage", "Single-threaded"],
            "story": "Champion Jennifer Chen stopped responding 8 days ago. Was daily contact.",
            "segment": "Enterprise",
            "industry": "Technology",
        },

        # THE SURE THING - GlobalTech
        {
            "account_name": "GlobalTech Industries",
            "account_id": "ACC-002",
            "amount": 850000,
            "stage": "Negotiation",
            "forecast": "Commit",
            "health_score": 92,
            "health_category": "Healthy",
            "owner": ("David Kim", "REP-004", "MGR-002"),
            "days_in_stage": 7,
            "last_activity_days_ago": 1,
            "stakeholder_count": 5,  # Multi-threaded!
            "has_champion": True,
            "competitor": None,
            "close_date_offset": 20,
            "risk_flags": [],
            "story": "Exec sponsor engaged, legal review started, should close.",
            "segment": "Enterprise",
            "industry": "Technology",
        },

        # THE COMPETITIVE BATTLE - Pacific Financial
        {
            "account_name": "Pacific Financial Group",
            "account_id": "ACC-004",
            "amount": 950000,
            "stage": "Proposal",
            "forecast": "Best Case",
            "health_score": 65,
            "health_category": "Monitor",
            "owner": ("Emily Rodriguez", "REP-003", "MGR-001"),
            "days_in_stage": 12,
            "last_activity_days_ago": 3,
            "stakeholder_count": 3,
            "has_champion": True,
            "competitor": "TechRival",  # Competitor involved!
            "close_date_offset": 35,
            "risk_flags": ["Competitive pressure"],
            "story": "TechRival is in the deal. Need differentiation on security features.",
            "segment": "Enterprise",
            "industry": "Financial Services",
        },

        # THE CLOSING DEAL - Meridian Healthcare
        {
            "account_name": "Meridian Healthcare",
            "account_id": "ACC-003",
            "amount": 720000,
            "stage": "Verbal Commit",
            "forecast": "Commit",
            "health_score": 95,
            "health_category": "Healthy",
            "owner": ("Emily Rodriguez", "REP-003", "MGR-001"),
            "days_in_stage": 3,
            "last_activity_days_ago": 0,  # Today
            "stakeholder_count": 4,
            "has_champion": True,
            "competitor": None,
            "close_date_offset": 10,
            "risk_flags": [],
            "story": "Contract in legal, verbal yes from CFO. Closing this week.",
            "segment": "Enterprise",
            "industry": "Healthcare",
        },

        # THE STUCK DEAL - Vertex Software
        {
            "account_name": "Vertex Software",
            "account_id": "ACC-006",
            "amount": 180000,
            "stage": "Proposal",
            "forecast": "Best Case",
            "health_score": 35,
            "health_category": "Critical",
            "owner": ("Lisa Wang", "REP-005", "MGR-002"),
            "days_in_stage": 45,  # WAY too long
            "last_activity_days_ago": 14,
            "stakeholder_count": 1,  # Single-threaded
            "has_champion": False,  # No champion!
            "competitor": None,
            "close_date_offset": 40,
            "risk_flags": ["Stalled in stage", "Gone dark", "Single-threaded", "No champion identified"],
            "story": "Lisa's deal stuck for 45 days. Single contact not responding.",
            "segment": "Mid-Market",
            "industry": "Technology",
        },

        # JESSICA'S BIG WIN - Summit Education (showing her skill)
        {
            "account_name": "Summit Education",
            "account_id": "ACC-008",
            "amount": 220000,
            "stage": "Negotiation",
            "forecast": "Commit",
            "health_score": 88,
            "health_category": "Healthy",
            "owner": ("Jessica Taylor", "REP-009", "MGR-003"),
            "days_in_stage": 5,
            "last_activity_days_ago": 1,
            "stakeholder_count": 4,  # Jessica multi-threads
            "has_champion": True,
            "competitor": None,
            "close_date_offset": 15,
            "risk_flags": [],
            "story": "Jessica's deal - perfect execution, 4 stakeholders, champion strong.",
            "segment": "Mid-Market",
            "industry": "Education",
        },

        # KEVIN'S STRUGGLING DEAL
        {
            "account_name": "Skyline Ventures",
            "account_id": "ACC-014",
            "amount": 45000,
            "stage": "Solution Design",
            "forecast": "Pipeline",
            "health_score": 42,
            "health_category": "At Risk",
            "owner": ("Kevin Brown", "REP-010", "MGR-004"),
            "days_in_stage": 28,
            "last_activity_days_ago": 10,
            "stakeholder_count": 1,
            "has_champion": False,
            "competitor": None,
            "close_date_offset": 45,
            "risk_flags": ["Stalled in stage", "Single-threaded", "No champion identified"],
            "story": "Kevin's qualification was weak. Deal dying in Solution Design.",
            "segment": "SMB",
            "industry": "Technology",
        },
    ]

    # Create hero deals
    for deal in HERO_DEALS:
        opp_id = f"OPP-{opp_counter:05d}"
        owner_name, owner_id, manager_id = deal["owner"]

        opportunities.append(Row(
            opportunity_id=opp_id,
            opportunity_name=f"{deal['account_name']} - Q4 Deal",
            account_id=deal["account_id"],
            account_name=deal["account_name"],
            segment=deal["segment"],
            industry=deal["industry"],
            amount=float(deal["amount"]),
            stage_name=deal["stage"],
            probability=get_stage_probability(deal["stage"]),
            close_date=SCENARIO_DATE + timedelta(days=deal["close_date_offset"]),
            days_in_current_stage=deal["days_in_stage"],
            owner_id=owner_id,
            owner_name=owner_name,
            manager_id=manager_id,
            manager_name=get_manager_name(manager_id),
            region=get_region(manager_id),
            forecast_category=deal["forecast"],
            created_date=SCENARIO_DATE - timedelta(days=random.randint(30, 90)),
            last_activity_date=SCENARIO_DATE - timedelta(days=deal["last_activity_days_ago"]),
            competitor_mentioned=deal["competitor"],
            discount_percent=random.choice([0, 0, 5, 10]) if deal["competitor"] else 0,
            has_champion=deal["has_champion"],
            stakeholder_count=deal["stakeholder_count"],
            next_step=get_next_step(deal["stage"]),
            loss_reason=None,
            lead_source=random.choice(["Inbound", "Outbound", "Referral"]),
            deal_type="New Business" if deal["amount"] > 500000 else "Expansion",
            is_closed=False,
            is_hero_deal=True,
            story_notes=deal["story"],
            health_score_override=deal["health_score"],
        ))
        opp_counter += 1

    # ===========================================
    # PART 2: CLOSED WON THIS QUARTER ($8.2M)
    # ===========================================

    # We need $8.2M in Closed Won
    # Jessica: $1.8M (crushing it)
    # Emily: $1.5M (big deals)
    # David: $1.2M (rising star)
    # Sarah: $1.0M (solid)
    # Daniel: $0.9M (turnaround)
    # Robert: $0.8M (steady)
    # James: $0.5M (slumping)
    # Amanda: $0.3M (ramping)
    # Lisa: $0.2M (struggling)
    # Kevin: $0.0M (no closes yet)

    CLOSED_WON_TARGETS = [
        ("Jessica Taylor", "REP-009", "MGR-003", 1800000, 8),
        ("Emily Rodriguez", "REP-003", "MGR-001", 1500000, 4),
        ("David Kim", "REP-004", "MGR-002", 1200000, 6),
        ("Sarah Chen", "REP-001", "MGR-001", 1000000, 5),
        ("Daniel Martinez", "REP-008", "MGR-003", 900000, 5),
        ("Robert Johnson", "REP-006", "MGR-002", 800000, 4),
        ("James Wilson", "REP-002", "MGR-001", 500000, 3),
        ("Amanda Foster", "REP-007", "MGR-003", 300000, 3),
        ("Lisa Wang", "REP-005", "MGR-002", 200000, 2),
    ]

    for rep_name, rep_id, mgr_id, target_amount, num_deals in CLOSED_WON_TARGETS:
        remaining = target_amount
        for i in range(num_deals):
            if i == num_deals - 1:
                amount = remaining
            else:
                amount = random.randint(int(remaining * 0.1), int(remaining * 0.4))
                remaining -= amount

            if amount <= 0:
                continue

            opp_id = f"OPP-{opp_counter:05d}"
            account = get_random_account()
            close_date = Q4_START + timedelta(days=random.randint(0, (SCENARIO_DATE - Q4_START).days - 1))

            # Jessica's wins have more stakeholders (her secret)
            if rep_name == "Jessica Taylor":
                stakeholders = random.randint(4, 6)
            else:
                stakeholders = random.randint(2, 4)

            opportunities.append(Row(
                opportunity_id=opp_id,
                opportunity_name=f"{account[0]} - Closed Deal",
                account_id=account[1],
                account_name=account[0],
                segment=account[2],
                industry=account[3],
                amount=float(amount),
                stage_name="Closed Won",
                probability=1.0,
                close_date=close_date,
                days_in_current_stage=0,
                owner_id=rep_id,
                owner_name=rep_name,
                manager_id=mgr_id,
                manager_name=get_manager_name(mgr_id),
                region=get_region(mgr_id),
                forecast_category="Closed",
                created_date=close_date - timedelta(days=random.randint(30, 90)),
                last_activity_date=close_date,
                competitor_mentioned=None,
                discount_percent=random.choice([0, 0, 0, 5, 10]),
                has_champion=True,
                stakeholder_count=stakeholders,
                next_step=None,
                loss_reason=None,
                lead_source=random.choice(["Inbound", "Outbound", "Referral", "Event"]),
                deal_type=random.choice(["New Business", "Expansion"]),
                is_closed=True,
                is_hero_deal=False,
                story_notes=None,
                health_score_override=None,
            ))
            opp_counter += 1

    # ===========================================
    # PART 3: CLOSED LOST (For Win Rate Analysis)
    # ===========================================

    # Need to show 24% win rate this quarter
    # If we have ~40 closed won, we need ~127 closed lost
    # But let's focus on interesting losses

    COMPETITIVE_LOSSES = [
        # TechRival wins (the competitor story)
        ("DataStream Corp", "ACC-021", 340000, "TechRival", "Price too high", "REP-002"),
        ("CloudNine Systems", "ACC-022", 280000, "TechRival", "Price too high", "REP-006"),
        ("Agile Works", "ACC-023", 195000, "TechRival", "Price too high", "REP-005"),
        ("FastTrack Inc", "ACC-024", 420000, "TechRival", "Missing features", "REP-003"),
        # Other losses
        ("NoDecision LLC", "ACC-025", 150000, None, "No decision made", "REP-007"),
        ("BudgetCut Co", "ACC-026", 280000, None, "No budget", "REP-005"),
        ("Delayed Corp", "ACC-027", 180000, None, "Timing not right", "REP-010"),
    ]

    for account_name, account_id, amount, competitor, reason, rep_id in COMPETITIVE_LOSSES:
        opp_id = f"OPP-{opp_counter:05d}"
        rep_name = get_rep_name(rep_id)
        mgr_id = get_manager_id(rep_id)
        close_date = Q4_START + timedelta(days=random.randint(5, 40))

        opportunities.append(Row(
            opportunity_id=opp_id,
            opportunity_name=f"{account_name} - Lost Deal",
            account_id=account_id,
            account_name=account_name,
            segment="Mid-Market",
            industry=random.choice(["Technology", "Financial Services", "Healthcare"]),
            amount=float(amount),
            stage_name="Closed Lost",
            probability=0.0,
            close_date=close_date,
            days_in_current_stage=0,
            owner_id=rep_id,
            owner_name=rep_name,
            manager_id=mgr_id,
            manager_name=get_manager_name(mgr_id),
            region=get_region(mgr_id),
            forecast_category="Omitted",
            created_date=close_date - timedelta(days=random.randint(30, 75)),
            last_activity_date=close_date - timedelta(days=random.randint(3, 10)),
            competitor_mentioned=competitor,
            discount_percent=random.choice([10, 15, 20]) if competitor else 0,
            has_champion=random.choice([True, False]),
            stakeholder_count=random.randint(1, 2),  # Losses tend to be single-threaded
            next_step=None,
            loss_reason=reason,
            lead_source=random.choice(["Inbound", "Outbound"]),
            deal_type="New Business",
            is_closed=True,
            is_hero_deal=False,
            story_notes=f"Lost to {competitor}" if competitor else reason,
            health_score_override=None,
        ))
        opp_counter += 1

    # Add more generic closed lost to hit 24% win rate
    for i in range(80):
        opp_id = f"OPP-{opp_counter:05d}"
        account = get_random_account()
        rep = get_random_rep()
        close_date = Q4_START + timedelta(days=random.randint(0, 45))

        opportunities.append(Row(
            opportunity_id=opp_id,
            opportunity_name=f"{account[0]} - Lost",
            account_id=account[1],
            account_name=account[0],
            segment=account[2],
            industry=account[3],
            amount=float(random.randint(20000, 200000)),
            stage_name="Closed Lost",
            probability=0.0,
            close_date=close_date,
            days_in_current_stage=0,
            owner_id=rep[1],
            owner_name=rep[0],
            manager_id=rep[2],
            manager_name=get_manager_name(rep[2]),
            region=get_region(rep[2]),
            forecast_category="Omitted",
            created_date=close_date - timedelta(days=random.randint(20, 60)),
            last_activity_date=close_date - timedelta(days=random.randint(5, 15)),
            competitor_mentioned=random.choice([None, None, None, "TechRival", "Competitor B"]),
            discount_percent=0,
            has_champion=random.choice([True, False, False]),
            stakeholder_count=random.randint(1, 2),
            next_step=None,
            loss_reason=random.choice(["No budget", "No decision made", "Went with competitor", "Timing not right", "Project cancelled"]),
            lead_source=random.choice(["Inbound", "Outbound"]),
            deal_type="New Business",
            is_closed=True,
            is_hero_deal=False,
            story_notes=None,
            health_score_override=None,
        ))
        opp_counter += 1

    # ===========================================
    # PART 4: OPEN PIPELINE (Rest of the deals)
    # ===========================================

    # Generate open pipeline for each rep
    OPEN_PIPELINE_BY_REP = [
        ("Jessica Taylor", "REP-009", "MGR-003", 6, 0.85),   # High quality deals
        ("David Kim", "REP-004", "MGR-002", 8, 0.75),
        ("Sarah Chen", "REP-001", "MGR-001", 7, 0.70),
        ("Emily Rodriguez", "REP-003", "MGR-001", 5, 0.70),
        ("Daniel Martinez", "REP-008", "MGR-003", 7, 0.65),
        ("Robert Johnson", "REP-006", "MGR-002", 6, 0.60),
        ("James Wilson", "REP-002", "MGR-001", 5, 0.50),   # Slumping
        ("Amanda Foster", "REP-007", "MGR-003", 8, 0.55),   # Ramping, many deals, lower quality
        ("Lisa Wang", "REP-005", "MGR-002", 4, 0.40),       # Struggling
        ("Kevin Brown", "REP-010", "MGR-004", 5, 0.35),     # Struggling
    ]

    for rep_name, rep_id, mgr_id, num_deals, health_factor in OPEN_PIPELINE_BY_REP:
        for i in range(num_deals):
            opp_id = f"OPP-{opp_counter:05d}"
            account = get_random_account()

            # Stage distribution
            stage = random.choices(
                ["Discovery", "Solution Design", "Proposal", "Negotiation"],
                weights=[0.3, 0.3, 0.25, 0.15]
            )[0]

            # Amount based on segment
            if account[2] == "Enterprise":
                amount = random.randint(300000, 800000)
            elif account[2] == "Mid-Market":
                amount = random.randint(80000, 250000)
            else:
                amount = random.randint(15000, 70000)

            # Health influenced by rep performance
            base_health = random.randint(40, 95)
            health = int(base_health * health_factor)
            health = max(20, min(100, health))

            # Jessica multi-threads
            if rep_name == "Jessica Taylor":
                stakeholders = random.randint(3, 5)
            elif rep_name in ["Kevin Brown", "Lisa Wang"]:
                stakeholders = random.randint(1, 2)
            else:
                stakeholders = random.randint(2, 4)

            # Days in stage
            if health < 50:
                days_in_stage = random.randint(20, 50)  # Stuck
            else:
                days_in_stage = random.randint(3, 18)   # Normal

            # Last activity
            if health < 50:
                last_activity_days = random.randint(10, 25)  # Gone dark
            else:
                last_activity_days = random.randint(0, 5)    # Active

            # Forecast category
            if stage == "Negotiation" and health >= 70:
                forecast = "Commit"
            elif stage in ["Proposal", "Negotiation"] and health >= 50:
                forecast = "Best Case"
            else:
                forecast = "Pipeline"

            # Health category
            if health >= 80:
                health_cat = "Healthy"
            elif health >= 60:
                health_cat = "Monitor"
            elif health >= 40:
                health_cat = "At Risk"
            else:
                health_cat = "Critical"

            opportunities.append(Row(
                opportunity_id=opp_id,
                opportunity_name=f"{account[0]} - {stage}",
                account_id=account[1],
                account_name=account[0],
                segment=account[2],
                industry=account[3],
                amount=float(amount),
                stage_name=stage,
                probability=get_stage_probability(stage),
                close_date=SCENARIO_DATE + timedelta(days=random.randint(15, 60)),
                days_in_current_stage=days_in_stage,
                owner_id=rep_id,
                owner_name=rep_name,
                manager_id=mgr_id,
                manager_name=get_manager_name(mgr_id),
                region=get_region(mgr_id),
                forecast_category=forecast,
                created_date=SCENARIO_DATE - timedelta(days=random.randint(20, 90)),
                last_activity_date=SCENARIO_DATE - timedelta(days=last_activity_days),
                competitor_mentioned=random.choice([None, None, None, None, "TechRival", "Competitor B"]),
                discount_percent=0,
                has_champion=health >= 60,
                stakeholder_count=stakeholders,
                next_step=get_next_step(stage),
                loss_reason=None,
                lead_source=random.choice(["Inbound", "Outbound", "Referral"]),
                deal_type=random.choice(["New Business", "Expansion"]),
                is_closed=False,
                is_hero_deal=False,
                story_notes=None,
                health_score_override=health,
            ))
            opp_counter += 1

    # ===========================================
    # CREATE DATAFRAME
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
        StructField("is_hero_deal", BooleanType(), True),
        StructField("story_notes", StringType(), True),
        StructField("health_score_override", IntegerType(), True),
    ])

    df = spark.createDataFrame(opportunities, schema)
    output.write_dataframe(df)


# ===========================================
# HELPER FUNCTIONS
# ===========================================

def get_stage_probability(stage):
    probs = {
        "Prospecting": 0.10,
        "Discovery": 0.20,
        "Solution Design": 0.40,
        "Proposal": 0.60,
        "Negotiation": 0.80,
        "Verbal Commit": 0.90,
        "Closed Won": 1.00,
        "Closed Lost": 0.00,
    }
    return probs.get(stage, 0.5)

def get_manager_name(mgr_id):
    managers = {
        "MGR-001": "Michael Torres",
        "MGR-002": "Jennifer Adams",
        "MGR-003": "Christopher Lee",
        "MGR-004": "Rachel Green",
    }
    return managers.get(mgr_id, "Unknown")

def get_region(mgr_id):
    regions = {
        "MGR-001": "West",
        "MGR-002": "East",
        "MGR-003": "Central",
        "MGR-004": "South",
    }
    return regions.get(mgr_id, "Unknown")

def get_rep_name(rep_id):
    reps = {
        "REP-001": "Sarah Chen",
        "REP-002": "James Wilson",
        "REP-003": "Emily Rodriguez",
        "REP-004": "David Kim",
        "REP-005": "Lisa Wang",
        "REP-006": "Robert Johnson",
        "REP-007": "Amanda Foster",
        "REP-008": "Daniel Martinez",
        "REP-009": "Jessica Taylor",
        "REP-010": "Kevin Brown",
    }
    return reps.get(rep_id, "Unknown")

def get_manager_id(rep_id):
    mgrs = {
        "REP-001": "MGR-001", "REP-002": "MGR-001", "REP-003": "MGR-001",
        "REP-004": "MGR-002", "REP-005": "MGR-002", "REP-006": "MGR-002",
        "REP-007": "MGR-003", "REP-008": "MGR-003", "REP-009": "MGR-003",
        "REP-010": "MGR-004",
    }
    return mgrs.get(rep_id, "MGR-001")

def get_next_step(stage):
    steps = {
        "Discovery": "Schedule solution workshop",
        "Solution Design": "Present proposal",
        "Proposal": "Negotiate terms",
        "Negotiation": "Get verbal commit",
        "Verbal Commit": "Send contract",
    }
    return steps.get(stage)

def get_random_account():
    accounts = [
        ("Atlas Manufacturing", "ACC-005", "Enterprise", "Manufacturing"),
        ("Titan Energy", "ACC-010", "Enterprise", "Energy"),
        ("Continental Logistics", "ACC-011", "Enterprise", "Logistics"),
        ("Coastal Retail Co", "ACC-007", "Mid-Market", "Retail"),
        ("Harbor Logistics", "ACC-009", "Mid-Market", "Logistics"),
        ("Pinnacle Media", "ACC-012", "Mid-Market", "Media"),
        ("Frontier Analytics", "ACC-013", "Mid-Market", "Technology"),
        ("Cascade Health", "ACC-015", "Mid-Market", "Healthcare"),
        ("Redwood Partners", "ACC-016", "Mid-Market", "Financial Services"),
        ("Maple Street Retail", "ACC-017", "SMB", "Retail"),
        ("Cornerstone Consulting", "ACC-018", "SMB", "Professional Services"),
        ("Brightside Marketing", "ACC-019", "SMB", "Marketing"),
        ("Evergreen Solutions", "ACC-020", "SMB", "Technology"),
    ]
    return random.choice(accounts)

def get_random_rep():
    reps = [
        ("Sarah Chen", "REP-001", "MGR-001"),
        ("James Wilson", "REP-002", "MGR-001"),
        ("Emily Rodriguez", "REP-003", "MGR-001"),
        ("David Kim", "REP-004", "MGR-002"),
        ("Lisa Wang", "REP-005", "MGR-002"),
        ("Robert Johnson", "REP-006", "MGR-002"),
        ("Amanda Foster", "REP-007", "MGR-003"),
        ("Daniel Martinez", "REP-008", "MGR-003"),
        ("Jessica Taylor", "REP-009", "MGR-003"),
        ("Kevin Brown", "REP-010", "MGR-004"),
    ]
    return random.choice(reps)
