# /transforms/sample_data/generate_accounts.py
"""
SAMPLE DATA GENERATOR: Accounts
================================
Generates B2B account/company data for testing.
"""

from transforms.api import transform, Output
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    DateType, IntegerType, BooleanType
)
import random
from datetime import datetime, timedelta


@transform(
    output=Output("/RevOps/Sample/accounts")
)
def compute(ctx, output):
    """Generate 50 realistic B2B accounts."""

    spark = ctx.spark_session
    random.seed(42)

    INDUSTRIES = [
        "Technology", "Healthcare", "Financial Services", "Manufacturing",
        "Retail", "Education", "Media", "Logistics", "Professional Services",
        "Marketing", "Energy", "Government"
    ]

    SEGMENTS = ["Enterprise", "Mid-Market", "SMB"]

    ACCOUNT_DATA = [
        # Enterprise accounts
        ("Acme Corporation", "Enterprise", "Technology", 5000, 2500000000),
        ("GlobalTech Industries", "Enterprise", "Technology", 8000, 4200000000),
        ("Meridian Healthcare", "Enterprise", "Healthcare", 12000, 3800000000),
        ("Pacific Financial Group", "Enterprise", "Financial Services", 3500, 8500000000),
        ("Atlas Manufacturing", "Enterprise", "Manufacturing", 15000, 2100000000),
        ("Titan Energy", "Enterprise", "Energy", 6000, 5600000000),
        ("Federal Solutions Inc", "Enterprise", "Government", 4000, 1200000000),
        ("Continental Logistics", "Enterprise", "Logistics", 9000, 3100000000),
        ("Premier Health Systems", "Enterprise", "Healthcare", 20000, 6700000000),
        ("United Financial", "Enterprise", "Financial Services", 7500, 12000000000),

        # Mid-Market accounts
        ("Vertex Software", "Mid-Market", "Technology", 500, 85000000),
        ("Coastal Retail Co", "Mid-Market", "Retail", 800, 120000000),
        ("Summit Education", "Mid-Market", "Education", 350, 45000000),
        ("Harbor Logistics", "Mid-Market", "Logistics", 600, 95000000),
        ("Pinnacle Media", "Mid-Market", "Media", 250, 38000000),
        ("Frontier Analytics", "Mid-Market", "Technology", 180, 28000000),
        ("Cascade Health", "Mid-Market", "Healthcare", 400, 62000000),
        ("Redwood Partners", "Mid-Market", "Financial Services", 150, 180000000),
        ("Mountain Manufacturing", "Mid-Market", "Manufacturing", 700, 110000000),
        ("Horizon Marketing", "Mid-Market", "Marketing", 200, 32000000),
        ("Lakeview Education", "Mid-Market", "Education", 280, 41000000),
        ("Riverdale Tech", "Mid-Market", "Technology", 320, 52000000),
        ("Greenfield Energy", "Mid-Market", "Energy", 450, 78000000),
        ("Metro Retail Group", "Mid-Market", "Retail", 550, 89000000),
        ("Clearwater Consulting", "Mid-Market", "Professional Services", 190, 35000000),

        # SMB accounts
        ("Skyline Ventures", "SMB", "Technology", 45, 8000000),
        ("Maple Street Retail", "SMB", "Retail", 30, 4500000),
        ("Cornerstone Consulting", "SMB", "Professional Services", 25, 3800000),
        ("Brightside Marketing", "SMB", "Marketing", 18, 2800000),
        ("Evergreen Solutions", "SMB", "Technology", 35, 5200000),
        ("Sunrise Healthcare", "SMB", "Healthcare", 50, 7500000),
        ("Bluewave Technologies", "SMB", "Technology", 28, 4200000),
        ("Oak Street Financial", "SMB", "Financial Services", 15, 12000000),
        ("Cedar Manufacturing", "SMB", "Manufacturing", 60, 9800000),
        ("Willow Education", "SMB", "Education", 40, 3200000),
        ("Aspen Media", "SMB", "Media", 22, 2100000),
        ("Birch Logistics", "SMB", "Logistics", 38, 5800000),
        ("Pine Energy", "SMB", "Energy", 55, 8200000),
        ("Spruce Retail", "SMB", "Retail", 25, 3500000),
        ("Cypress Tech", "SMB", "Technology", 32, 4800000),
    ]

    accounts = []
    today = datetime.now().date()

    for i, (name, segment, industry, employees, revenue) in enumerate(ACCOUNT_DATA):
        account_id = f"ACC-{i+1:03d}"

        # Customer since (older for enterprise)
        if segment == "Enterprise":
            customer_since = today - timedelta(days=random.randint(365, 1825))
            arr = random.randint(100000, 500000)
        elif segment == "Mid-Market":
            customer_since = today - timedelta(days=random.randint(180, 1095))
            arr = random.randint(30000, 150000)
        else:
            customer_since = today - timedelta(days=random.randint(90, 730))
            arr = random.randint(5000, 40000)

        # Some accounts are prospects (no ARR yet)
        is_customer = random.random() < 0.6
        if not is_customer:
            arr = 0
            customer_since = None

        # Health score for customers
        health_score = random.randint(40, 100) if is_customer else None

        # Assign account owner
        owner_id = f"REP-{random.randint(1, 10):03d}"

        accounts.append(Row(
            account_id=account_id,
            account_name=name,
            segment=segment,
            industry=industry,
            employee_count=employees,
            annual_revenue=float(revenue),
            website=f"www.{name.lower().replace(' ', '')}.com",
            billing_city=random.choice(["San Francisco", "New York", "Chicago", "Austin", "Seattle", "Boston", "Denver", "Atlanta"]),
            billing_state=random.choice(["CA", "NY", "IL", "TX", "WA", "MA", "CO", "GA"]),
            billing_country="USA",
            is_customer=is_customer,
            customer_since=customer_since,
            current_arr=float(arr),
            health_score=health_score,
            owner_id=owner_id,
            created_date=today - timedelta(days=random.randint(180, 1095)),
            last_activity_date=today - timedelta(days=random.randint(0, 30)),
            open_opportunities=random.randint(0, 3),
            lifetime_value=float(arr * random.uniform(2, 5)) if is_customer else 0.0
        ))

    schema = StructType([
        StructField("account_id", StringType(), False),
        StructField("account_name", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("industry", StringType(), True),
        StructField("employee_count", IntegerType(), True),
        StructField("annual_revenue", DoubleType(), True),
        StructField("website", StringType(), True),
        StructField("billing_city", StringType(), True),
        StructField("billing_state", StringType(), True),
        StructField("billing_country", StringType(), True),
        StructField("is_customer", BooleanType(), True),
        StructField("customer_since", DateType(), True),
        StructField("current_arr", DoubleType(), True),
        StructField("health_score", IntegerType(), True),
        StructField("owner_id", StringType(), True),
        StructField("created_date", DateType(), True),
        StructField("last_activity_date", DateType(), True),
        StructField("open_opportunities", IntegerType(), True),
        StructField("lifetime_value", DoubleType(), True),
    ])

    df = spark.createDataFrame(accounts, schema)
    output.write_dataframe(df)
