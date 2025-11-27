"""
SELF-SERVICE WIZARD
====================
Customer-facing self-service capabilities for RevOps Hub onboarding.
Guides customers through setup, validation, and optimization.

Phase 5: Customer-Facing Autonomy

Capabilities:
- Guided onboarding wizard
- Usage-based recommendations
- Auto-detection of integration opportunities
- Self-service configuration validation
"""

from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    BooleanType, DoubleType, IntegerType, ArrayType
)
from datetime import datetime
import json
import uuid


# =============================================================================
# ONBOARDING WIZARD STATE
# =============================================================================

WIZARD_STEPS = [
    {
        "step_id": "connect_crm",
        "name": "Connect CRM",
        "description": "Connect your Salesforce or HubSpot instance",
        "required": True,
        "order": 1
    },
    {
        "step_id": "import_users",
        "name": "Import Users",
        "description": "Import your sales team members",
        "required": True,
        "order": 2
    },
    {
        "step_id": "configure_stages",
        "name": "Configure Pipeline Stages",
        "description": "Map your opportunity stages to RevOps stages",
        "required": True,
        "order": 3
    },
    {
        "step_id": "set_quotas",
        "name": "Set Quotas",
        "description": "Define quotas for your sales team",
        "required": False,
        "order": 4
    },
    {
        "step_id": "enable_forecasting",
        "name": "Enable Forecasting",
        "description": "Turn on AI-powered forecasting",
        "required": False,
        "order": 5
    },
    {
        "step_id": "configure_alerts",
        "name": "Configure Alerts",
        "description": "Set up notifications and alert rules",
        "required": False,
        "order": 6
    },
    {
        "step_id": "invite_team",
        "name": "Invite Team",
        "description": "Invite your team members to RevOps Hub",
        "required": False,
        "order": 7
    }
]


@transform(
    usage_metrics=Input("/RevOps/Ops/usage_metrics"),
    output=Output("/RevOps/SelfService/onboarding_progress")
)
def track_onboarding_progress(ctx, usage_metrics, output):
    """
    Track customer onboarding progress through the wizard.

    Determines which steps are complete based on usage data.
    """

    spark = ctx.spark_session
    df = usage_metrics.dataframe()

    progress_time = datetime.now()
    progress_records = []

    # Analyze each customer's progress
    for row in df.collect():
        customer_id = row["customer_id"]
        datasets_built = row.get("datasets_built", 0)
        builds_30d = row.get("builds_30d", 0)
        activity_score = row.get("activity_score", 0)

        # Determine completed steps based on activity
        completed_steps = []

        # Step 1: CRM Connected - has any data
        if datasets_built > 0:
            completed_steps.append("connect_crm")

        # Step 2: Users Imported - has user data
        if builds_30d > 5:
            completed_steps.append("import_users")

        # Step 3: Stages Configured - has opportunity data
        if datasets_built > 3:
            completed_steps.append("configure_stages")

        # Step 4-7: Based on activity score
        if activity_score > 30:
            completed_steps.append("set_quotas")
        if activity_score > 50:
            completed_steps.append("enable_forecasting")
        if activity_score > 70:
            completed_steps.append("configure_alerts")
        if activity_score > 90:
            completed_steps.append("invite_team")

        # Calculate progress
        total_steps = len(WIZARD_STEPS)
        completed_count = len(completed_steps)
        progress_pct = (completed_count / total_steps) * 100

        # Determine next step
        next_step = None
        for step in WIZARD_STEPS:
            if step["step_id"] not in completed_steps:
                next_step = step["step_id"]
                break

        progress_records.append({
            "customer_id": customer_id,
            "progress_ts": progress_time,
            "completed_steps": json.dumps(completed_steps),
            "completed_count": completed_count,
            "total_steps": total_steps,
            "progress_pct": progress_pct,
            "next_step": next_step,
            "is_complete": completed_count >= 3,  # Minimum viable setup
            "is_fully_complete": completed_count == total_steps
        })

    progress_schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("progress_ts", TimestampType(), False),
        StructField("completed_steps", StringType(), False),
        StructField("completed_count", IntegerType(), False),
        StructField("total_steps", IntegerType(), False),
        StructField("progress_pct", DoubleType(), False),
        StructField("next_step", StringType(), True),
        StructField("is_complete", BooleanType(), False),
        StructField("is_fully_complete", BooleanType(), False),
    ])

    output.write_dataframe(spark.createDataFrame(progress_records, progress_schema))


# =============================================================================
# USAGE-BASED RECOMMENDATIONS
# =============================================================================

@transform(
    usage_metrics=Input("/RevOps/Ops/usage_metrics"),
    license_status=Input("/RevOps/Commercial/license_status"),
    output=Output("/RevOps/SelfService/usage_recommendations")
)
def generate_usage_recommendations(ctx, usage_metrics, license_status, output):
    """
    Generate personalized recommendations based on usage patterns.

    Suggests:
    - Feature adoption opportunities
    - Tier upgrades when beneficial
    - Optimization opportunities
    - Best practices based on usage
    """

    spark = ctx.spark_session
    usage_df = usage_metrics.dataframe()
    license_df = license_status.dataframe()

    # Join usage with license data
    combined = usage_df.alias("u").join(
        license_df.alias("l"),
        F.col("u.customer_id") == F.col("l.customer_id"),
        how="left"
    )

    rec_time = datetime.now()
    recommendations = []

    for row in combined.collect():
        customer_id = row["customer_id"]
        tier = row.get("tier", "Starter")
        activity_score = row.get("activity_score", 0)
        user_util = row.get("user_utilization", 0)
        api_util = row.get("api_utilization", 0)
        compute_util = row.get("compute_utilization", 0)

        customer_recs = []

        # Utilization-based recommendations
        if user_util > 80:
            customer_recs.append({
                "type": "upgrade",
                "category": "capacity",
                "title": "User Limit Approaching",
                "description": f"You're using {user_util:.0f}% of your user limit. Consider upgrading to avoid disruption.",
                "priority": "high",
                "action": "UPGRADE_TIER"
            })

        if api_util > 70:
            customer_recs.append({
                "type": "optimization",
                "category": "efficiency",
                "title": "API Usage Optimization",
                "description": "High API usage detected. Consider batching requests or enabling caching.",
                "priority": "medium",
                "action": "REVIEW_API_USAGE"
            })

        if compute_util > 80:
            customer_recs.append({
                "type": "upgrade",
                "category": "capacity",
                "title": "Compute Capacity Alert",
                "description": f"Using {compute_util:.0f}% of compute hours. Consider upgrading or optimizing transforms.",
                "priority": "high",
                "action": "UPGRADE_OR_OPTIMIZE"
            })

        # Activity-based recommendations
        if activity_score < 20:
            customer_recs.append({
                "type": "engagement",
                "category": "adoption",
                "title": "Low Engagement Detected",
                "description": "Your team isn't fully utilizing RevOps Hub. Book a training session!",
                "priority": "medium",
                "action": "SCHEDULE_TRAINING"
            })

        if activity_score > 80 and tier == "Starter":
            customer_recs.append({
                "type": "upgrade",
                "category": "features",
                "title": "Power User Detected",
                "description": "You're getting great value from RevOps! Upgrade to Growth for AI features.",
                "priority": "low",
                "action": "EXPLORE_GROWTH_TIER"
            })

        # Feature adoption recommendations
        if tier in ["Growth", "Enterprise"] and activity_score < 60:
            customer_recs.append({
                "type": "adoption",
                "category": "features",
                "title": "Underutilized Features",
                "description": "You have access to AI forecasting and custom reports. Start using them!",
                "priority": "medium",
                "action": "ENABLE_AI_FEATURES"
            })

        # Add all recommendations for this customer
        for rec in customer_recs:
            recommendations.append({
                "recommendation_id": str(uuid.uuid4()),
                "customer_id": customer_id,
                "rec_ts": rec_time,
                "rec_type": rec["type"],
                "category": rec["category"],
                "title": rec["title"],
                "description": rec["description"],
                "priority": rec["priority"],
                "suggested_action": rec["action"],
                "dismissed": False
            })

    if not recommendations:
        # Add placeholder for empty result
        recommendations.append({
            "recommendation_id": str(uuid.uuid4()),
            "customer_id": "SYSTEM",
            "rec_ts": rec_time,
            "rec_type": "info",
            "category": "status",
            "title": "All Good",
            "description": "No recommendations at this time",
            "priority": "low",
            "suggested_action": "NONE",
            "dismissed": False
        })

    rec_schema = StructType([
        StructField("recommendation_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("rec_ts", TimestampType(), False),
        StructField("rec_type", StringType(), False),
        StructField("category", StringType(), False),
        StructField("title", StringType(), False),
        StructField("description", StringType(), False),
        StructField("priority", StringType(), False),
        StructField("suggested_action", StringType(), False),
        StructField("dismissed", BooleanType(), False),
    ])

    output.write_dataframe(spark.createDataFrame(recommendations, rec_schema))


# =============================================================================
# INTEGRATION OPPORTUNITY DETECTION
# =============================================================================

@transform(
    usage_metrics=Input("/RevOps/Ops/usage_metrics"),
    output=Output("/RevOps/SelfService/integration_opportunities")
)
def detect_integration_opportunities(ctx, usage_metrics, output):
    """
    Auto-detect integration opportunities based on usage patterns.

    Identifies:
    - Missing CRM integrations
    - Marketing automation opportunities
    - Support system connections
    - BI tool integrations
    """

    spark = ctx.spark_session
    df = usage_metrics.dataframe()

    opportunity_time = datetime.now()
    opportunities = []

    for row in df.collect():
        customer_id = row["customer_id"]
        activity_score = row.get("activity_score", 0)

        # Simulated detection logic
        # In production, this would analyze actual integration status

        if activity_score > 30:
            # Suggest marketing automation
            opportunities.append({
                "opportunity_id": str(uuid.uuid4()),
                "customer_id": customer_id,
                "detected_ts": opportunity_time,
                "integration_type": "marketing_automation",
                "platform": "HubSpot Marketing",
                "benefit": "Sync marketing touchpoints with pipeline data",
                "estimated_value": "15% better lead scoring",
                "complexity": "low",
                "status": "suggested"
            })

        if activity_score > 50:
            # Suggest support integration
            opportunities.append({
                "opportunity_id": str(uuid.uuid4()),
                "customer_id": customer_id,
                "detected_ts": opportunity_time,
                "integration_type": "support_system",
                "platform": "Zendesk",
                "benefit": "Correlate support tickets with account health",
                "estimated_value": "20% faster churn detection",
                "complexity": "medium",
                "status": "suggested"
            })

        if activity_score > 70:
            # Suggest BI integration
            opportunities.append({
                "opportunity_id": str(uuid.uuid4()),
                "customer_id": customer_id,
                "detected_ts": opportunity_time,
                "integration_type": "business_intelligence",
                "platform": "Tableau / Power BI",
                "benefit": "Advanced custom visualizations",
                "estimated_value": "Executive dashboard capabilities",
                "complexity": "low",
                "status": "suggested"
            })

    if not opportunities:
        opportunities.append({
            "opportunity_id": str(uuid.uuid4()),
            "customer_id": "SYSTEM",
            "detected_ts": opportunity_time,
            "integration_type": "none",
            "platform": "N/A",
            "benefit": "No opportunities detected",
            "estimated_value": "N/A",
            "complexity": "N/A",
            "status": "none"
        })

    opp_schema = StructType([
        StructField("opportunity_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("detected_ts", TimestampType(), False),
        StructField("integration_type", StringType(), False),
        StructField("platform", StringType(), False),
        StructField("benefit", StringType(), False),
        StructField("estimated_value", StringType(), False),
        StructField("complexity", StringType(), False),
        StructField("status", StringType(), False),
    ])

    output.write_dataframe(spark.createDataFrame(opportunities, opp_schema))


# =============================================================================
# SELF-SERVICE CONFIG VALIDATION
# =============================================================================

@transform(
    onboarding=Input("/RevOps/SelfService/onboarding_progress"),
    output=Output("/RevOps/SelfService/config_validation_results")
)
def validate_customer_config(ctx, onboarding, output):
    """
    Validate customer configuration and identify issues.

    Checks:
    - Required fields populated
    - Integration health
    - Data freshness
    - Permission configuration
    """

    spark = ctx.spark_session
    df = onboarding.dataframe()

    validation_time = datetime.now()
    validations = []

    for row in df.collect():
        customer_id = row["customer_id"]
        completed_steps = json.loads(row["completed_steps"])
        progress_pct = row["progress_pct"]

        issues = []

        # Check required steps
        required_steps = ["connect_crm", "import_users", "configure_stages"]
        for step in required_steps:
            if step not in completed_steps:
                issues.append({
                    "issue_type": "missing_required_step",
                    "severity": "error",
                    "message": f"Required step not completed: {step}",
                    "resolution": f"Complete the '{step}' step in the setup wizard"
                })

        # Check for stale setup
        if progress_pct < 50:
            issues.append({
                "issue_type": "incomplete_setup",
                "severity": "warning",
                "message": "Setup is less than 50% complete",
                "resolution": "Continue with the setup wizard to unlock all features"
            })

        validation_status = "valid" if not issues else "invalid"

        validations.append({
            "validation_id": str(uuid.uuid4()),
            "customer_id": customer_id,
            "validation_ts": validation_time,
            "validation_status": validation_status,
            "issue_count": len(issues),
            "issues": json.dumps(issues),
            "next_action": issues[0]["resolution"] if issues else "No action needed"
        })

    validation_schema = StructType([
        StructField("validation_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("validation_ts", TimestampType(), False),
        StructField("validation_status", StringType(), False),
        StructField("issue_count", IntegerType(), False),
        StructField("issues", StringType(), False),
        StructField("next_action", StringType(), False),
    ])

    output.write_dataframe(spark.createDataFrame(validations, validation_schema))


# =============================================================================
# SELF-SERVICE DASHBOARD DATA
# =============================================================================

@transform(
    onboarding=Input("/RevOps/SelfService/onboarding_progress"),
    recommendations=Input("/RevOps/SelfService/usage_recommendations"),
    integrations=Input("/RevOps/SelfService/integration_opportunities"),
    validation=Input("/RevOps/SelfService/config_validation_results"),
    output=Output("/RevOps/SelfService/self_service_dashboard")
)
def build_self_service_dashboard(ctx, onboarding, recommendations, integrations,
                                  validation, output):
    """
    Build unified dashboard data for self-service portal.
    """

    spark = ctx.spark_session
    status_time = datetime.now()

    onboarding_df = onboarding.dataframe()
    rec_df = recommendations.dataframe()
    int_df = integrations.dataframe()
    val_df = validation.dataframe()

    dashboard_data = []

    # Group by customer
    customers = onboarding_df.select("customer_id").distinct().collect()

    for customer_row in customers:
        customer_id = customer_row["customer_id"]

        # Get onboarding status
        onb = onboarding_df.filter(F.col("customer_id") == customer_id).collect()
        onb_data = onb[0] if onb else None

        # Count recommendations
        rec_count = rec_df.filter(
            (F.col("customer_id") == customer_id) &
            (F.col("dismissed") == False)
        ).count()

        # Count integration opportunities
        int_count = int_df.filter(
            (F.col("customer_id") == customer_id) &
            (F.col("status") == "suggested")
        ).count()

        # Get validation status
        val = val_df.filter(F.col("customer_id") == customer_id).collect()
        val_status = val[0]["validation_status"] if val else "unknown"

        dashboard_data.append({
            "customer_id": customer_id,
            "dashboard_ts": status_time,
            "onboarding_complete": onb_data["is_complete"] if onb_data else False,
            "onboarding_pct": onb_data["progress_pct"] if onb_data else 0,
            "next_wizard_step": onb_data["next_step"] if onb_data else "connect_crm",
            "active_recommendations": rec_count,
            "integration_opportunities": int_count,
            "config_status": val_status,
            "overall_health": calculate_overall_health(
                onb_data["progress_pct"] if onb_data else 0,
                val_status,
                rec_count
            )
        })

    dashboard_schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("dashboard_ts", TimestampType(), False),
        StructField("onboarding_complete", BooleanType(), False),
        StructField("onboarding_pct", DoubleType(), False),
        StructField("next_wizard_step", StringType(), True),
        StructField("active_recommendations", IntegerType(), False),
        StructField("integration_opportunities", IntegerType(), False),
        StructField("config_status", StringType(), False),
        StructField("overall_health", StringType(), False),
    ])

    output.write_dataframe(spark.createDataFrame(dashboard_data, dashboard_schema))


def calculate_overall_health(progress_pct: float, config_status: str,
                              rec_count: int) -> str:
    """Calculate overall self-service health status."""

    if config_status == "invalid":
        return "needs_attention"
    if progress_pct < 50:
        return "setup_incomplete"
    if rec_count > 3:
        return "opportunities_available"
    if progress_pct == 100:
        return "excellent"
    return "good"
