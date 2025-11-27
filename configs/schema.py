# /configs/schema.py
"""
CONFIGURATION SCHEMA
====================
Defines the structure for customer and environment configurations.
Provides validation, defaults, and tier-based feature resolution.

Usage:
    from configs.schema import CustomerSettings, load_customer_config

    settings = load_customer_config("acme-corp")
    if settings.features.deal_predictions:
        # Enable ML predictions
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any
from enum import Enum
import json
from pathlib import Path


# =============================================================================
# ENUMS
# =============================================================================

class Tier(str, Enum):
    """Subscription tier levels."""
    STARTER = "starter"
    GROWTH = "growth"
    ENTERPRISE = "enterprise"


class CRMType(str, Enum):
    """Supported CRM systems."""
    SALESFORCE = "salesforce"
    HUBSPOT = "hubspot"


class AlertChannel(str, Enum):
    """Alert delivery channels."""
    EMAIL = "email"
    SLACK = "slack"
    WEBHOOK = "webhook"
    PAGERDUTY = "pagerduty"


# =============================================================================
# FEATURE FLAGS
# =============================================================================

@dataclass
class FeatureFlags:
    """Feature availability by tier."""
    # Core (all tiers)
    pipeline_health: bool = True
    pipeline_hygiene: bool = True
    forecast_summary: bool = True

    # Growth+
    deal_predictions: bool = False
    coaching_insights: bool = False
    rep_performance: bool = False
    activity_analytics: bool = False

    # Enterprise only
    scenario_modeling: bool = False
    territory_planning: bool = False
    custom_integrations: bool = False
    api_access: bool = False
    sso_enabled: bool = False
    white_labeling: bool = False
    data_export: bool = False

    @classmethod
    def for_tier(cls, tier: Tier) -> "FeatureFlags":
        """Get feature flags for a specific tier."""
        if tier == Tier.STARTER:
            return cls()
        elif tier == Tier.GROWTH:
            return cls(
                deal_predictions=True,
                coaching_insights=True,
                rep_performance=True,
                activity_analytics=True,
            )
        else:  # ENTERPRISE
            return cls(
                deal_predictions=True,
                coaching_insights=True,
                rep_performance=True,
                activity_analytics=True,
                scenario_modeling=True,
                territory_planning=True,
                custom_integrations=True,
                api_access=True,
                sso_enabled=True,
                white_labeling=True,
                data_export=True,
            )


# =============================================================================
# THRESHOLDS
# =============================================================================

@dataclass
class HealthThresholds:
    """Thresholds for health scoring."""
    critical: float = 40.0      # Below this = Critical
    at_risk: float = 60.0       # Below this = At Risk
    monitor: float = 80.0       # Below this = Monitor
    healthy: float = 80.0       # At or above = Healthy


@dataclass
class HygieneThresholds:
    """Thresholds for pipeline hygiene alerts."""
    days_stale_warning: int = 7
    days_stale_critical: int = 14
    days_past_close_warning: int = 7
    days_past_close_critical: int = 30
    missing_next_step_days: int = 3
    missing_amount_threshold: float = 0.0


@dataclass
class ForecastThresholds:
    """Thresholds for forecast and coverage."""
    pipeline_coverage_target: float = 3.0
    commit_coverage_target: float = 1.2
    win_rate_benchmark: float = 0.25
    avg_cycle_days_benchmark: int = 45
    quota_attainment_target: float = 1.0


@dataclass
class Thresholds:
    """Combined threshold configuration."""
    health: HealthThresholds = field(default_factory=HealthThresholds)
    hygiene: HygieneThresholds = field(default_factory=HygieneThresholds)
    forecast: ForecastThresholds = field(default_factory=ForecastThresholds)


# =============================================================================
# PIPELINE STAGES
# =============================================================================

@dataclass
class StageConfig:
    """Configuration for a pipeline stage."""
    name: str
    order: int
    probability: float
    benchmark_days: int
    is_closed: bool = False
    is_won: bool = False


DEFAULT_STAGES = [
    StageConfig("Prospecting", 1, 0.10, 14),
    StageConfig("Discovery", 2, 0.20, 21),
    StageConfig("Solution Design", 3, 0.40, 14),
    StageConfig("Proposal", 4, 0.60, 7),
    StageConfig("Negotiation", 5, 0.75, 14),
    StageConfig("Verbal Commit", 6, 0.90, 7),
    StageConfig("Closed Won", 7, 1.00, 0, is_closed=True, is_won=True),
    StageConfig("Closed Lost", 8, 0.00, 0, is_closed=True, is_won=False),
]


# =============================================================================
# SEGMENTS
# =============================================================================

@dataclass
class SegmentConfig:
    """Configuration for account/deal segmentation."""
    name: str
    min_revenue: Optional[float] = None
    max_revenue: Optional[float] = None
    min_employees: Optional[int] = None
    max_employees: Optional[int] = None


DEFAULT_SEGMENTS = [
    SegmentConfig("Enterprise", min_revenue=100_000_000, min_employees=1000),
    SegmentConfig("Mid-Market", min_revenue=10_000_000, min_employees=100),
    SegmentConfig("SMB", min_revenue=1_000_000, min_employees=10),
    SegmentConfig("Velocity", max_revenue=1_000_000, max_employees=10),
]


# =============================================================================
# SCHEMA ALIASES (for config validation)
# =============================================================================

@dataclass
class TierConfig:
    """Canonical tier definition schema for validation."""

    tier_name: str
    display_name: str
    price_monthly: Optional[float]
    price_annual: Optional[float]
    limits: Dict[str, Any]
    features: List[str]
    sla: Dict[str, Any] = field(default_factory=dict)
    billing: Dict[str, Any] = field(default_factory=dict)


# =============================================================================
# ALERTING
# =============================================================================

@dataclass
class AlertRule:
    """Configuration for an alert rule."""
    name: str
    enabled: bool = True
    severity: str = "warning"  # info, warning, critical
    channels: List[AlertChannel] = field(default_factory=list)
    threshold: Optional[float] = None
    cooldown_minutes: int = 60


@dataclass
class AlertConfig:
    """Alert configuration."""
    enabled: bool = True
    default_channels: List[AlertChannel] = field(default_factory=lambda: [AlertChannel.EMAIL])

    # Slack config
    slack_webhook_url: Optional[str] = None
    slack_channel: Optional[str] = None

    # Email config
    email_recipients: List[str] = field(default_factory=list)
    email_from: str = "alerts@revops-hub.io"

    # PagerDuty config
    pagerduty_service_key: Optional[str] = None

    # Rules
    rules: List[AlertRule] = field(default_factory=lambda: [
        AlertRule("stale_pipeline", threshold=0.1, severity="warning"),
        AlertRule("build_failure", severity="critical"),
        AlertRule("data_quality_drop", threshold=0.05, severity="warning"),
        AlertRule("sync_delay", threshold=60, severity="warning"),  # minutes
    ])


# =============================================================================
# SCHEDULING
# =============================================================================

@dataclass
class ScheduleConfig:
    """Build schedule configuration."""
    name: str
    cron: str
    datasets: List[str]
    enabled: bool = True
    retry_count: int = 2
    timeout_minutes: int = 60


DEFAULT_SCHEDULES = [
    ScheduleConfig(
        "hourly-sync",
        "0 * * * *",
        ["/RevOps/Staging/opportunities", "/RevOps/Staging/activities"],
    ),
    ScheduleConfig(
        "daily-analytics",
        "0 6 * * *",
        [
            "/RevOps/Enriched/deal_health_scores",
            "/RevOps/Analytics/pipeline_health_summary",
            "/RevOps/Analytics/pipeline_hygiene_alerts",
            "/RevOps/Dashboard/kpis",
        ],
    ),
    ScheduleConfig(
        "weekly-reports",
        "0 8 * * 1",
        ["/RevOps/Analytics/forecast_summary", "/RevOps/Coaching/rep_performance"],
    ),
]


# =============================================================================
# CUSTOMER SETTINGS (MAIN CONFIG)
# =============================================================================

@dataclass
class CustomerSettings:
    """Complete customer configuration."""
    # Identity
    customer_id: str
    customer_name: str
    tier: Tier = Tier.STARTER

    # CRM
    primary_crm: CRMType = CRMType.SALESFORCE
    crm_instance_url: Optional[str] = None

    # Localization
    timezone: str = "America/New_York"
    currency: str = "USD"
    currency_symbol: str = "$"
    date_format: str = "YYYY-MM-DD"
    fiscal_year_start: str = "01-01"  # MM-DD

    # Features and limits
    features: FeatureFlags = field(default_factory=FeatureFlags)
    max_users: int = 10
    max_opportunities: int = 10000
    data_retention_days: int = 365

    # Thresholds
    thresholds: Thresholds = field(default_factory=Thresholds)

    # Pipeline
    stages: List[StageConfig] = field(default_factory=lambda: DEFAULT_STAGES.copy())
    segments: List[SegmentConfig] = field(default_factory=lambda: DEFAULT_SEGMENTS.copy())

    # Alerting
    alerts: AlertConfig = field(default_factory=AlertConfig)

    # Scheduling
    schedules: List[ScheduleConfig] = field(default_factory=lambda: DEFAULT_SCHEDULES.copy())

    # Custom fields (customer-specific extensions)
    custom_fields: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Apply tier-based defaults if not overridden."""
        if isinstance(self.tier, str):
            self.tier = Tier(self.tier)
        if isinstance(self.primary_crm, str):
            self.primary_crm = CRMType(self.primary_crm)

        # Apply tier features if using defaults
        if self.features == FeatureFlags():
            self.features = FeatureFlags.for_tier(self.tier)

        # Apply tier limits
        tier_limits = {
            Tier.STARTER: {"max_users": 10, "max_opportunities": 10000},
            Tier.GROWTH: {"max_users": 50, "max_opportunities": 50000},
            Tier.ENTERPRISE: {"max_users": 500, "max_opportunities": 500000},
        }
        limits = tier_limits.get(self.tier, tier_limits[Tier.STARTER])
        if self.max_users == 10:  # default
            self.max_users = limits["max_users"]
        if self.max_opportunities == 10000:  # default
            self.max_opportunities = limits["max_opportunities"]


    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return asdict(self)

    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string."""
        def serialize(obj):
            if isinstance(obj, Enum):
                return obj.value
            return obj

        d = self.to_dict()
        return json.dumps(d, indent=indent, default=serialize)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CustomerSettings":
        """Create from dictionary."""
        # Handle nested dataclasses
        if "features" in data and isinstance(data["features"], dict):
            data["features"] = FeatureFlags(**data["features"])

        if "thresholds" in data and isinstance(data["thresholds"], dict):
            t = data["thresholds"]
            data["thresholds"] = Thresholds(
                health=HealthThresholds(**t.get("health", {})),
                hygiene=HygieneThresholds(**t.get("hygiene", {})),
                forecast=ForecastThresholds(**t.get("forecast", {})),
            )

        if "alerts" in data and isinstance(data["alerts"], dict):
            a = data["alerts"]
            if "default_channels" in a:
                a["default_channels"] = [AlertChannel(c) for c in a["default_channels"]]
            if "rules" in a:
                a["rules"] = [AlertRule(**r) for r in a["rules"]]
            data["alerts"] = AlertConfig(**a)

        if "stages" in data:
            data["stages"] = [StageConfig(**s) for s in data["stages"]]

        if "segments" in data:
            data["segments"] = [SegmentConfig(**s) for s in data["segments"]]

        if "schedules" in data:
            data["schedules"] = [ScheduleConfig(**s) for s in data["schedules"]]

        return cls(**data)


@dataclass
class CustomerConfig(CustomerSettings):
    """Alias to align validation expectations with config schema."""

    def __post_init__(self):
        super().__post_init__()


# =============================================================================
# CONFIG LOADING
# =============================================================================

CONFIG_DIR = Path(__file__).parent


def load_customer_config(customer_id: str) -> CustomerSettings:
    """
    Load customer configuration from file.

    Looks for configs/{customer_id}.json
    Falls back to default configuration if not found.
    """
    config_path = CONFIG_DIR / f"{customer_id}.json"

    if config_path.exists():
        with open(config_path, "r") as f:
            data = json.load(f)
        return CustomerSettings.from_dict(data)

    # Return default config with customer_id
    return CustomerSettings(
        customer_id=customer_id,
        customer_name=customer_id.replace("-", " ").title(),
    )


def save_customer_config(settings: CustomerSettings) -> Path:
    """Save customer configuration to file."""
    config_path = CONFIG_DIR / f"{settings.customer_id}.json"

    with open(config_path, "w") as f:
        f.write(settings.to_json())

    return config_path


def list_customer_configs() -> List[str]:
    """List all available customer configurations."""
    return [
        p.stem for p in CONFIG_DIR.glob("*.json")
        if p.stem not in ("schema", "__init__")
    ]


# =============================================================================
# CONFIG TEMPLATES
# =============================================================================

def create_starter_template(customer_id: str, customer_name: str) -> CustomerSettings:
    """Create a starter tier configuration."""
    return CustomerSettings(
        customer_id=customer_id,
        customer_name=customer_name,
        tier=Tier.STARTER,
    )


def create_growth_template(customer_id: str, customer_name: str) -> CustomerSettings:
    """Create a growth tier configuration."""
    return CustomerSettings(
        customer_id=customer_id,
        customer_name=customer_name,
        tier=Tier.GROWTH,
        alerts=AlertConfig(
            enabled=True,
            default_channels=[AlertChannel.EMAIL, AlertChannel.SLACK],
        ),
    )


def create_enterprise_template(
    customer_id: str,
    customer_name: str,
    crm: CRMType = CRMType.SALESFORCE,
) -> CustomerSettings:
    """Create an enterprise tier configuration."""
    return CustomerSettings(
        customer_id=customer_id,
        customer_name=customer_name,
        tier=Tier.ENTERPRISE,
        primary_crm=crm,
        data_retention_days=730,  # 2 years
        alerts=AlertConfig(
            enabled=True,
            default_channels=[AlertChannel.EMAIL, AlertChannel.SLACK, AlertChannel.PAGERDUTY],
        ),
    )
