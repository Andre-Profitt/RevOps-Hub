"""
RevOps SDK Models
Pydantic models for API types
"""

from datetime import datetime
from enum import Enum
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field


# =============================================================================
# EVENT TYPES
# =============================================================================


class EventType(str, Enum):
    """Event types supported by RevOps webhooks"""

    TENANT_CREATED = "tenant.created"
    TENANT_UPDATED = "tenant.updated"
    TENANT_SUSPENDED = "tenant.suspended"
    TENANT_DELETED = "tenant.deleted"

    IMPLEMENTATION_STAGE_CHANGED = "implementation.stage_changed"
    IMPLEMENTATION_BLOCKED = "implementation.blocked"
    IMPLEMENTATION_COMPLETED = "implementation.completed"
    IMPLEMENTATION_CHECKLIST_UPDATED = "implementation.checklist_updated"

    HEALTH_SCORE_CHANGED = "health.score_changed"
    HEALTH_ALERT_TRIGGERED = "health.alert_triggered"
    HEALTH_THRESHOLD_BREACHED = "health.threshold_breached"

    DATA_SYNC_COMPLETED = "data.sync_completed"
    DATA_SYNC_FAILED = "data.sync_failed"
    DATA_QUALITY_ALERT = "data.quality_alert"

    AI_PREDICTION_GENERATED = "ai.prediction_generated"
    AI_INSIGHT_CREATED = "ai.insight_created"
    AI_MODEL_RETRAINED = "ai.model_retrained"

    BILLING_PLAN_CHANGED = "billing.plan_changed"
    BILLING_USAGE_THRESHOLD = "billing.usage_threshold"
    BILLING_INVOICE_GENERATED = "billing.invoice_generated"

    USER_INVITED = "user.invited"
    USER_ACTIVATED = "user.activated"
    USER_ROLE_CHANGED = "user.role_changed"

    INTEGRATION_CONNECTED = "integration.connected"
    INTEGRATION_DISCONNECTED = "integration.disconnected"


# =============================================================================
# TENANT
# =============================================================================


class TenantTier(str, Enum):
    STARTER = "starter"
    GROWTH = "growth"
    ENTERPRISE = "enterprise"


class TenantStatus(str, Enum):
    ACTIVE = "active"
    SUSPENDED = "suspended"
    PENDING = "pending"


class CrmType(str, Enum):
    SALESFORCE = "salesforce"
    HUBSPOT = "hubspot"
    DYNAMICS = "dynamics"


class TenantConfig(BaseModel):
    """Tenant configuration settings"""

    crm_type: CrmType
    timezone: str = "UTC"
    fiscal_year_start: int = Field(ge=1, le=12, default=1)
    currency: str = "USD"
    features: list[str] = Field(default_factory=list)


class Tenant(BaseModel):
    """Tenant/customer record"""

    id: str
    name: str
    tier: TenantTier
    status: TenantStatus
    created_at: datetime
    config: TenantConfig


# =============================================================================
# IMPLEMENTATION
# =============================================================================


class ImplementationStage(str, Enum):
    ONBOARDING = "onboarding"
    CONNECTOR_SETUP = "connector_setup"
    DASHBOARD_CONFIG = "dashboard_config"
    AGENT_TRAINING = "agent_training"
    LIVE = "live"


class ImplementationStatus(BaseModel):
    """Implementation progress for a tenant"""

    customer_id: str
    current_stage: ImplementationStage
    completion_percentage: float = Field(ge=0, le=100)
    blocked_items: list[str] = Field(default_factory=list)
    started_at: datetime
    estimated_completion_date: datetime | None = None
    checklist_progress: dict[str, bool] = Field(default_factory=dict)


# =============================================================================
# HEALTH
# =============================================================================


class HealthTrend(str, Enum):
    IMPROVING = "improving"
    STABLE = "stable"
    DECLINING = "declining"


class HealthDimensions(BaseModel):
    """Health score breakdown by dimension"""

    data_freshness: float = Field(ge=0, le=100)
    adoption: float = Field(ge=0, le=100)
    ai_accuracy: float = Field(ge=0, le=100)
    build_health: float = Field(ge=0, le=100)


class HealthScore(BaseModel):
    """Health score for a tenant"""

    customer_id: str
    overall_score: float = Field(ge=0, le=100)
    dimensions: HealthDimensions
    trend: HealthTrend
    last_updated: datetime


# =============================================================================
# ROI METRICS
# =============================================================================


class ROIMetrics(BaseModel):
    """ROI metrics for a tenant"""

    customer_id: str
    win_rate_improvement: float  # percentage points
    cycle_time_reduction: float  # percentage
    forecast_accuracy_gain: float  # percentage points
    data_hygiene_improvement: float  # percentage points
    estimated_annual_value: float  # USD
    period_start: datetime
    period_end: datetime


# =============================================================================
# WEBHOOKS
# =============================================================================


class DeliveryStatus(str, Enum):
    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"
    RETRYING = "retrying"


class WebhookSubscription(BaseModel):
    """Webhook subscription configuration"""

    id: str
    name: str
    url: str
    events: list[EventType]
    is_active: bool = True
    created_at: datetime
    last_triggered_at: datetime | None = None


class WebhookDelivery(BaseModel):
    """Webhook delivery record"""

    id: str
    webhook_id: str
    event_type: EventType
    status: DeliveryStatus
    attempt_count: int = 0
    response_code: int | None = None
    created_at: datetime
    completed_at: datetime | None = None


class WebhookMeta(BaseModel):
    """Webhook payload metadata"""

    webhook_id: str
    webhook_name: str
    attempt_number: int
    delivered_at: datetime


class WebhookPayload(BaseModel):
    """Webhook payload structure"""

    delivery_id: str
    event_id: str
    event_type: EventType
    customer_id: str
    timestamp: datetime
    data: dict[str, Any]
    meta: WebhookMeta


# =============================================================================
# API RESPONSE
# =============================================================================

T = TypeVar("T")


class ApiError(BaseModel):
    """API error details"""

    code: str
    message: str
    details: dict[str, Any] | None = None


class ApiMeta(BaseModel):
    """API response metadata"""

    request_id: str
    timestamp: datetime


class ApiResponse(BaseModel, Generic[T]):
    """Standard API response wrapper"""

    success: bool
    data: T | None = None
    error: ApiError | None = None
    meta: ApiMeta | None = None


class PaginationInfo(BaseModel):
    """Pagination details"""

    page: int
    page_size: int
    total_items: int
    total_pages: int
    has_next: bool
    has_prev: bool


class PaginatedResponse(ApiResponse[list[T]], Generic[T]):
    """Paginated API response"""

    pagination: PaginationInfo | None = None


__all__ = [
    # Enums
    "EventType",
    "TenantTier",
    "TenantStatus",
    "CrmType",
    "ImplementationStage",
    "HealthTrend",
    "DeliveryStatus",
    # Models
    "TenantConfig",
    "Tenant",
    "ImplementationStatus",
    "HealthDimensions",
    "HealthScore",
    "ROIMetrics",
    "WebhookSubscription",
    "WebhookDelivery",
    "WebhookMeta",
    "WebhookPayload",
    # API Response
    "ApiError",
    "ApiMeta",
    "ApiResponse",
    "PaginationInfo",
    "PaginatedResponse",
]
