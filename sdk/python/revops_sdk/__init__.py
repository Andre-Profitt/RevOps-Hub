"""
RevOps SDK for Python
Official SDK for the RevOps Command Center

Example:
    >>> from revops_sdk import RevOpsClient, verify_webhook
    >>>
    >>> # Create API client
    >>> client = RevOpsClient(
    ...     base_url="https://api.revops.io",
    ...     api_key="your-api-key",
    ... )
    >>>
    >>> # Get tenant health
    >>> health = await client.get_health_score("tenant-123")
    >>>
    >>> # Verify incoming webhooks
    >>> result = verify_webhook(body, signature, secret)
    >>> if result.valid:
    ...     print(f"Event: {result.payload.event_type}")
"""

from revops_sdk.client import RevOpsClient, create_client
from revops_sdk.webhooks import (
    verify_webhook,
    WebhookRouter,
    WebhookMiddleware,
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    DELIVERY_ID_HEADER,
)
from revops_sdk.models import (
    Tenant,
    TenantConfig,
    ImplementationStatus,
    HealthScore,
    ROIMetrics,
    WebhookSubscription,
    WebhookDelivery,
    WebhookPayload,
    EventType,
    ApiResponse,
)

__version__ = "0.1.0"
__all__ = [
    # Client
    "RevOpsClient",
    "create_client",
    # Webhooks
    "verify_webhook",
    "WebhookRouter",
    "WebhookMiddleware",
    "SIGNATURE_HEADER",
    "TIMESTAMP_HEADER",
    "DELIVERY_ID_HEADER",
    # Models
    "Tenant",
    "TenantConfig",
    "ImplementationStatus",
    "HealthScore",
    "ROIMetrics",
    "WebhookSubscription",
    "WebhookDelivery",
    "WebhookPayload",
    "EventType",
    "ApiResponse",
]
