"""
RevOps API Client
Python client for the RevOps Command Center API
"""

from dataclasses import dataclass, field
from typing import Any

import httpx

from revops_sdk.models import (
    ApiResponse,
    HealthScore,
    ImplementationStatus,
    PaginatedResponse,
    ROIMetrics,
    Tenant,
    WebhookDelivery,
    WebhookSubscription,
    EventType,
)


# =============================================================================
# CONFIG
# =============================================================================


@dataclass
class RevOpsConfig:
    """Configuration for RevOps API client"""

    base_url: str
    api_key: str
    webhook_secret: str = ""
    timeout: float = 30.0
    debug: bool = False


# =============================================================================
# CLIENT
# =============================================================================


class RevOpsClient:
    """
    Async client for the RevOps Command Center API.

    Example:
        >>> from revops_sdk import RevOpsClient
        >>>
        >>> async with RevOpsClient(
        ...     base_url="https://api.revops.io",
        ...     api_key="your-api-key",
        ... ) as client:
        ...     health = await client.get_health_score("tenant-123")
        ...     print(f"Health: {health.data.overall_score}")
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        webhook_secret: str = "",
        timeout: float = 30.0,
        debug: bool = False,
    ) -> None:
        """
        Initialize RevOps client.

        Args:
            base_url: API base URL
            api_key: API key for authentication
            webhook_secret: Secret for webhook verification (optional)
            timeout: Request timeout in seconds
            debug: Enable debug logging
        """
        self.config = RevOpsConfig(
            base_url=base_url.rstrip("/"),
            api_key=api_key,
            webhook_secret=webhook_secret,
            timeout=timeout,
            debug=debug,
        )
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "RevOpsClient":
        """Enter async context."""
        self._client = httpx.AsyncClient(
            base_url=self.config.base_url,
            headers={
                "Authorization": f"Bearer {self.config.api_key}",
                "Content-Type": "application/json",
                "User-Agent": "RevOps-SDK-Python/0.1.0",
            },
            timeout=self.config.timeout,
        )
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit async context."""
        if self._client:
            await self._client.aclose()
            self._client = None

    @property
    def client(self) -> httpx.AsyncClient:
        """Get HTTP client, raising if not in context."""
        if self._client is None:
            raise RuntimeError(
                "Client not initialized. Use 'async with RevOpsClient(...) as client:'"
            )
        return self._client

    # =========================================================================
    # HTTP HELPERS
    # =========================================================================

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Make HTTP request and return JSON response."""
        if self.config.debug:
            print(f"[RevOps] {method} {path}")

        response = await self.client.request(
            method,
            path,
            json=json,
            params=params,
        )

        data = response.json()

        if not response.is_success:
            return {
                "success": False,
                "error": {
                    "code": data.get("error", {}).get("code", "API_ERROR"),
                    "message": data.get("error", {}).get("message", response.reason_phrase),
                    "details": data.get("error", {}).get("details"),
                },
                "meta": {
                    "request_id": response.headers.get("x-request-id", ""),
                    "timestamp": response.headers.get("date", ""),
                },
            }

        return {
            "success": True,
            "data": data.get("data", data),
            "meta": {
                "request_id": response.headers.get("x-request-id", ""),
                "timestamp": response.headers.get("date", ""),
            },
        }

    # =========================================================================
    # TENANTS
    # =========================================================================

    async def get_tenant(self, tenant_id: str) -> ApiResponse[Tenant]:
        """
        Get tenant by ID.

        Args:
            tenant_id: Tenant ID

        Returns:
            API response with tenant data
        """
        data = await self._request("GET", f"/api/v1/tenants/{tenant_id}")
        return ApiResponse[Tenant].model_validate(data)

    async def list_tenants(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        status: str | None = None,
    ) -> PaginatedResponse[Tenant]:
        """
        List all tenants.

        Args:
            page: Page number
            page_size: Number of items per page
            status: Filter by status (active, suspended, pending)

        Returns:
            Paginated response with tenants
        """
        params: dict[str, Any] = {"page": page, "pageSize": page_size}
        if status:
            params["status"] = status

        data = await self._request("GET", "/api/v1/tenants", params=params)
        return PaginatedResponse[Tenant].model_validate(data)

    async def update_tenant(
        self,
        tenant_id: str,
        updates: dict[str, Any],
    ) -> ApiResponse[Tenant]:
        """
        Update tenant configuration.

        Args:
            tenant_id: Tenant ID
            updates: Fields to update

        Returns:
            API response with updated tenant
        """
        data = await self._request("PATCH", f"/api/v1/tenants/{tenant_id}", json=updates)
        return ApiResponse[Tenant].model_validate(data)

    # =========================================================================
    # IMPLEMENTATION STATUS
    # =========================================================================

    async def get_implementation_status(
        self,
        tenant_id: str,
    ) -> ApiResponse[ImplementationStatus]:
        """
        Get implementation status for a tenant.

        Args:
            tenant_id: Tenant ID

        Returns:
            API response with implementation status
        """
        data = await self._request("GET", f"/api/v1/tenants/{tenant_id}/implementation")
        return ApiResponse[ImplementationStatus].model_validate(data)

    async def update_checklist_item(
        self,
        tenant_id: str,
        item_id: str,
        completed: bool,
    ) -> ApiResponse[ImplementationStatus]:
        """
        Update implementation checklist item.

        Args:
            tenant_id: Tenant ID
            item_id: Checklist item ID
            completed: Whether item is completed

        Returns:
            API response with updated status
        """
        data = await self._request(
            "PATCH",
            f"/api/v1/tenants/{tenant_id}/implementation/checklist/{item_id}",
            json={"completed": completed},
        )
        return ApiResponse[ImplementationStatus].model_validate(data)

    async def complete_stage(
        self,
        tenant_id: str,
        stage: str,
    ) -> ApiResponse[ImplementationStatus]:
        """
        Mark implementation stage as complete.

        Args:
            tenant_id: Tenant ID
            stage: Stage to complete

        Returns:
            API response with updated status
        """
        data = await self._request(
            "POST",
            f"/api/v1/tenants/{tenant_id}/implementation/complete-stage",
            json={"stage": stage},
        )
        return ApiResponse[ImplementationStatus].model_validate(data)

    # =========================================================================
    # HEALTH SCORES
    # =========================================================================

    async def get_health_score(self, tenant_id: str) -> ApiResponse[HealthScore]:
        """
        Get health score for a tenant.

        Args:
            tenant_id: Tenant ID

        Returns:
            API response with health score
        """
        data = await self._request("GET", f"/api/v1/tenants/{tenant_id}/health")
        return ApiResponse[HealthScore].model_validate(data)

    async def get_health_history(
        self,
        tenant_id: str,
        *,
        days: int = 30,
    ) -> ApiResponse[list[HealthScore]]:
        """
        Get health score history for a tenant.

        Args:
            tenant_id: Tenant ID
            days: Number of days of history

        Returns:
            API response with health history
        """
        data = await self._request(
            "GET",
            f"/api/v1/tenants/{tenant_id}/health/history",
            params={"days": days},
        )
        return ApiResponse[list[HealthScore]].model_validate(data)

    # =========================================================================
    # ROI METRICS
    # =========================================================================

    async def get_roi_metrics(self, tenant_id: str) -> ApiResponse[ROIMetrics]:
        """
        Get ROI metrics for a tenant.

        Args:
            tenant_id: Tenant ID

        Returns:
            API response with ROI metrics
        """
        data = await self._request("GET", f"/api/v1/tenants/{tenant_id}/roi")
        return ApiResponse[ROIMetrics].model_validate(data)

    async def get_roi_comparison(
        self,
        tenant_id: str,
    ) -> ApiResponse[dict[str, ROIMetrics]]:
        """
        Get ROI comparison (baseline vs current).

        Args:
            tenant_id: Tenant ID

        Returns:
            API response with baseline, current, and improvement metrics
        """
        data = await self._request("GET", f"/api/v1/tenants/{tenant_id}/roi/comparison")
        return ApiResponse[dict[str, ROIMetrics]].model_validate(data)

    # =========================================================================
    # WEBHOOKS
    # =========================================================================

    async def list_webhooks(self) -> ApiResponse[list[WebhookSubscription]]:
        """
        List all webhook subscriptions.

        Returns:
            API response with webhooks
        """
        data = await self._request("GET", "/api/v1/webhooks")
        return ApiResponse[list[WebhookSubscription]].model_validate(data)

    async def get_webhook(self, webhook_id: str) -> ApiResponse[WebhookSubscription]:
        """
        Get webhook by ID.

        Args:
            webhook_id: Webhook ID

        Returns:
            API response with webhook
        """
        data = await self._request("GET", f"/api/v1/webhooks/{webhook_id}")
        return ApiResponse[WebhookSubscription].model_validate(data)

    async def create_webhook(
        self,
        name: str,
        url: str,
        events: list[EventType | str],
        *,
        description: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> ApiResponse[WebhookSubscription]:
        """
        Create webhook subscription.

        Args:
            name: Webhook name
            url: Target URL
            events: Event types to subscribe to
            description: Optional description
            headers: Optional custom headers

        Returns:
            API response with created webhook
        """
        payload: dict[str, Any] = {
            "name": name,
            "url": url,
            "events": [e.value if isinstance(e, EventType) else e for e in events],
        }
        if description:
            payload["description"] = description
        if headers:
            payload["headers"] = headers

        data = await self._request("POST", "/api/v1/webhooks", json=payload)
        return ApiResponse[WebhookSubscription].model_validate(data)

    async def update_webhook(
        self,
        webhook_id: str,
        updates: dict[str, Any],
    ) -> ApiResponse[WebhookSubscription]:
        """
        Update webhook subscription.

        Args:
            webhook_id: Webhook ID
            updates: Fields to update

        Returns:
            API response with updated webhook
        """
        data = await self._request("PATCH", f"/api/v1/webhooks/{webhook_id}", json=updates)
        return ApiResponse[WebhookSubscription].model_validate(data)

    async def delete_webhook(self, webhook_id: str) -> ApiResponse[None]:
        """
        Delete webhook subscription.

        Args:
            webhook_id: Webhook ID

        Returns:
            API response
        """
        data = await self._request("DELETE", f"/api/v1/webhooks/{webhook_id}")
        return ApiResponse[None].model_validate(data)

    async def get_webhook_deliveries(
        self,
        webhook_id: str,
        *,
        page: int = 1,
        page_size: int = 20,
    ) -> PaginatedResponse[WebhookDelivery]:
        """
        Get webhook delivery history.

        Args:
            webhook_id: Webhook ID
            page: Page number
            page_size: Items per page

        Returns:
            Paginated response with deliveries
        """
        data = await self._request(
            "GET",
            f"/api/v1/webhooks/{webhook_id}/deliveries",
            params={"page": page, "pageSize": page_size},
        )
        return PaginatedResponse[WebhookDelivery].model_validate(data)

    async def retry_webhook_delivery(
        self,
        webhook_id: str,
        delivery_id: str,
    ) -> ApiResponse[WebhookDelivery]:
        """
        Retry a failed webhook delivery.

        Args:
            webhook_id: Webhook ID
            delivery_id: Delivery ID

        Returns:
            API response with delivery status
        """
        data = await self._request(
            "POST",
            f"/api/v1/webhooks/{webhook_id}/deliveries/{delivery_id}/retry",
            json={},
        )
        return ApiResponse[WebhookDelivery].model_validate(data)

    async def test_webhook(
        self,
        webhook_id: str,
        event_type: EventType | str | None = None,
    ) -> ApiResponse[WebhookDelivery]:
        """
        Test webhook endpoint with sample event.

        Args:
            webhook_id: Webhook ID
            event_type: Optional event type for test

        Returns:
            API response with test delivery
        """
        payload: dict[str, Any] = {}
        if event_type:
            payload["eventType"] = (
                event_type.value if isinstance(event_type, EventType) else event_type
            )

        data = await self._request(
            "POST",
            f"/api/v1/webhooks/{webhook_id}/test",
            json=payload,
        )
        return ApiResponse[WebhookDelivery].model_validate(data)

    # =========================================================================
    # UTILITIES
    # =========================================================================

    def get_webhook_secret(self) -> str:
        """Get configured webhook secret."""
        return self.config.webhook_secret

    async def health_check(self) -> ApiResponse[dict[str, str]]:
        """
        Check API health.

        Returns:
            API response with status and version
        """
        data = await self._request("GET", "/api/v1/health")
        return ApiResponse[dict[str, str]].model_validate(data)


# =============================================================================
# FACTORY
# =============================================================================


def create_client(
    base_url: str,
    api_key: str,
    **kwargs: Any,
) -> RevOpsClient:
    """
    Create a RevOps API client.

    Args:
        base_url: API base URL
        api_key: API key for authentication
        **kwargs: Additional configuration options

    Returns:
        RevOpsClient instance

    Example:
        >>> from revops_sdk import create_client
        >>>
        >>> client = create_client(
        ...     base_url="https://api.revops.io",
        ...     api_key=os.environ["REVOPS_API_KEY"],
        ... )
        >>>
        >>> async with client:
        ...     health = await client.get_health_score("tenant-123")
    """
    return RevOpsClient(base_url, api_key, **kwargs)


__all__ = [
    "RevOpsConfig",
    "RevOpsClient",
    "create_client",
]
