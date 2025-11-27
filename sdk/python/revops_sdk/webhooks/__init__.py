"""
Webhook Utilities
Verify and handle incoming webhooks from RevOps
"""

import hashlib
import hmac
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from revops_sdk.models import EventType, WebhookPayload


# =============================================================================
# CONSTANTS
# =============================================================================

SIGNATURE_HEADER = "x-revops-signature"
TIMESTAMP_HEADER = "x-revops-timestamp"
DELIVERY_ID_HEADER = "x-revops-delivery-id"

DEFAULT_TOLERANCE_SECONDS = 300  # 5 minutes


# =============================================================================
# VERIFICATION
# =============================================================================


@dataclass
class VerificationResult:
    """Result of webhook signature verification"""

    valid: bool
    reason: str | None = None
    payload: WebhookPayload | None = None


def verify_webhook(
    raw_body: str | bytes,
    signature_header: str | None,
    secret: str,
    *,
    tolerance_seconds: int = DEFAULT_TOLERANCE_SECONDS,
) -> VerificationResult:
    """
    Verify webhook signature and parse payload.

    Args:
        raw_body: Raw request body (string or bytes)
        signature_header: Value of X-RevOps-Signature header
        secret: Webhook signing secret
        tolerance_seconds: Maximum age of webhook in seconds (default: 300)

    Returns:
        VerificationResult with valid flag, optional reason, and parsed payload

    Example:
        >>> from revops_sdk import verify_webhook
        >>>
        >>> result = verify_webhook(
        ...     request.body,
        ...     request.headers.get("x-revops-signature"),
        ...     os.environ["REVOPS_WEBHOOK_SECRET"],
        ... )
        >>>
        >>> if not result.valid:
        ...     return Response(status_code=401)
        >>>
        >>> print(f"Event: {result.payload.event_type}")
    """
    # Validate inputs
    if not signature_header:
        return VerificationResult(valid=False, reason="Missing signature header")

    if not secret:
        return VerificationResult(valid=False, reason="Webhook secret not configured")

    # Parse signature header (format: t=<timestamp>,v1=<signature>)
    parsed = _parse_signature_header(signature_header)
    if parsed is None:
        return VerificationResult(valid=False, reason="Invalid signature header format")

    timestamp, signature = parsed

    # Check timestamp tolerance
    now = int(time.time())
    time_diff = abs(now - timestamp)
    if time_diff > tolerance_seconds:
        return VerificationResult(
            valid=False,
            reason=f"Timestamp outside tolerance ({time_diff}s > {tolerance_seconds}s)",
        )

    # Generate expected signature
    body = raw_body if isinstance(raw_body, str) else raw_body.decode("utf-8")
    signature_message = f"{timestamp}.{body}"
    expected_signature = hmac.new(
        secret.encode("utf-8"),
        signature_message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    # Timing-safe comparison
    if not hmac.compare_digest(signature, expected_signature):
        return VerificationResult(valid=False, reason="Signature mismatch")

    # Parse and validate payload
    try:
        data = json.loads(body)
        payload = WebhookPayload.model_validate(data)
        return VerificationResult(valid=True, payload=payload)
    except json.JSONDecodeError:
        return VerificationResult(valid=False, reason="Invalid JSON payload")
    except Exception as e:
        return VerificationResult(valid=False, reason=f"Invalid payload schema: {e}")


def _parse_signature_header(header: str) -> tuple[int, str] | None:
    """Parse signature header into timestamp and signature components."""
    try:
        parts = header.split(",")
        values: dict[str, str] = {}

        for part in parts:
            if "=" in part:
                key, value = part.split("=", 1)
                values[key.strip()] = value.strip()

        if "t" not in values or "v1" not in values:
            return None

        return int(values["t"]), values["v1"]
    except (ValueError, KeyError):
        return None


# =============================================================================
# WEBHOOK ROUTER
# =============================================================================

WebhookHandler = Callable[[WebhookPayload], None | Awaitable[None]]


class WebhookRouter:
    """
    Router for handling different webhook event types.

    Example:
        >>> from revops_sdk import WebhookRouter, verify_webhook
        >>>
        >>> router = WebhookRouter()
        >>>
        >>> @router.on("tenant.created")
        ... async def handle_tenant_created(payload):
        ...     print(f"New tenant: {payload.customer_id}")
        >>>
        >>> @router.on("health.alert_triggered")
        ... async def handle_health_alert(payload):
        ...     await send_slack_alert(payload.data)
        >>>
        >>> # In your webhook endpoint:
        >>> result = verify_webhook(body, signature, secret)
        >>> if result.valid:
        ...     await router.handle(result.payload)
    """

    def __init__(self) -> None:
        self._handlers: dict[str, list[WebhookHandler]] = {}
        self._any_handlers: list[WebhookHandler] = []

    def on(self, event_type: EventType | str) -> Callable[[WebhookHandler], WebhookHandler]:
        """
        Decorator to register a handler for a specific event type.

        Args:
            event_type: Event type to handle

        Returns:
            Decorator function
        """
        event_key = event_type.value if isinstance(event_type, EventType) else event_type

        def decorator(handler: WebhookHandler) -> WebhookHandler:
            if event_key not in self._handlers:
                self._handlers[event_key] = []
            self._handlers[event_key].append(handler)
            return handler

        return decorator

    def on_any(self, handler: WebhookHandler) -> WebhookHandler:
        """
        Register a handler for all event types.

        Args:
            handler: Handler function

        Returns:
            The handler function (unchanged)
        """
        self._any_handlers.append(handler)
        return handler

    async def handle(self, payload: WebhookPayload) -> None:
        """
        Route a webhook payload to registered handlers.

        Args:
            payload: Verified webhook payload
        """
        import asyncio

        # Run specific handlers
        event_key = payload.event_type.value
        specific_handlers = self._handlers.get(event_key, [])

        for handler in specific_handlers:
            result = handler(payload)
            if asyncio.iscoroutine(result):
                await result

        # Run catch-all handlers
        for handler in self._any_handlers:
            result = handler(payload)
            if asyncio.iscoroutine(result):
                await result


# =============================================================================
# FRAMEWORK MIDDLEWARE
# =============================================================================


class WebhookMiddleware:
    """
    Framework-agnostic webhook middleware.

    Example (FastAPI):
        >>> from fastapi import FastAPI, Request, Response
        >>> from revops_sdk import WebhookMiddleware, verify_webhook
        >>>
        >>> app = FastAPI()
        >>> middleware = WebhookMiddleware(secret=os.environ["REVOPS_WEBHOOK_SECRET"])
        >>>
        >>> @app.post("/webhooks/revops")
        ... async def webhook_handler(request: Request):
        ...     body = await request.body()
        ...     result = middleware.verify(
        ...         body,
        ...         request.headers.get("x-revops-signature"),
        ...     )
        ...
        ...     if not result.valid:
        ...         return Response(status_code=401, content=result.reason)
        ...
        ...     # Process result.payload
        ...     return Response(status_code=200)
    """

    def __init__(
        self,
        secret: str,
        *,
        tolerance_seconds: int = DEFAULT_TOLERANCE_SECONDS,
    ) -> None:
        """
        Initialize webhook middleware.

        Args:
            secret: Webhook signing secret
            tolerance_seconds: Maximum age of webhook in seconds
        """
        self.secret = secret
        self.tolerance_seconds = tolerance_seconds

    def verify(
        self,
        raw_body: str | bytes,
        signature_header: str | None,
    ) -> VerificationResult:
        """
        Verify webhook signature.

        Args:
            raw_body: Raw request body
            signature_header: Value of signature header

        Returns:
            VerificationResult
        """
        return verify_webhook(
            raw_body,
            signature_header,
            self.secret,
            tolerance_seconds=self.tolerance_seconds,
        )


__all__ = [
    "SIGNATURE_HEADER",
    "TIMESTAMP_HEADER",
    "DELIVERY_ID_HEADER",
    "DEFAULT_TOLERANCE_SECONDS",
    "VerificationResult",
    "verify_webhook",
    "WebhookHandler",
    "WebhookRouter",
    "WebhookMiddleware",
]
