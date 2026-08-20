"""Gateway auth: validate the caller's APIM subscription key.

Mirrors the InferencePlatform App Service ApiKeyAuthScheme. The subscription key
(header Ocp-Apim-Subscription-Key) plus the requested account (header
x-requested-account) are POSTed to the shared APIM validate-key operation, which
returns 204 (valid), 403 (key not authorized for that account), or 401.

The dependency is env-gated: when APIM_VALIDATE_KEY_URL is unset (local/dev) it
is a no-op, so the service runs without the gateway. When set (behind Front Door
+ APIM) every functional route requires a valid key.
"""

from __future__ import annotations

import os

import httpx
from fastapi import Header

from app.core.errors import GatewayAuthError

APIM_VALIDATE_KEY_URL_ENV = "APIM_VALIDATE_KEY_URL"

SUBSCRIPTION_KEY_HEADER = "Ocp-Apim-Subscription-Key"
ACCOUNT_HEADER = "x-requested-account"


async def require_apim_key(
    ocp_apim_subscription_key: str | None = Header(default=None, alias=SUBSCRIPTION_KEY_HEADER),
    x_requested_account: str | None = Header(default=None, alias=ACCOUNT_HEADER),
) -> None:
    validate_url = os.getenv(APIM_VALIDATE_KEY_URL_ENV)
    if not validate_url:
        return

    if not ocp_apim_subscription_key or not x_requested_account:
        raise GatewayAuthError(401, "Unauthorized", "Missing subscription key or account header.")

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.post(
                validate_url,
                headers={
                    SUBSCRIPTION_KEY_HEADER: ocp_apim_subscription_key,
                    ACCOUNT_HEADER: x_requested_account,
                },
            )
    except httpx.HTTPError as exc:
        raise GatewayAuthError(
            503, "Key Validation Unavailable", "Could not reach the key validation service."
        ) from exc

    if response.status_code == 204:
        return
    if response.status_code == 403:
        raise GatewayAuthError(403, "Forbidden", "Key is not authorized for the requested account.")
    raise GatewayAuthError(401, "Unauthorized", "Invalid or missing subscription key.")
