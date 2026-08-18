"""Entra ID (Azure AD) bearer-token verification for hosted / HTTP mode.

In hosted Streamable-HTTP mode the server acts as an OAuth 2.0 *resource server*:
it validates the incoming JWT access token (signature via the tenant JWKS,
issuer, audience, expiry) and exposes the verified claims so the tool layer can
derive the memory owner (``user_id``) from the token instead of trusting a
client-supplied value. See ``context.resolve_user_id``.

Auth is off by default (local / stdio). Enable it with ``AGENT_MEMORY_AUTH_ENABLED=true``
plus ``ENTRA_TENANT_ID`` and ``ENTRA_AUDIENCE``. PyJWT (the ``auth`` extra) is
imported lazily so the base install does not require it.
"""

from __future__ import annotations

import asyncio
from typing import Optional

from azure.cosmos.agent_memory.exceptions import ConfigurationError
from mcp.server.auth.provider import AccessToken, TokenVerifier
from mcp.server.auth.settings import AuthSettings

from .config import Settings


def _entra_issuers(tenant_id: str) -> set[str]:
    """Valid issuer strings Entra ID emits for v1 and v2 tokens."""
    return {
        f"https://login.microsoftonline.com/{tenant_id}/v2.0",
        f"https://sts.windows.net/{tenant_id}/",
    }


def _extract_scopes(claims: dict) -> list[str]:
    """Collect delegated scopes (``scp``) and app roles (``roles``)."""
    scopes: list[str] = []
    scp = claims.get("scp")
    if isinstance(scp, str):
        scopes.extend(scp.split())
    elif isinstance(scp, list):
        scopes.extend(str(s) for s in scp)
    roles = claims.get("roles")
    if isinstance(roles, list):
        scopes.extend(str(r) for r in roles)
    return scopes


class EntraTokenVerifier(TokenVerifier):
    """Validate Entra ID JWT access tokens against the tenant JWKS."""

    def __init__(self, settings: Settings) -> None:
        if not settings.entra_tenant_id or not settings.entra_audience:
            raise ConfigurationError(
                "Auth is enabled but ENTRA_TENANT_ID / ENTRA_AUDIENCE are not set."
            )
        self._settings = settings
        self._tenant = settings.entra_tenant_id
        self._audience = settings.entra_audience
        self._issuers = _entra_issuers(self._tenant)
        self._jwks_url = f"https://login.microsoftonline.com/{self._tenant}/discovery/v2.0/keys"
        self._jwk_client = None  # lazily built (needs the optional PyJWT dep)

    def _client(self):
        if self._jwk_client is None:
            try:
                from jwt import PyJWKClient
            except ImportError as exc:  # pragma: no cover - depends on install extras
                raise ConfigurationError(
                    "Token verification requires PyJWT. Install the 'auth' extra: "
                    "pip install 'azure-cosmos-agent-memory-mcp[auth]'."
                ) from exc
            self._jwk_client = PyJWKClient(self._jwks_url)
        return self._jwk_client

    async def verify_token(self, token: str) -> Optional[AccessToken]:
        """Return the verified :class:`AccessToken`, or ``None`` if invalid."""
        try:
            return await asyncio.to_thread(self._verify_sync, token)
        except Exception:  # noqa: BLE001 - any failure => unauthenticated
            return None

    def _verify_sync(self, token: str) -> Optional[AccessToken]:
        import jwt

        signing_key = self._client().get_signing_key_from_jwt(token).key
        claims = jwt.decode(
            token,
            signing_key,
            algorithms=["RS256"],
            audience=self._audience,
            options={"require": ["exp", "iss"]},
        )
        if claims.get("iss") not in self._issuers:
            return None
        subject = (
            claims.get(self._settings.user_id_claim)
            or claims.get("oid")
            or claims.get("sub")
        )
        return AccessToken(
            token=token,
            client_id=str(claims.get("azp") or claims.get("appid") or claims.get("aud") or ""),
            scopes=_extract_scopes(claims),
            expires_at=claims.get("exp"),
            subject=str(subject) if subject is not None else None,
            claims=claims,
        )


def current_access_token() -> Optional[AccessToken]:
    """Return the verified access token bound to the active request, if any."""
    try:
        from mcp.server.auth.middleware.auth_context import get_access_token

        return get_access_token()
    except Exception:  # noqa: BLE001 - no auth context (stdio / auth disabled)
        return None


def current_subject(settings: Settings) -> Optional[str]:
    """Return the memory owner id derived from the active token, or ``None``."""
    token = current_access_token()
    if token is None:
        return None
    claims = token.claims or {}
    return claims.get(settings.user_id_claim) or token.subject


def build_auth(settings: Settings) -> tuple[Optional[TokenVerifier], Optional[AuthSettings]]:
    """Build the ``(token_verifier, auth_settings)`` pair for FastMCP.

    Returns ``(None, None)`` when auth is disabled so the server runs unauthenticated
    (local / stdio).
    """
    if not settings.auth_enabled:
        return None, None
    verifier = EntraTokenVerifier(settings)
    issuer = f"https://login.microsoftonline.com/{settings.entra_tenant_id}/v2.0"
    resource_url = settings.resource_server_url or issuer
    auth_settings = AuthSettings(
        issuer_url=issuer,
        resource_server_url=resource_url,
        required_scopes=settings.required_scope_list() or None,
    )
    return verifier, auth_settings
