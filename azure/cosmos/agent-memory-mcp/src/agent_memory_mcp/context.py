"""Per-request scope resolution and shared lifespan state.

This module is the contract every tool module depends on:

* :class:`AppContext` — the lifespan-scoped state (shared async SDK client +
  settings) yielded once per process and reachable from any tool via
  :func:`get_client` / :func:`get_app`.
* :class:`Deps` — build-time dependencies captured in tool closures (settings).
* :func:`resolve_user_id` / :func:`resolve_thread_id` — turn optional tool
  arguments + auth context into concrete partition-key values, enforcing tenant
  isolation when auth is enabled.

Tool modules never read Cosmos config directly; they call these helpers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from azure.cosmos.agent_memory.aio import AsyncCosmosMemoryClient
from azure.cosmos.agent_memory.exceptions import ValidationError
from mcp.server.fastmcp import Context

from .auth import current_subject
from .config import Settings


@dataclass
class AppContext:
    """Lifespan-scoped state shared across every tool call in the process."""

    client: AsyncCosmosMemoryClient
    settings: Settings


@dataclass
class Deps:
    """Build-time dependencies captured by tool closures at registration."""

    settings: Settings


def get_app(ctx: Context) -> AppContext:
    """Return the :class:`AppContext` yielded by the server lifespan."""
    return ctx.request_context.lifespan_context


def get_client(ctx: Context) -> AsyncCosmosMemoryClient:
    """Return the shared async memory client for the active request."""
    return get_app(ctx).client


def resolve_user_id(deps: Deps, user_id_arg: Optional[str] = None) -> str:
    """Resolve the memory owner id, enforcing tenant isolation under auth.

    Precedence:

    * **Auth enabled** (hosted): the id comes from the verified token. A
      client-supplied ``user_id`` that differs is rejected unless
      ``AGENT_MEMORY_ALLOW_USER_ID_ARG=true`` (trusted server-to-server).
    * **Auth disabled** (local/stdio): trust the caller —
      ``user_id_arg`` → ``AGENT_MEMORY_DEFAULT_USER_ID``.

    Raises :class:`ValidationError` when no owner can be determined.
    """
    settings = deps.settings
    token_uid = current_subject(settings) if settings.auth_enabled else None

    if token_uid is not None:
        if user_id_arg and user_id_arg != token_uid:
            if not settings.allow_user_id_arg:
                raise ValidationError(
                    "user_id does not match the authenticated identity; "
                    "cross-tenant access is not permitted."
                )
            return user_id_arg
        return token_uid

    resolved = user_id_arg or settings.default_user_id
    if not resolved:
        raise ValidationError(
            "user_id is required: pass user_id or set AGENT_MEMORY_DEFAULT_USER_ID."
        )
    return resolved


def resolve_thread_id(
    thread_id_arg: Optional[str] = None,
    *,
    required: bool = True,
) -> Optional[str]:
    """Resolve the conversation id from the explicit argument.

    Raises :class:`ValidationError` when ``required`` and none is provided.
    """
    resolved = thread_id_arg
    if required and not resolved:
        raise ValidationError("thread_id is required: pass an explicit thread_id.")
    return resolved
