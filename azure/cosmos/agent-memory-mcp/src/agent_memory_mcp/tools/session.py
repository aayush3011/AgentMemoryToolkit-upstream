"""Session-introspection tool: ``whoami``.

This module is the reference implementation every other tool module follows:

1. ``register(mcp, deps)`` declares tools with ``@mcp.tool()``.
2. Each tool takes a ``ctx: Context`` parameter (auto-injected; excluded from the
   input schema) plus JSON-friendly arguments with clear descriptions.
3. The body is wrapped with ``@tool_errors`` so SDK exceptions become clean
   ``ToolError``s while the FastMCP-visible signature is preserved.
4. Owner ids are resolved via ``context.resolve_user_id`` — never read from
   settings directly.
"""

from __future__ import annotations

from mcp.server.fastmcp import Context, FastMCP

from agent_memory_mcp.context import Deps, resolve_user_id
from agent_memory_mcp.errors import tool_errors


def register(mcp: FastMCP, deps: Deps) -> None:
    @mcp.tool(
        title="Who am I",
        description=(
            "Return the currently resolved memory owner: the effective user_id and "
            "whether authentication is enabled. Useful to confirm identity before "
            "reading or writing memory."
        ),
    )
    @tool_errors
    async def whoami(ctx: Context = None) -> dict:  # type: ignore[assignment]
        """Report the effective ``user_id`` and whether auth is enabled."""
        return {
            "user_id": resolve_user_id(deps, None),
            "auth_enabled": deps.settings.auth_enabled,
        }
