"""Profile / durable-context (read) tools."""

from __future__ import annotations

from typing import Optional

from mcp.server.fastmcp import Context, FastMCP

from agent_memory_mcp.context import Deps, get_client, resolve_thread_id, resolve_user_id
from agent_memory_mcp.errors import tool_errors
from agent_memory_mcp.serialization import project_record


def _clamp_optional_k(value: Optional[int], max_top_k: int) -> Optional[int]:
    """Clamp an optional recent-k style argument when provided."""
    if value is None:
        return None
    return min(value, max_top_k)


def register(mcp: FastMCP, deps: Deps) -> None:
    @mcp.tool(
        title="Get user summary",
        description=(
            "Return the durable narrative profile and preferences for the user. "
            "Call this near session start to personalize responses with stable, "
            "cross-thread context."
        ),
    )
    @tool_errors
    async def get_user_summary(
        user_id: Optional[str] = None,
        ctx: Context = None,  # type: ignore[assignment]
    ) -> dict:
        """Fetch the user's durable narrative profile."""
        user_id = resolve_user_id(deps, user_id)
        client = get_client(ctx)
        summary = await client.get_user_summary(user_id)
        return {
            "user_id": user_id,
            "summary": project_record(summary) if summary else None,
            "exists": summary is not None,
        }

    @mcp.tool(
        title="Get thread summary",
        description=(
            "Return a concise summary of one conversation thread. Use this to "
            "rehydrate the current conversation without fetching every raw turn."
        ),
    )
    @tool_errors
    async def get_thread_summary(
        thread_id: Optional[str] = None,
        user_id: Optional[str] = None,
        recent_k: Optional[int] = None,
        ctx: Context = None,  # type: ignore[assignment]
    ) -> dict:
        """Fetch the summary for one conversation thread."""
        user_id = resolve_user_id(deps, user_id)
        thread_id = resolve_thread_id(thread_id, required=True)
        recent_k = _clamp_optional_k(recent_k, deps.settings.max_top_k)
        client = get_client(ctx)
        summary = await client.get_thread_summary(user_id=user_id, thread_id=thread_id, recent_k=recent_k)
        return {
            "thread_id": thread_id,
            "summary": project_record(summary) if summary else None,
            "exists": summary is not None,
        }
