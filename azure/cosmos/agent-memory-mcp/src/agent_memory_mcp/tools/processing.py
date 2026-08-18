"""Consolidation / processing tools for Agent Memory.

The default surface exposes one explicit flush hook, ``process_thread``. More
granular pipeline steps are registered only when ``AGENT_MEMORY_EXPOSE_GRANULAR``
is enabled.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Optional

from mcp.server.fastmcp import Context, FastMCP

from agent_memory_mcp.context import Deps, get_client, resolve_thread_id, resolve_user_id
from agent_memory_mcp.errors import tool_errors
from agent_memory_mcp.serialization import project_record


def _serialize_process_result(result: Any) -> dict[str, Any]:
    """Serialize SDK process results while stripping vector-heavy nested docs."""
    data = dataclasses.asdict(result) if dataclasses.is_dataclass(result) else dict(result)
    for key in ("thread_summary", "procedural", "user_summary"):
        if data.get(key) is not None:
            data[key] = project_record(data[key])
    return data


def register(mcp: FastMCP, deps: Deps) -> None:
    @mcp.tool(
        title="Process thread now",
        description=(
            "Force immediate consolidation for a conversation: summary, memory "
            "extraction, reconciliation, procedural updates, and user-profile "
            "refresh. This normally happens automatically in the background; use "
            "this to flush a thread before reading results."
        ),
    )
    @tool_errors
    async def process_thread(
        thread_id: Optional[str] = None,
        user_id: Optional[str] = None,
        ctx: Context = None,  # type: ignore[assignment]
    ) -> dict:
        """Run the full processing pipeline for one thread."""
        user_id = resolve_user_id(deps, user_id)
        thread_id = resolve_thread_id(thread_id, required=True)
        client = get_client(ctx)
        result = await client.process_now(user_id=user_id, thread_id=thread_id)
        return _serialize_process_result(result)

    if not deps.settings.expose_granular:
        return

    @mcp.tool(
        title="Summarize thread",
        description=(
            "Regenerate the compact summary for a conversation thread. Advanced "
            "pipeline step; most agents should call process_thread instead."
        ),
    )
    @tool_errors
    async def summarize_thread(
        thread_id: Optional[str] = None,
        user_id: Optional[str] = None,
        recent_k: Optional[int] = None,
        ctx: Context = None,  # type: ignore[assignment]
    ) -> dict:
        """Generate a thread summary."""
        user_id = resolve_user_id(deps, user_id)
        thread_id = resolve_thread_id(thread_id, required=True)
        client = get_client(ctx)
        summary = await client.generate_thread_summary(user_id, thread_id, recent_k=recent_k)
        return {"summary": project_record(summary) if summary else None}

    @mcp.tool(
        title="Extract memories",
        description=(
            "Extract durable fact, episodic, and procedural memories from a "
            "conversation thread. Advanced pipeline step; most agents should call "
            "process_thread instead."
        ),
    )
    @tool_errors
    async def extract_memories(
        thread_id: Optional[str] = None,
        user_id: Optional[str] = None,
        recent_k: Optional[int] = None,
        ctx: Context = None,  # type: ignore[assignment]
    ) -> dict:
        """Extract durable memories from a thread."""
        user_id = resolve_user_id(deps, user_id)
        thread_id = resolve_thread_id(thread_id, required=True)
        client = get_client(ctx)
        counts = await client.extract_memories(user_id, thread_id, recent_k=recent_k)
        return {"extracted": counts}

    @mcp.tool(
        title="Update user profile",
        description=(
            "Regenerate the user's profile summary from recent or selected threads. "
            "Advanced pipeline step; most agents should call process_thread instead."
        ),
    )
    @tool_errors
    async def update_user_profile(
        thread_ids: Optional[list[str]] = None,
        recent_k: Optional[int] = None,
        user_id: Optional[str] = None,
        ctx: Context = None,  # type: ignore[assignment]
    ) -> dict:
        """Generate the user's profile summary."""
        user_id = resolve_user_id(deps, user_id)
        client = get_client(ctx)
        summary = await client.generate_user_summary(user_id, thread_ids=thread_ids, recent_k=recent_k)
        return {"summary": project_record(summary) if summary else None}

    @mcp.tool(
        title="Reconcile memories",
        description=(
            "Deduplicate and supersede recent durable memories for a user. Advanced "
            "pipeline step; most agents should call process_thread instead."
        ),
    )
    @tool_errors
    async def reconcile_memories(
        n: Optional[int] = None,
        user_id: Optional[str] = None,
        ctx: Context = None,  # type: ignore[assignment]
    ) -> dict:
        """Reconcile recent memories for a user."""
        user_id = resolve_user_id(deps, user_id)
        client = get_client(ctx)
        result = await client.reconcile(user_id, n=n)
        return {"reconciled": result}
