"""Lifecycle / correction tools for updating, deleting, and history."""

from __future__ import annotations

from mcp.server.fastmcp import Context, FastMCP

from agent_memory_mcp.context import Deps, get_client, resolve_thread_id, resolve_user_id
from agent_memory_mcp.errors import tool_errors
from agent_memory_mcp.serialization import list_payload, project_record


def register(mcp: FastMCP, deps: Deps) -> None:
    @mcp.tool(
        title="Update memory",
        description=(
            "Correct or revise an existing memory by creating a new version. Use "
            "memory_type to locate the record; turn memories also require a thread_id."
        ),
    )
    @tool_errors
    async def update_memory(
        memory_id: str,
        memory_type: str,
        content: str | None = None,
        role: str | None = None,
        metadata: dict | None = None,
        thread_id: str | None = None,
        user_id: str | None = None,
        ctx: Context = None,  # type: ignore[assignment]
    ) -> dict:
        """Correct or revise an existing memory, creating a new version."""
        user_id = resolve_user_id(deps, user_id)
        thread_id = resolve_thread_id(thread_id, required=(memory_type == "turn"))
        client = get_client(ctx)
        updated = await client.update_cosmos(
            memory_id,
            user_id=user_id,
            thread_id=thread_id,
            memory_type=memory_type,
            content=content,
            role=role,
            metadata=metadata,
        )
        return {"updated": True, "memory": project_record(updated)}

    @mcp.tool(
        title="Delete memory",
        description=(
            "Permanently delete a memory. Provide memory_type to locate the record; "
            "turn memories also require a thread_id."
        ),
    )
    @tool_errors
    async def delete_memory(
        memory_id: str,
        memory_type: str,
        thread_id: str | None = None,
        user_id: str | None = None,
        ctx: Context = None,  # type: ignore[assignment]
    ) -> dict:
        """Permanently delete a memory."""
        user_id = resolve_user_id(deps, user_id)
        thread_id = resolve_thread_id(thread_id, required=(memory_type == "turn"))
        client = get_client(ctx)
        await client.delete_cosmos(
            memory_id,
            user_id=user_id,
            thread_id=thread_id,
            memory_type=memory_type,
        )
        return {"deleted": True, "id": memory_id}

    @mcp.tool(
        title="Get memory history",
        description=(
            "Show the version lineage for a memory, newest to oldest, so the agent "
            "can inspect what changed across corrections."
        ),
    )
    @tool_errors
    async def get_memory_history(
        memory_id: str,
        thread_id: str | None = None,
        user_id: str | None = None,
        max_depth: int = 20,
        ctx: Context = None,  # type: ignore[assignment]
    ) -> dict:
        """Show a memory's version lineage, newest to oldest."""
        user_id = resolve_user_id(deps, user_id)
        thread_id = resolve_thread_id(thread_id, required=False)
        client = get_client(ctx)
        history = await client.get_memory_history(
            memory_id,
            user_id,
            thread_id=thread_id,
            max_depth=max_depth,
        )
        return {"id": memory_id, **list_payload(history)}
