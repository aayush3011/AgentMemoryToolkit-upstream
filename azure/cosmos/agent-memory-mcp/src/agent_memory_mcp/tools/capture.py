"""Capture (write) tool: ``add_memory``.

A single tool writes any memory record — a conversation turn or a durable fact,
episodic, or procedural memory — through the shared async SDK client, resolving
user and thread scope through the MCP context helpers.
"""

from __future__ import annotations

from typing import Optional

from azure.cosmos.agent_memory.exceptions import ValidationError
from mcp.server.fastmcp import Context, FastMCP

from agent_memory_mcp.context import Deps, get_client, resolve_thread_id, resolve_user_id
from agent_memory_mcp.errors import tool_errors

_VALID_ROLES = frozenset({"user", "agent", "tool", "system"})
_VALID_MEMORY_TYPES = frozenset({"turn", "fact", "episodic", "procedural"})


def register(mcp: FastMCP, deps: Deps) -> None:
    @mcp.tool(
        title="Add a memory",
        description=(
            "Store any memory for the user. Set memory_type='turn' to record a raw "
            "conversation message (requires role and thread_id). Use 'fact', "
            "'episodic', or 'procedural' to assert a durable memory the agent "
            "already knows; these may optionally belong to a thread."
        ),
    )
    @tool_errors
    async def add_memory(
        content: str,
        memory_type: str = "fact",
        role: Optional[str] = None,
        thread_id: Optional[str] = None,
        user_id: Optional[str] = None,
        metadata: Optional[dict] = None,
        tags: Optional[list[str]] = None,
        salience: Optional[float] = None,
        ctx: Context = None,  # type: ignore[assignment]
    ) -> dict:
        """Store a turn or a durable fact/episodic/procedural memory."""
        if memory_type not in _VALID_MEMORY_TYPES:
            raise ValidationError(
                "memory_type must be one of: turn, fact, episodic, procedural."
            )

        is_turn = memory_type == "turn"
        if is_turn and role is None:
            raise ValidationError("role is required when memory_type is 'turn'.")
        role = role or "user"
        if role not in _VALID_ROLES:
            raise ValidationError("role must be one of: user, agent, tool, system.")

        user_id = resolve_user_id(deps, user_id)
        thread_id = resolve_thread_id(thread_id, required=is_turn)
        client = get_client(ctx)
        memory_id = await client.add_cosmos(
            user_id=user_id,
            role=role,
            content=content,
            memory_type=memory_type,
            thread_id=thread_id,
            metadata=metadata,
            tags=tags,
            salience=salience,
        )
        return {
            "id": memory_id,
            "memory_type": memory_type,
            "thread_id": thread_id,
            "stored": True,
        }
