"""Retrieval (read) tools for durable memories and raw conversation turns."""

from __future__ import annotations

from typing import Optional

from azure.cosmos.agent_memory.exceptions import ValidationError
from mcp.server.fastmcp import Context, FastMCP

from agent_memory_mcp.context import Deps, get_client, resolve_thread_id, resolve_user_id
from agent_memory_mcp.errors import tool_errors
from agent_memory_mcp.serialization import list_payload

_VALID_MEMORY_TYPES = frozenset({"fact", "episodic", "procedural"})


def _clamp_required_k(value: Optional[int], max_top_k: int, default: int) -> int:
    """Clamp a required top-k style argument, tolerating JSON null."""
    return min(default if value is None else value, max_top_k)


def _clamp_optional_k(value: Optional[int], max_top_k: int) -> Optional[int]:
    """Clamp an optional recent-k style argument when provided."""
    if value is None:
        return None
    return min(value, max_top_k)


def _validate_memory_types(memory_types: Optional[list[str]]) -> None:
    if memory_types is None:
        return
    invalid = sorted(set(memory_types) - _VALID_MEMORY_TYPES)
    if invalid:
        raise ValidationError("memory_types must be a subset of: fact, episodic, procedural.")


def register(mcp: FastMCP, deps: Deps) -> None:
    @mcp.tool(
        title="Search memories",
        description=(
            "Primary recall: semantic/hybrid search over the user's durable memory "
            "(facts, past experiences, and behavioral rules). Optionally blend raw "
            "conversation turns when include_turns is enabled."
        ),
    )
    @tool_errors
    async def search_memories(
        query: str,
        memory_types: Optional[list[str]] = None,
        top_k: int = 5,
        thread_id: Optional[str] = None,
        user_id: Optional[str] = None,
        min_confidence: Optional[float] = None,
        tags_any: Optional[list[str]] = None,
        tags_all: Optional[list[str]] = None,
        exclude_tags: Optional[list[str]] = None,
        include_turns: bool = False,
        ctx: Context = None,  # type: ignore[assignment]
    ) -> dict:
        """Search durable memories and optionally turns for a user."""
        _validate_memory_types(memory_types)
        user_id = resolve_user_id(deps, user_id)
        thread_id = resolve_thread_id(thread_id, required=False)
        top_k = _clamp_required_k(top_k, deps.settings.max_top_k, 5)
        client = get_client(ctx)
        results = await client.search_cosmos(
            search_terms=query,
            user_id=user_id,
            memory_types=memory_types,
            thread_id=thread_id,
            top_k=top_k,
            min_confidence=min_confidence,
            tags_any=tags_any,
            tags_all=tags_all,
            exclude_tags=exclude_tags,
            include_turns=include_turns,
        )
        return list_payload(results)

    @mcp.tool(
        title="Get memories",
        description=(
            "Deterministically fetch durable memories without a search query. Use "
            "filters such as memory type, tags, confidence, salience, thread, or "
            "recent_k when exact retrieval is preferred over semantic recall."
        ),
    )
    @tool_errors
    async def get_memories(
        memory_types: Optional[list[str]] = None,
        recent_k: Optional[int] = None,
        thread_id: Optional[str] = None,
        user_id: Optional[str] = None,
        min_confidence: Optional[float] = None,
        min_salience: Optional[float] = None,
        tags_any: Optional[list[str]] = None,
        tags_all: Optional[list[str]] = None,
        exclude_tags: Optional[list[str]] = None,
        ctx: Context = None,  # type: ignore[assignment]
    ) -> dict:
        """Fetch durable memories by deterministic filters."""
        _validate_memory_types(memory_types)
        user_id = resolve_user_id(deps, user_id)
        thread_id = resolve_thread_id(thread_id, required=False)
        recent_k = _clamp_optional_k(recent_k, deps.settings.max_top_k)
        client = get_client(ctx)
        results = await client.get_memories(
            user_id=user_id,
            thread_id=thread_id,
            memory_types=memory_types,
            recent_k=recent_k,
            min_confidence=min_confidence,
            min_salience=min_salience,
            tags_any=tags_any,
            tags_all=tags_all,
            exclude_tags=exclude_tags,
        )
        return list_payload(results)

    @mcp.tool(
        title="Recall thread",
        description=(
            "Return raw conversation turns for a thread in oldest-first order. Use "
            "this to rehydrate local context from the literal conversation history."
        ),
    )
    @tool_errors
    async def recall_thread(
        thread_id: Optional[str] = None,
        user_id: Optional[str] = None,
        recent_k: Optional[int] = None,
        ctx: Context = None,  # type: ignore[assignment]
    ) -> dict:
        """Fetch raw turns for one thread."""
        user_id = resolve_user_id(deps, user_id)
        thread_id = resolve_thread_id(thread_id, required=True)
        recent_k = _clamp_optional_k(recent_k, deps.settings.max_top_k)
        client = get_client(ctx)
        results = await client.get_thread(thread_id=thread_id, user_id=user_id, recent_k=recent_k)
        return list_payload(results)

    @mcp.tool(
        title="Search turns",
        description=(
            "Semantic search over the literal raw conversation log, not extracted "
            "facts. Requires turn embeddings to be enabled (ENABLE_TURN_EMBEDDINGS)."
        ),
    )
    @tool_errors
    async def search_turns(
        query: str,
        thread_id: Optional[str] = None,
        user_id: Optional[str] = None,
        top_k: int = 5,
        ctx: Context = None,  # type: ignore[assignment]
    ) -> dict:
        """Search raw conversation turns."""
        user_id = resolve_user_id(deps, user_id)
        thread_id = resolve_thread_id(thread_id, required=False)
        top_k = _clamp_required_k(top_k, deps.settings.max_top_k, 5)
        client = get_client(ctx)
        results = await client.search_turns(
            search_terms=query,
            user_id=user_id,
            thread_id=thread_id,
            top_k=top_k,
        )
        return list_payload(results)
