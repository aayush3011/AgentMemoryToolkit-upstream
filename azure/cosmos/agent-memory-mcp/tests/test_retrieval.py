from __future__ import annotations

import pytest
from conftest import catch_tools
from mcp.server.fastmcp.exceptions import ToolError

from agent_memory_mcp.tools import retrieval


async def test_search_memories_ok_resolves_scope_and_clamps_top_k(deps, mock_client, make_ctx):
    tools = catch_tools(retrieval.register, deps)
    ctx = make_ctx(mock_client)
    mock_client.search_cosmos.return_value = [{"id": "m1", "type": "fact"}]

    out = await tools["search_memories"](
        query="preferences",
        memory_types=["fact"],
        top_k=999,
        thread_id="t1",
        min_confidence=0.7,
        tags_any=["preference"],
        tags_all=["stable"],
        exclude_tags=["stale"],
        include_turns=True,
        ctx=ctx,
    )

    assert out == {"items": [{"id": "m1", "memory_type": "fact"}], "count": 1, "truncated": False}
    mock_client.search_cosmos.assert_awaited_once()
    kwargs = mock_client.search_cosmos.await_args.kwargs
    assert kwargs["search_terms"] == "preferences"
    assert kwargs["user_id"] == "user-test"
    assert kwargs["thread_id"] == "t1"
    assert kwargs["memory_types"] == ["fact"]
    assert kwargs["top_k"] == deps.settings.max_top_k
    assert kwargs["min_confidence"] == 0.7
    assert kwargs["tags_any"] == ["preference"]
    assert kwargs["tags_all"] == ["stable"]
    assert kwargs["exclude_tags"] == ["stale"]
    assert kwargs["include_turns"] is True


async def test_get_memories_ok_resolves_scope_and_clamps_recent_k(deps, mock_client, make_ctx):
    tools = catch_tools(retrieval.register, deps)
    ctx = make_ctx(mock_client)
    mock_client.get_memories.return_value = [{"id": "m2", "type": "episodic"}]

    out = await tools["get_memories"](
        memory_types=["episodic"],
        recent_k=999,
        thread_id="t2",
        min_confidence=0.6,
        min_salience=0.8,
        tags_any=["incident"],
        tags_all=["resolved"],
        exclude_tags=["draft"],
        ctx=ctx,
    )

    assert out["items"] == [{"id": "m2", "memory_type": "episodic"}]
    mock_client.get_memories.assert_awaited_once()
    kwargs = mock_client.get_memories.await_args.kwargs
    assert kwargs["user_id"] == "user-test"
    assert kwargs["thread_id"] == "t2"
    assert kwargs["memory_types"] == ["episodic"]
    assert kwargs["recent_k"] == deps.settings.max_top_k
    assert kwargs["min_confidence"] == 0.6
    assert kwargs["min_salience"] == 0.8
    assert kwargs["tags_any"] == ["incident"]
    assert kwargs["tags_all"] == ["resolved"]
    assert kwargs["exclude_tags"] == ["draft"]


async def test_recall_thread_ok_resolves_scope_and_clamps_recent_k(deps, mock_client, make_ctx):
    tools = catch_tools(retrieval.register, deps)
    ctx = make_ctx(mock_client)
    mock_client.get_thread.return_value = [{"id": "turn1", "role": "user", "content": "hi"}]

    out = await tools["recall_thread"](thread_id="t3", recent_k=999, ctx=ctx)

    assert out == {
        "items": [{"id": "turn1", "role": "user", "content": "hi"}],
        "count": 1,
        "truncated": False,
    }
    mock_client.get_thread.assert_awaited_once_with(
        thread_id="t3",
        user_id="user-test",
        recent_k=deps.settings.max_top_k,
    )


async def test_search_turns_ok_resolves_scope_and_clamps_top_k(deps, mock_client, make_ctx):
    tools = catch_tools(retrieval.register, deps)
    ctx = make_ctx(mock_client)
    mock_client.search_turns.return_value = [{"id": "turn2", "content": "raw turn"}]

    out = await tools["search_turns"](query="raw", thread_id="t4", top_k=999, ctx=ctx)

    assert out["items"] == [{"id": "turn2", "content": "raw turn"}]
    mock_client.search_turns.assert_awaited_once_with(
        search_terms="raw",
        user_id="user-test",
        thread_id="t4",
        top_k=deps.settings.max_top_k,
    )


async def test_search_memories_top_k_is_clamped_when_huge(deps, mock_client, make_ctx):
    tools = catch_tools(retrieval.register, deps)
    ctx = make_ctx(mock_client)
    mock_client.search_cosmos.return_value = []

    await tools["search_memories"]("anything", top_k=10_000, ctx=ctx)

    assert mock_client.search_cosmos.await_args.kwargs["top_k"] == deps.settings.max_top_k


async def test_search_turns_top_k_is_clamped_when_huge(deps, mock_client, make_ctx):
    tools = catch_tools(retrieval.register, deps)
    ctx = make_ctx(mock_client)
    mock_client.search_turns.return_value = []

    await tools["search_turns"]("anything", top_k=10_000, ctx=ctx)

    assert mock_client.search_turns.await_args.kwargs["top_k"] == deps.settings.max_top_k


async def test_recall_thread_missing_thread_id_raises_tool_error(deps, mock_client, make_ctx):
    tools = catch_tools(retrieval.register, deps)
    ctx = make_ctx(mock_client)

    with pytest.raises(ToolError) as exc_info:
        await tools["recall_thread"](ctx=ctx)

    assert "invalid_params" in str(exc_info.value)
    assert "thread_id" in str(exc_info.value)
    mock_client.get_thread.assert_not_awaited()


async def test_search_memories_serialization_strips_embeddings_and_cosmos_fields(deps, mock_client, make_ctx):
    tools = catch_tools(retrieval.register, deps)
    ctx = make_ctx(mock_client)
    mock_client.search_cosmos.return_value = [
        {"id": "m1", "content": "x", "embedding": [0.1, 0.2], "_rid": "r", "type": "fact"}
    ]

    out = await tools["search_memories"]("x", ctx=ctx)

    item = out["items"][0]
    assert "embedding" not in item
    assert "_rid" not in item
    assert item["memory_type"] == "fact"
    assert item["id"] == "m1"
    assert item["content"] == "x"
