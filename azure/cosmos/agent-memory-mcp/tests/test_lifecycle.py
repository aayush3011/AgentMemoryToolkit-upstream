from __future__ import annotations

import pytest
from conftest import catch_tools
from mcp.server.fastmcp.exceptions import ToolError

from agent_memory_mcp.tools import lifecycle


async def test_update_memory_ok_resolves_user_and_allows_none_thread_for_non_turn(deps, mock_client, make_ctx):
    tools = catch_tools(lifecycle.register, deps)
    ctx = make_ctx(mock_client)
    mock_client.update_cosmos.return_value = {
        "id": "m1",
        "type": "fact",
        "content": "updated",
        "embedding": [0.1, 0.2],
    }

    out = await tools["update_memory"](
        memory_id="m1",
        memory_type="fact",
        content="updated",
        metadata={"source": "test"},
        ctx=ctx,
    )

    assert out == {"updated": True, "memory": {"id": "m1", "memory_type": "fact", "content": "updated"}}
    mock_client.update_cosmos.assert_awaited_once()
    args = mock_client.update_cosmos.await_args
    assert args.args == ("m1",)
    assert args.kwargs["user_id"] == "user-test"
    assert args.kwargs["thread_id"] is None
    assert args.kwargs["memory_type"] == "fact"
    assert args.kwargs["content"] == "updated"
    assert args.kwargs["role"] is None
    assert args.kwargs["metadata"] == {"source": "test"}


async def test_update_memory_turn_missing_thread_id_raises_tool_error(deps, mock_client, make_ctx):
    tools = catch_tools(lifecycle.register, deps)
    ctx = make_ctx(mock_client)

    with pytest.raises(ToolError) as exc_info:
        await tools["update_memory"](memory_id="turn1", memory_type="turn", content="fixed", ctx=ctx)

    assert "invalid_params" in str(exc_info.value)
    assert "thread_id" in str(exc_info.value)
    mock_client.update_cosmos.assert_not_awaited()


async def test_delete_memory_ok_returns_deleted_and_calls_delete(deps, mock_client, make_ctx):
    tools = catch_tools(lifecycle.register, deps)
    ctx = make_ctx(mock_client)

    out = await tools["delete_memory"](memory_id="m2", memory_type="fact", ctx=ctx)

    assert out == {"deleted": True, "id": "m2"}
    mock_client.delete_cosmos.assert_awaited_once_with(
        "m2",
        user_id="user-test",
        thread_id=None,
        memory_type="fact",
    )


async def test_get_memory_history_returns_list_payload_and_strips_embeddings(deps, mock_client, make_ctx):
    tools = catch_tools(lifecycle.register, deps)
    ctx = make_ctx(mock_client)
    mock_client.get_memory_history.return_value = [
        {"id": "m3", "type": "fact", "content": "new", "embedding": [0.1, 0.2], "_etag": "etag"}
    ]

    out = await tools["get_memory_history"](memory_id="m3", max_depth=5, ctx=ctx)

    assert out == {
        "id": "m3",
        "items": [{"id": "m3", "memory_type": "fact", "content": "new"}],
        "count": 1,
        "truncated": False,
    }
    mock_client.get_memory_history.assert_awaited_once_with(
        "m3",
        "user-test",
        thread_id=None,
        max_depth=5,
    )
