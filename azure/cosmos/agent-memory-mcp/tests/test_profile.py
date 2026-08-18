from __future__ import annotations

import pytest
from conftest import catch_tools
from mcp.server.fastmcp.exceptions import ToolError

from agent_memory_mcp.tools import profile


async def test_get_user_summary_exists_strips_embedding(deps, mock_client, make_ctx):
    tools = catch_tools(profile.register, deps)
    ctx = make_ctx(mock_client)
    mock_client.get_user_summary.return_value = {"id": "u", "content": "profile", "embedding": [0.1]}

    out = await tools["get_user_summary"](ctx=ctx)

    assert out == {"user_id": "user-test", "summary": {"id": "u", "content": "profile"}, "exists": True}
    mock_client.get_user_summary.assert_awaited_once_with("user-test")


async def test_get_user_summary_missing(deps, mock_client, make_ctx):
    tools = catch_tools(profile.register, deps)
    ctx = make_ctx(mock_client)
    mock_client.get_user_summary.return_value = None

    out = await tools["get_user_summary"](ctx=ctx)

    assert out == {"user_id": "user-test", "summary": None, "exists": False}
    mock_client.get_user_summary.assert_awaited_once_with("user-test")


async def test_get_thread_summary_missing_thread_id_raises_tool_error(deps, mock_client, make_ctx):
    tools = catch_tools(profile.register, deps)
    ctx = make_ctx(mock_client)

    with pytest.raises(ToolError) as exc_info:
        await tools["get_thread_summary"](ctx=ctx)

    assert "invalid_params" in str(exc_info.value)
    assert "thread_id" in str(exc_info.value)
    mock_client.get_thread_summary.assert_not_awaited()


async def test_get_thread_summary_ok_strips_embedding_and_clamps_recent_k(deps, mock_client, make_ctx):
    tools = catch_tools(profile.register, deps)
    ctx = make_ctx(mock_client)
    mock_client.get_thread_summary.return_value = {
        "id": "s1",
        "content": "summary",
        "embedding": [0.1, 0.2],
    }

    out = await tools["get_thread_summary"](thread_id="t1", recent_k=999, ctx=ctx)

    assert out == {"thread_id": "t1", "summary": {"id": "s1", "content": "summary"}, "exists": True}
    mock_client.get_thread_summary.assert_awaited_once_with(
        user_id="user-test", thread_id="t1", recent_k=deps.settings.max_top_k
    )
