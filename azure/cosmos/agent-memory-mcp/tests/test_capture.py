from __future__ import annotations

import pytest
from conftest import catch_tools
from mcp.server.fastmcp.exceptions import ToolError

from agent_memory_mcp.tools import capture


async def test_add_memory_turn_ok(deps, mock_client, make_ctx):
    tools = catch_tools(capture.register, deps)
    ctx = make_ctx(mock_client)

    out = await tools["add_memory"](
        content="hi",
        memory_type="turn",
        role="user",
        thread_id="t1",
        metadata={"source": "chat"},
        tags=["greeting"],
        ctx=ctx,
    )

    assert out == {"id": "mem-123", "memory_type": "turn", "thread_id": "t1", "stored": True}
    mock_client.add_cosmos.assert_awaited_once()
    kwargs = mock_client.add_cosmos.await_args.kwargs
    assert kwargs["user_id"] == "user-test"
    assert kwargs["role"] == "user"
    assert kwargs["content"] == "hi"
    assert kwargs["memory_type"] == "turn"
    assert kwargs["thread_id"] == "t1"
    assert kwargs["metadata"] == {"source": "chat"}
    assert kwargs["tags"] == ["greeting"]


async def test_add_memory_turn_missing_thread_id_raises_tool_error(deps, mock_client, make_ctx):
    tools = catch_tools(capture.register, deps)
    ctx = make_ctx(mock_client)

    with pytest.raises(ToolError) as exc_info:
        await tools["add_memory"](content="hi", memory_type="turn", role="user", ctx=ctx)

    assert "invalid_params" in str(exc_info.value)
    assert "thread_id" in str(exc_info.value)
    mock_client.add_cosmos.assert_not_awaited()


async def test_add_memory_turn_missing_role_raises_tool_error(deps, mock_client, make_ctx):
    tools = catch_tools(capture.register, deps)
    ctx = make_ctx(mock_client)

    with pytest.raises(ToolError) as exc_info:
        await tools["add_memory"](content="hi", memory_type="turn", thread_id="t1", ctx=ctx)

    assert "invalid_params" in str(exc_info.value)
    assert "role" in str(exc_info.value)
    mock_client.add_cosmos.assert_not_awaited()


async def test_add_memory_invalid_role_raises_tool_error(deps, mock_client, make_ctx):
    tools = catch_tools(capture.register, deps)
    ctx = make_ctx(mock_client)

    with pytest.raises(ToolError) as exc_info:
        await tools["add_memory"](
            content="hi", memory_type="turn", role="assistant", thread_id="t1", ctx=ctx
        )

    assert "invalid_params" in str(exc_info.value)
    assert "role" in str(exc_info.value)
    mock_client.add_cosmos.assert_not_awaited()


async def test_add_memory_invalid_memory_type_raises_tool_error(deps, mock_client, make_ctx):
    tools = catch_tools(capture.register, deps)
    ctx = make_ctx(mock_client)

    with pytest.raises(ToolError) as exc_info:
        await tools["add_memory"](content="hi", memory_type="note", ctx=ctx)

    assert "invalid_params" in str(exc_info.value)
    assert "memory_type" in str(exc_info.value)
    mock_client.add_cosmos.assert_not_awaited()


async def test_add_memory_fact_ok(deps, mock_client, make_ctx):
    tools = catch_tools(capture.register, deps)
    ctx = make_ctx(mock_client)

    out = await tools["add_memory"](
        content="The user prefers concise answers.",
        memory_type="fact",
        thread_id="t1",
        tags=["preference"],
        salience=0.8,
        ctx=ctx,
    )

    assert out == {"id": "mem-123", "memory_type": "fact", "thread_id": "t1", "stored": True}
    mock_client.add_cosmos.assert_awaited_once()
    kwargs = mock_client.add_cosmos.await_args.kwargs
    assert kwargs["user_id"] == "user-test"
    assert kwargs["role"] == "user"
    assert kwargs["content"] == "The user prefers concise answers."
    assert kwargs["memory_type"] == "fact"
    assert kwargs["thread_id"] == "t1"
    assert kwargs["tags"] == ["preference"]
    assert kwargs["salience"] == 0.8


async def test_add_memory_fact_defaults_user_and_thread(deps, mock_client, make_ctx):
    tools = catch_tools(capture.register, deps)
    ctx = make_ctx(mock_client)

    out = await tools["add_memory"]("A durable fact", ctx=ctx)

    assert out["stored"] is True
    assert out["memory_type"] == "fact"
    assert out["thread_id"] is None
    mock_client.add_cosmos.assert_awaited_once()
    kwargs = mock_client.add_cosmos.await_args.kwargs
    assert kwargs["user_id"] == "user-test"
    assert kwargs["role"] == "user"
    assert kwargs["memory_type"] == "fact"
    assert kwargs["thread_id"] is None


async def test_add_memory_episodic_accepts_explicit_role(deps, mock_client, make_ctx):
    tools = catch_tools(capture.register, deps)
    ctx = make_ctx(mock_client)

    out = await tools["add_memory"](
        content="Resolved an outage by rolling back.",
        memory_type="episodic",
        role="agent",
        ctx=ctx,
    )

    assert out["memory_type"] == "episodic"
    kwargs = mock_client.add_cosmos.await_args.kwargs
    assert kwargs["role"] == "agent"
    assert kwargs["memory_type"] == "episodic"
