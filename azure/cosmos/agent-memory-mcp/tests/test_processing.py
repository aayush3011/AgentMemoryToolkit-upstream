from __future__ import annotations

import dataclasses

import pytest
from conftest import catch_tools
from mcp.server.fastmcp.exceptions import ToolError

from agent_memory_mcp.config import Settings
from agent_memory_mcp.context import Deps
from agent_memory_mcp.tools import processing


@dataclasses.dataclass
class SampleProcessThreadResult:
    thread_summary: dict | None
    extracted_counts: dict[str, int]
    reconciled_count: int
    elapsed_ms: int
    procedural: dict | None
    user_summary: dict | None


@pytest.fixture
def deps() -> Deps:
    settings = Settings(
        cosmos_endpoint="https://test.documents.azure.com:443/",
        ai_foundry_endpoint="https://test.openai.azure.com/",
        cosmos_database="ai_memory",
        auth_enabled=False,
        default_user_id="user-test",
        enable_turn_embeddings=True,
        max_top_k=50,
    )
    return Deps(settings=settings)


def _with_granular(deps: Deps) -> Deps:
    return Deps(
        settings=deps.settings.model_copy(update={"expose_granular": True}),
    )


async def test_process_thread_ok_serializes_and_strips_embeddings(deps, mock_client, make_ctx):
    tools = catch_tools(processing.register, deps)
    ctx = make_ctx(mock_client)
    mock_client.process_now.return_value = SampleProcessThreadResult(
        thread_summary={"id": "s1", "type": "thread_summary", "embedding": [0.1, 0.2], "_etag": "etag"},
        extracted_counts={"fact": 2, "episodic": 1, "procedural": 0},
        reconciled_count=3,
        elapsed_ms=42,
        procedural={"id": "p1", "content": "Prefer concise replies.", "content_vector": [0.3]},
        user_summary={"id": "u1", "content": "User profile", "profileEmbedding": [0.4]},
    )

    out = await tools["process_thread"](thread_id="t1", ctx=ctx)

    assert out["extracted_counts"] == {"fact": 2, "episodic": 1, "procedural": 0}
    assert out["thread_summary"] == {"id": "s1", "memory_type": "thread_summary"}
    assert out["procedural"] == {"id": "p1", "content": "Prefer concise replies."}
    assert out["user_summary"] == {"id": "u1", "content": "User profile"}
    mock_client.process_now.assert_awaited_once_with(user_id="user-test", thread_id="t1")


async def test_process_thread_missing_thread_id_raises_tool_error(deps, mock_client, make_ctx):
    tools = catch_tools(processing.register, deps)
    ctx = make_ctx(mock_client)

    with pytest.raises(ToolError) as exc_info:
        await tools["process_thread"](ctx=ctx)

    assert "invalid_params" in str(exc_info.value)
    assert "thread_id" in str(exc_info.value)
    mock_client.process_now.assert_not_awaited()


def test_granular_tools_are_gated_off_by_default(deps):
    tools = catch_tools(processing.register, deps)

    assert set(tools) == {"process_thread"}
    assert "summarize_thread" not in tools
    assert "extract_memories" not in tools
    assert "update_user_profile" not in tools
    assert "reconcile_memories" not in tools


async def test_granular_tools_register_when_enabled_and_require_thread_id(deps, mock_client, make_ctx):
    tools = catch_tools(processing.register, _with_granular(deps))
    ctx = make_ctx(mock_client)

    assert set(tools) == {
        "process_thread",
        "summarize_thread",
        "extract_memories",
        "update_user_profile",
        "reconcile_memories",
    }

    with pytest.raises(ToolError) as summarize_exc:
        await tools["summarize_thread"](ctx=ctx)
    assert "thread_id" in str(summarize_exc.value)
    mock_client.generate_thread_summary.assert_not_awaited()

    with pytest.raises(ToolError) as extract_exc:
        await tools["extract_memories"](ctx=ctx)
    assert "thread_id" in str(extract_exc.value)
    mock_client.extract_memories.assert_not_awaited()
