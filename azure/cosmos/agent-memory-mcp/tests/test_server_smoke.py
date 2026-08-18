import pytest
from conftest import catch_tools
from mcp.server.fastmcp import FastMCP

from agent_memory_mcp.server import build_server
from agent_memory_mcp.tools import session

BASE_TOOLS = {
    "add_memory",
    "search_memories",
    "get_memories",
    "recall_thread",
    "search_turns",
    "get_user_summary",
    "get_thread_summary",
    "update_memory",
    "delete_memory",
    "get_memory_history",
    "process_thread",
    "whoami",
}

GRANULAR_TOOLS = {
    "summarize_thread",
    "extract_memories",
    "update_user_profile",
    "reconcile_memories",
}


@pytest.mark.asyncio
async def test_build_server_exposes_exactly_the_base_surface(settings):
    base_settings = settings.model_copy(update={"expose_granular": False})
    mcp = build_server(settings=base_settings)

    assert isinstance(mcp, FastMCP)
    tool_names = {tool.name for tool in await mcp.list_tools()}
    assert tool_names == BASE_TOOLS


@pytest.mark.asyncio
async def test_build_server_adds_granular_tools_when_enabled(settings):
    granular_settings = settings.model_copy(update={"expose_granular": True})
    mcp = build_server(settings=granular_settings)

    tool_names = {tool.name for tool in await mcp.list_tools()}
    assert tool_names == BASE_TOOLS | GRANULAR_TOOLS


def test_session_register_wires_only_whoami(deps):
    tools = catch_tools(session.register, deps)

    assert set(tools) == {"whoami"}
