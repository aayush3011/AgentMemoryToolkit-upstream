from __future__ import annotations

from conftest import catch_tools

from agent_memory_mcp.tools import session


async def test_whoami_reports_default_user_and_auth_flag(deps, make_ctx, mock_client):
    tools = catch_tools(session.register, deps)
    ctx = make_ctx(mock_client)

    assert await tools["whoami"](ctx=ctx) == {
        "user_id": "user-test",
        "auth_enabled": False,
    }


def test_session_register_wires_only_whoami(deps):
    tools = catch_tools(session.register, deps)

    assert set(tools) == {"whoami"}
