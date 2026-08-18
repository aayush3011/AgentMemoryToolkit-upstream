"""Shared pytest fixtures and helpers for the Agent Memory MCP tests.

Testing pattern (no private FastMCP APIs):

    from agent_memory_mcp.tools import capture

    def test_add_memory(deps, make_ctx, mock_client):
        tools = catch_tools(capture.register, deps)      # {name: wrapped_fn}
        ctx = make_ctx(mock_client)
        result = await tools["add_memory"](
            content="hi", memory_type="turn", role="user", thread_id="t1", ctx=ctx
        )

``catch_tools`` registers a module against a fake MCP whose ``.tool()`` decorator
captures each function (already wrapped by ``@tool_errors``). ``make_ctx`` builds a
fake :class:`Context` whose ``request_context.lifespan_context`` is an
:class:`AppContext` carrying a mock async client, so tools that call
``get_client(ctx)`` work without a live Cosmos connection.
"""

from __future__ import annotations

from typing import Any, Callable, Optional
from unittest.mock import AsyncMock

import pytest

from agent_memory_mcp.config import Settings
from agent_memory_mcp.context import AppContext, Deps

# --- Fake MCP that captures registered tool functions --------------------------

class _ToolCatcher:
    """Minimal stand-in for FastMCP: ``.tool(...)`` captures the decorated fn."""

    def __init__(self) -> None:
        self.tools: dict[str, Callable[..., Any]] = {}

    def tool(self, *_args: Any, name: Optional[str] = None, **_kwargs: Any):
        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            self.tools[name or fn.__name__] = fn
            return fn

        return decorator


def catch_tools(register: Callable[[Any, Deps], None], deps: Deps) -> dict[str, Callable[..., Any]]:
    """Run a module's ``register`` against a catcher and return ``{name: fn}``."""
    catcher = _ToolCatcher()
    register(catcher, deps)
    return catcher.tools


# --- Fake request context / Context -------------------------------------------

class _FakeRequestContext:
    def __init__(self, app: AppContext, session: Any) -> None:
        self.lifespan_context = app
        self.session = session
        self.request = None
        self.request_id = "test-request"
        self.meta = None


class _FakeContext:
    """Duck-typed :class:`mcp.server.fastmcp.Context` for unit tests."""

    def __init__(self, app: AppContext, session: Any) -> None:
        self.request_context = _FakeRequestContext(app, session)
        # Async logging helpers tools may call.
        self.info = AsyncMock()
        self.debug = AsyncMock()
        self.warning = AsyncMock()
        self.error = AsyncMock()
        self.report_progress = AsyncMock()


# --- Fixtures ------------------------------------------------------------------

@pytest.fixture
def settings() -> Settings:
    """Auth-disabled settings with a default user for local-style tests."""
    return Settings(
        cosmos_endpoint="https://test.documents.azure.com:443/",
        ai_foundry_endpoint="https://test.openai.azure.com/",
        cosmos_database="ai_memory",
        auth_enabled=False,
        default_user_id="user-test",
        enable_turn_embeddings=True,
        expose_granular=True,
        max_top_k=50,
    )


@pytest.fixture
def mock_client() -> AsyncMock:
    """AsyncMock of AsyncCosmosMemoryClient with the wrapped methods present."""
    client = AsyncMock()
    # Sensible default return values; individual tests override as needed.
    client.add_cosmos = AsyncMock(return_value="mem-123")
    client.get_memories = AsyncMock(return_value=[])
    client.search_cosmos = AsyncMock(return_value=[])
    client.search_turns = AsyncMock(return_value=[])
    client.get_thread = AsyncMock(return_value=[])
    client.get_thread_summary = AsyncMock(return_value=[])
    client.get_user_summary = AsyncMock(return_value=None)
    client.get_memory_history = AsyncMock(return_value=[])
    client.update_cosmos = AsyncMock(return_value=None)
    client.delete_cosmos = AsyncMock(return_value=None)
    client.extract_memories = AsyncMock(return_value={})
    client.generate_thread_summary = AsyncMock(return_value={})
    client.generate_user_summary = AsyncMock(return_value={})
    client.reconcile = AsyncMock(return_value={})
    client.process_now = AsyncMock()
    return client


@pytest.fixture
def deps(settings: Settings) -> Deps:
    return Deps(settings=settings)


@pytest.fixture
def make_ctx(settings: Settings):
    """Factory: ``make_ctx(mock_client, session=<obj>)`` -> fake Context."""

    def _factory(client: Any, session: Optional[Any] = None) -> Any:
        app = AppContext(client=client, settings=settings)
        return _FakeContext(app=app, session=session if session is not None else object())

    return _factory
