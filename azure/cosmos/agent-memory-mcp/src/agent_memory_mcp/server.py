"""FastMCP application factory and process entry points.

``build_server`` wires configuration, auth, the shared async memory client
(created/closed in the FastMCP lifespan), and the tool registry into a single
:class:`FastMCP` instance. ``run`` builds it and serves the configured transport
(``stdio`` locally, ``streamable-http`` when hosted).
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import AsyncIterator
from typing import Optional

from azure.cosmos.agent_memory.aio import (
    AsyncCosmosMemoryClient,
    AsyncDurableFunctionProcessor,
)
from mcp.server.fastmcp import FastMCP

from .auth import build_auth
from .config import Settings, get_settings
from .context import AppContext, Deps
from .tools import register_all

logger = logging.getLogger("agent_memory_mcp")

INSTRUCTIONS = """\
Long-term memory for AI agents, backed by Azure Cosmos DB.

Use these tools to give the agent durable, cross-session memory:
- Capture what happens: `add_memory` stores any memory — a conversation turn
  (memory_type="turn" with a role), or a durable fact/episodic/procedural memory.
- Recall for context: `search_memories` (semantic/hybrid recall of facts,
  experiences, and rules), `get_memories` (filtered fetch), `recall_thread`
  (raw conversation), `search_turns` (semantic search over turns),
  `get_user_summary` (who the user is), `get_thread_summary`.
- Consolidate: `process_thread` runs summarization + extraction over recent turns.
- Correct: `update_memory`, `delete_memory`, `get_memory_history`.
- Identity: `whoami` reports the resolved user id.

Pass `thread_id` explicitly to scope conversation calls. In hosted mode the user
is taken from the authenticated identity; locally it falls back to
`AGENT_MEMORY_DEFAULT_USER_ID`.
"""


def build_client(settings: Settings) -> AsyncCosmosMemoryClient:
    """Construct (but do not connect) the async memory client from settings."""
    processor = None
    if (settings.processor_owner or "").strip().lower() == "durable":
        # Thin-writer mode: defer heavy LLM processing to the sibling Function app.
        processor = AsyncDurableFunctionProcessor()
    return AsyncCosmosMemoryClient(
        cosmos_endpoint=settings.cosmos_endpoint,
        cosmos_key=settings.cosmos_key or None,
        cosmos_database=settings.cosmos_database,
        cosmos_container=settings.cosmos_memories_container,
        cosmos_turns_container=settings.cosmos_turns_container,
        cosmos_summaries_container=settings.cosmos_summaries_container,
        cosmos_counter_container=settings.cosmos_counters_container,
        cosmos_lease_container=settings.cosmos_lease_container,
        ai_foundry_endpoint=settings.ai_foundry_endpoint,
        ai_foundry_api_key=settings.ai_foundry_api_key or None,
        embedding_deployment_name=settings.embedding_deployment_name,
        embedding_dimensions=settings.embedding_dimensions,
        chat_deployment_name=settings.chat_deployment_name,
        enable_turn_embeddings=settings.enable_turn_embeddings,
        use_default_credential=True,
        user_agent="agent-memory-mcp",
        processor=processor,
    )


def _make_lifespan(settings: Settings):
    @contextlib.asynccontextmanager
    async def lifespan(_server: FastMCP) -> AsyncIterator[AppContext]:
        client = build_client(settings)
        if settings.auto_create:
            await client.create_memory_store()
        else:
            await client.connect_cosmos()
        try:
            await client.validate_topology()
        except Exception as exc:  # noqa: BLE001 - warn, don't crash on drift
            logger.warning("Topology validation failed (continuing): %s", exc)
        try:
            logger.info(
                "Agent Memory MCP connected to %s/%s",
                settings.cosmos_database,
                settings.cosmos_memories_container,
            )
            yield AppContext(client=client, settings=settings)
        finally:
            await client.close()

    return lifespan


def build_server(settings: Optional[Settings] = None) -> FastMCP:
    """Build a fully-configured :class:`FastMCP` server (tools registered)."""
    settings = settings or get_settings()
    token_verifier, auth_settings = build_auth(settings)

    kwargs: dict = dict(
        name="agent-memory",
        instructions=INSTRUCTIONS,
        lifespan=_make_lifespan(settings),
        host=settings.host,
        port=settings.port,
        streamable_http_path=settings.streamable_http_path,
        stateless_http=settings.stateless_http,
        log_level=settings.log_level.upper(),
    )
    if token_verifier is not None and auth_settings is not None:
        kwargs["token_verifier"] = token_verifier
        kwargs["auth"] = auth_settings

    mcp = FastMCP(**kwargs)
    deps = Deps(settings=settings)
    register_all(mcp, deps)
    return mcp


def run(settings: Optional[Settings] = None) -> None:
    """Build the server and run it over the configured transport."""
    settings = settings or get_settings()
    logging.basicConfig(level=settings.log_level.upper())
    mcp = build_server(settings)
    logger.info("Starting Agent Memory MCP server (transport=%s)", settings.transport)
    mcp.run(transport=settings.transport)
