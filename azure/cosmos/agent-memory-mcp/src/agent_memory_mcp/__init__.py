"""Agent Memory MCP server.

A Model Context Protocol server that exposes the Azure Cosmos DB Agent Memory
Toolkit (`azure-cosmos-agent-memory`) as a curated set of agent-facing tools.

The public entry points are :func:`agent_memory_mcp.server.build_server` (returns a
configured :class:`mcp.server.fastmcp.FastMCP` instance) and
:func:`agent_memory_mcp.server.run` (builds and runs it over the configured
transport). See ``DESIGN.md`` for the full design.
"""

from __future__ import annotations

from agent_memory_mcp.config import Settings, get_settings
from agent_memory_mcp.server import build_server, run

__all__ = ["Settings", "get_settings", "build_server", "run"]

__version__ = "0.1.0b1"
