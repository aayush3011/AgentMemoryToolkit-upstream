"""Tool registry.

Each tool module exposes ``register(mcp: FastMCP, deps: Deps) -> None`` which
declares its ``@mcp.tool()`` functions. :func:`register_all` wires them all onto a
single server. Keeping registration in per-module functions lets the modules be
developed and tested independently without editing a shared file.
"""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from agent_memory_mcp.context import Deps

from . import capture, lifecycle, processing, profile, retrieval, session


def register_all(mcp: FastMCP, deps: Deps) -> None:
    """Register every tool module onto ``mcp``."""
    session.register(mcp, deps)
    capture.register(mcp, deps)
    retrieval.register(mcp, deps)
    profile.register(mcp, deps)
    lifecycle.register(mcp, deps)
    processing.register(mcp, deps)


__all__ = ["register_all"]
