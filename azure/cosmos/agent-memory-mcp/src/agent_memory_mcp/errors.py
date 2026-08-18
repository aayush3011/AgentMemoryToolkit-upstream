"""Map Agent Memory SDK exceptions to clean MCP tool errors.

Tools should wrap their body with :func:`tool_errors` (a decorator that preserves
the wrapped function's signature so FastMCP still generates the correct input
schema and injects ``Context``). SDK exceptions are translated into
:class:`mcp.server.fastmcp.exceptions.ToolError` with a short, category-prefixed
message and never leak stack traces, endpoints, or credentials to the caller.
"""

from __future__ import annotations

import functools
from typing import Awaitable, Callable, TypeVar

from azure.cosmos.agent_memory.exceptions import (
    AgentMemoryError,
    ConfigurationError,
    CosmosNotConnectedError,
    CosmosOperationError,
    LLMError,
    MemoryConflictError,
    MemoryNotFoundError,
    MemoryTypeMismatchError,
    ValidationError,
)
from mcp.server.fastmcp.exceptions import ToolError

T = TypeVar("T")

# Error categories surfaced to the agent (prefix on the ToolError message).
INVALID_PARAMS = "invalid_params"
NOT_FOUND = "not_found"
CONFLICT = "conflict"
UNAVAILABLE = "unavailable"
INTERNAL = "internal_error"


def to_tool_error(exc: Exception) -> ToolError:
    """Translate an exception into a categorized :class:`ToolError`."""
    if isinstance(exc, ToolError):
        return exc
    if isinstance(exc, (ValidationError, MemoryTypeMismatchError, ConfigurationError)):
        return ToolError(f"{INVALID_PARAMS}: {exc}")
    if isinstance(exc, MemoryNotFoundError):
        return ToolError(f"{NOT_FOUND}: {exc}")
    if isinstance(exc, MemoryConflictError):
        return ToolError(f"{CONFLICT}: {exc}")
    if isinstance(exc, (CosmosNotConnectedError, CosmosOperationError, LLMError)):
        return ToolError(f"{UNAVAILABLE}: {exc}")
    if isinstance(exc, AgentMemoryError):
        return ToolError(f"{INTERNAL}: {exc}")
    # Common Python builtins raised by bad arguments.
    if isinstance(exc, (KeyError, ValueError, TypeError)):
        return ToolError(f"{INVALID_PARAMS}: {exc}")
    return ToolError(f"{INTERNAL}: {exc}")


def tool_errors(fn: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
    """Decorator: translate SDK/Python exceptions into :class:`ToolError`.

    Preserves ``fn``'s signature via :func:`functools.wraps` so FastMCP's schema
    generation and ``Context`` injection continue to work unchanged.
    """

    @functools.wraps(fn)
    async def wrapper(*args: object, **kwargs: object) -> T:
        try:
            return await fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001 - deliberate: normalize all failures
            raise to_tool_error(exc) from exc

    return wrapper
