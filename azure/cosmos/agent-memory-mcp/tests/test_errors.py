import pytest
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

from agent_memory_mcp.errors import (
    CONFLICT,
    INTERNAL,
    INVALID_PARAMS,
    NOT_FOUND,
    UNAVAILABLE,
    to_tool_error,
    tool_errors,
)


@pytest.mark.asyncio
async def test_tool_errors_returns_success_value_and_preserves_name():
    async def succeeds():
        return {"ok": True}

    wrapped = tool_errors(succeeds)

    assert wrapped.__name__ == "succeeds"
    assert await wrapped() == {"ok": True}


@pytest.mark.parametrize(
    ("exc", "category"),
    [
        (ValidationError("bad input"), INVALID_PARAMS),
        (MemoryTypeMismatchError("wrong type"), INVALID_PARAMS),
        (ConfigurationError("bad config"), INVALID_PARAMS),
        (MemoryNotFoundError("missing"), NOT_FOUND),
        (MemoryConflictError("conflict"), CONFLICT),
        (CosmosNotConnectedError("offline"), UNAVAILABLE),
        (CosmosOperationError("cosmos failed"), UNAVAILABLE),
        (LLMError("llm failed"), UNAVAILABLE),
        (AgentMemoryError("sdk failed"), INTERNAL),
        (ValueError("bad value"), INVALID_PARAMS),
        (RuntimeError("boom"), INTERNAL),
    ],
)
@pytest.mark.asyncio
async def test_tool_errors_maps_exceptions_to_categorized_tool_errors(exc, category):
    async def fails():
        raise exc

    with pytest.raises(ToolError) as raised:
        await tool_errors(fails)()

    assert category in str(raised.value)


def test_to_tool_error_passes_existing_tool_error_through():
    original = ToolError("already categorized")

    assert to_tool_error(original) is original
