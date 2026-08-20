from fastapi.responses import JSONResponse

from azure.cosmos.agent_memory.exceptions import (
    AgentMemoryError,
    CosmosNotConnectedError,
    CosmosOperationError,
    MemoryConflictError,
    MemoryNotFoundError,
    ValidationError,
)


class GatewayAuthError(Exception):
    """Raised by the gateway auth dependency; rendered as problem+json."""

    def __init__(self, status: int, title: str, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.title = title
        self.detail = detail


def _problem(status: int, title: str, detail: str) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        media_type="application/problem+json",
        content={
            "type": "about:blank",
            "title": title,
            "status": status,
            "detail": detail,
        },
    )


def register_exception_handlers(app) -> None:
    """Register SDK exception handlers as RFC9457 problem+json responses."""

    async def validation_handler(request, exc: ValidationError) -> JSONResponse:
        return _problem(422, "Validation Error", str(exc))

    async def not_found_handler(request, exc: MemoryNotFoundError) -> JSONResponse:
        return _problem(404, "Memory Not Found", str(exc))

    async def conflict_handler(request, exc: MemoryConflictError) -> JSONResponse:
        return _problem(409, "Memory Conflict", str(exc))

    async def cosmos_handler(request, exc: CosmosOperationError | CosmosNotConnectedError) -> JSONResponse:
        return _problem(503, "Cosmos Service Unavailable", str(exc))

    async def agent_memory_handler(request, exc: AgentMemoryError) -> JSONResponse:
        return _problem(500, "Agent Memory Error", str(exc))

    async def gateway_auth_handler(request, exc: GatewayAuthError) -> JSONResponse:
        return _problem(exc.status, exc.title, exc.detail)

    app.add_exception_handler(ValidationError, validation_handler)
    app.add_exception_handler(MemoryNotFoundError, not_found_handler)
    app.add_exception_handler(MemoryConflictError, conflict_handler)
    app.add_exception_handler(CosmosOperationError, cosmos_handler)
    app.add_exception_handler(CosmosNotConnectedError, cosmos_handler)
    app.add_exception_handler(AgentMemoryError, agent_memory_handler)
    app.add_exception_handler(GatewayAuthError, gateway_auth_handler)
