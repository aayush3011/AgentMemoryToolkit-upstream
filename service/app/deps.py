from fastapi import Request

try:
    from azure.cosmos.agent_memory.aio import CosmosMemoryClient as AsyncCosmosMemoryClient
except ImportError:
    from azure.cosmos.agent_memory.aio import AsyncCosmosMemoryClient


async def get_client(request: Request) -> AsyncCosmosMemoryClient:
    """Return the process-wide AsyncCosmosMemoryClient stored on app.state.memory_client.
    Used by routers as: client = Depends(get_client).
    """
    return request.app.state.memory_client


async def require_identity() -> None:
    """Local dev no-op; production resolves user_id and tenant_id from the credential."""
    return None
