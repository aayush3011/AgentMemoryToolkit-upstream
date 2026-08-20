from unittest.mock import AsyncMock

import pytest
from app.deps import get_client
from app.main import app
from fastapi.testclient import TestClient

try:
    from azure.cosmos.agent_memory.aio import CosmosMemoryClient as AsyncCosmosMemoryClient
except ImportError:
    from azure.cosmos.agent_memory.aio import AsyncCosmosMemoryClient


@pytest.fixture
def mock_memory():
    return AsyncMock(spec=AsyncCosmosMemoryClient)


@pytest.fixture
def api(mock_memory):
    async def _override_get_client():
        return mock_memory

    app.dependency_overrides[get_client] = _override_get_client
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.pop(get_client, None)
