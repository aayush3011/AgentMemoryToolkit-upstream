"""SDK-exception to HTTP mapping (RFC9457 problem+json).

These exercise the app-level exception handlers registered by
``app.core.errors.register_exception_handlers``: when a wrapped SDK call raises
a typed SDK exception, the endpoint must translate it to the right HTTP status
and a ``application/problem+json`` body, instead of surfacing a bare 500.
"""

from azure.cosmos.agent_memory.exceptions import (
    AgentMemoryError,
    CosmosNotConnectedError,
    CosmosOperationError,
    MemoryConflictError,
    MemoryNotFoundError,
    ValidationError,
)


def _assert_problem(resp, status: int) -> None:
    assert resp.status_code == status
    assert resp.headers["content-type"] == "application/problem+json"
    body = resp.json()
    assert body["status"] == status
    assert body["title"]
    assert "detail" in body


def test_sdk_validation_error_maps_to_422(api, mock_memory):
    mock_memory.reconcile.side_effect = ValidationError("bad user_id")
    _assert_problem(api.post("/users/u1/reconcile", json={}), 422)


def test_sdk_not_found_maps_to_404(api, mock_memory):
    mock_memory.search_cosmos.side_effect = MemoryNotFoundError(memory_id="x")
    _assert_problem(api.post("/users/u1/search", json={"query": "hi"}), 404)


def test_sdk_conflict_maps_to_409(api, mock_memory):
    mock_memory.upsert_memory.side_effect = MemoryConflictError("dup")
    _assert_problem(
        api.post(
            "/users/u1/threads/t1/memory",
            json={"role": "user", "content": "c"},
        ),
        409,
    )


def test_sdk_cosmos_operation_error_maps_to_503(api, mock_memory):
    mock_memory.upsert_memory.side_effect = CosmosOperationError("cosmos down")
    _assert_problem(
        api.post(
            "/users/u1/threads/t1/memory",
            json={"role": "user", "content": "hi"},
        ),
        503,
    )


def test_sdk_cosmos_not_connected_maps_to_503(api, mock_memory):
    mock_memory.search_episodic_memories.side_effect = CosmosNotConnectedError()
    _assert_problem(api.post("/users/u1/search/episodes", json={"query": "q"}), 503)


def test_base_agent_memory_error_maps_to_500(api, mock_memory):
    mock_memory.reconcile.side_effect = AgentMemoryError("unexpected")
    _assert_problem(api.post("/users/u1/reconcile", json={}), 500)
