def test_create_memory_happy_path(api, mock_memory):
    mock_memory.upsert_memory.return_value = "turn-1"

    response = api.post(
        "/users/user-1/threads/thread-1/memory",
        json={
            "role": "user",
            "content": "Hello",
            "tags": ["greeting"],
            "metadata": {"source": "test"},
            "salience": 0.7,
            "created_at": "2026-08-17T16:40:37Z",
        },
    )

    assert response.status_code == 201
    assert response.json() == {"id": "turn-1"}
    mock_memory.upsert_memory.assert_awaited_once_with(
        user_id="user-1",
        role="user",
        content="Hello",
        memory_type="turn",
        thread_id="thread-1",
        tags=["greeting"],
        metadata={"source": "test"},
        salience=0.7,
        created_at="2026-08-17T16:40:37Z",
    )


def test_create_memory_optional_fields_omitted_still_succeed(api, mock_memory):
    mock_memory.upsert_memory.return_value = "turn-2"

    response = api.post(
        "/users/user-1/threads/thread-1/memory",
        json={"role": "agent", "content": "Optional fields omitted."},
    )

    assert response.status_code == 201
    assert response.json() == {"id": "turn-2"}
    mock_memory.upsert_memory.assert_awaited_once_with(
        user_id="user-1",
        role="agent",
        content="Optional fields omitted.",
        memory_type="turn",
        thread_id="thread-1",
        tags=None,
        metadata=None,
        salience=None,
        created_at=None,
    )


def test_create_memory_missing_content_is_422(api, mock_memory):
    response = api.post(
        "/users/user-1/threads/thread-1/memory",
        json={"role": "user"},
    )

    assert response.status_code == 422
    mock_memory.upsert_memory.assert_not_awaited()


def test_create_memory_missing_role_is_422(api, mock_memory):
    response = api.post(
        "/users/user-1/threads/thread-1/memory",
        json={"content": "no role"},
    )

    assert response.status_code == 422
    mock_memory.upsert_memory.assert_not_awaited()
