def test_generate_thread_summary(api, mock_memory):
    mock_memory.generate_thread_summary.return_value = {
        "id": "ts1",
        "type": "thread_summary",
        "content": "Jordan is planning a Portugal trip.",
        "thread_id": "t1",
        "created_at": "2026-01-01",
        "metadata": {"structured_summary": {"topic": "trip"}},
        "embedding": [0.1],
    }

    response = api.post("/users/u1/threads/t1/summary", json={})

    assert response.status_code == 201
    body = response.json()
    assert body["memory_type"] == "thread_summary"
    assert body["content"] == "Jordan is planning a Portugal trip."
    assert body["structured_summary"] == {"topic": "trip"}
    assert "embedding" not in body
    mock_memory.generate_thread_summary.assert_awaited_once_with(user_id="u1", thread_id="t1", recent_k=None)


def test_generate_thread_summary_with_recent_k(api, mock_memory):
    mock_memory.generate_thread_summary.return_value = {"id": "ts1", "type": "thread_summary", "content": "x"}

    response = api.post("/users/u1/threads/t1/summary", json={"recent_k": 5})

    assert response.status_code == 201
    mock_memory.generate_thread_summary.assert_awaited_once_with(user_id="u1", thread_id="t1", recent_k=5)


def test_get_thread_summary(api, mock_memory):
    mock_memory.get_thread_summary.return_value = [
        {"id": "ts1", "type": "thread_summary", "content": "recap", "thread_id": "t1", "embedding": [0.1]}
    ]

    response = api.get("/users/u1/threads/t1/summary")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 1
    assert body["items"][0]["content"] == "recap"
    assert "embedding" not in body["items"][0]
    mock_memory.get_thread_summary.assert_awaited_once_with(user_id="u1", thread_id="t1")


def test_generate_user_summary(api, mock_memory):
    mock_memory.generate_user_summary.return_value = {
        "id": "us1",
        "type": "user_summary",
        "content": "Jordan: Portugal traveler.",
        "created_at": "2026-01-02",
    }

    response = api.post("/users/u1/summary", json={})

    assert response.status_code == 201
    body = response.json()
    assert body["memory_type"] == "user_summary"
    assert body["content"] == "Jordan: Portugal traveler."
    mock_memory.generate_user_summary.assert_awaited_once_with(user_id="u1", thread_ids=None, recent_k=None)


def test_generate_user_summary_with_options(api, mock_memory):
    mock_memory.generate_user_summary.return_value = {"id": "us1", "type": "user_summary", "content": "x"}

    response = api.post("/users/u1/summary", json={"thread_ids": ["t1", "t2"], "recent_k": 3})

    assert response.status_code == 201
    mock_memory.generate_user_summary.assert_awaited_once_with(user_id="u1", thread_ids=["t1", "t2"], recent_k=3)


def test_get_user_summary(api, mock_memory):
    mock_memory.get_user_summary.return_value = {
        "id": "us1",
        "type": "user_summary",
        "content": "Jordan profile.",
    }

    response = api.get("/users/u1/summary")

    assert response.status_code == 200
    assert response.json()["content"] == "Jordan profile."
    mock_memory.get_user_summary.assert_awaited_once_with(user_id="u1")


def test_get_user_summary_missing_returns_404(api, mock_memory):
    mock_memory.get_user_summary.return_value = None

    response = api.get("/users/u1/summary")

    assert response.status_code == 404
    assert response.headers["content-type"].startswith("application/problem+json")
