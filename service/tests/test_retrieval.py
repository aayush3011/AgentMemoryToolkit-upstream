def test_search_returns_full_document(api, mock_memory):
    mock_memory.search_cosmos.return_value = [
        {
            "id": "m1",
            "type": "fact",
            "content": "hello",
            "created_at": "2026-01-01",
            "updated_at": "2026-01-02",
            "tags": ["sys:agent-fact"],
            "confidence": 0.8,
            "salience": 0.5,
            "source_turn_ids": ["t1", "t2"],
            "metadata": {"foo": "bar"},
            "score": 0.9,
            "embedding": [0.1, 0.2],
            "_etag": "abc",
            "_rid": "xyz",
            "_ts": 1234567890,
        }
    ]

    response = api.post("/users/u1/search", json={"query": "hi", "top_k": 5})

    assert response.status_code == 200
    item = response.json()["items"][0]
    # Everything the store persisted is passed through...
    for key in (
        "id",
        "type",
        "content",
        "created_at",
        "updated_at",
        "tags",
        "confidence",
        "salience",
        "source_turn_ids",
        "metadata",
        "score",
    ):
        assert key in item
    assert item["metadata"] == {"foo": "bar"}
    assert item["memory_type"] == "fact"  # alias added
    # ...except embeddings and Cosmos system fields.
    assert "embedding" not in item
    assert not any(k.startswith("_") for k in item)


def test_search_happy_path(api, mock_memory):
    mock_memory.search_cosmos.return_value = [
        {
            "id": "m1",
            "type": "fact",
            "content": "hello",
            "created_at": "2026-01-01",
            "score": 0.9,
            "embedding": [0.1, 0.2],
        }
    ]

    response = api.post("/users/u1/search", json={"query": "hi", "top_k": 5})

    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"items", "count", "truncated"}
    assert body["count"] == 1
    assert body["truncated"] is False
    assert body["items"][0]["memory_type"] == "fact"
    assert body["items"][0]["score"] == 0.9
    assert "embedding" not in body["items"][0]
    mock_memory.search_cosmos.assert_awaited_once_with(
        search_terms="hi",
        user_id="u1",
        memory_types=None,
        top_k=5,
        min_confidence=None,
        min_salience=None,
        tags_any=None,
        tags_all=None,
        exclude_tags=None,
        created_after=None,
        created_before=None,
        include_episodes=True,
    )


def test_search_include_episodes_can_be_disabled(api, mock_memory):
    mock_memory.search_cosmos.return_value = []

    response = api.post(
        "/users/u1/search",
        json={"query": "hi", "include_episodes": False},
    )

    assert response.status_code == 200
    assert response.json() == {"items": [], "count": 0, "truncated": False}
    assert mock_memory.search_cosmos.await_args.kwargs["include_episodes"] is False


def test_search_missing_query_returns_422(api):
    response = api.post("/users/u1/search", json={"top_k": 5})

    assert response.status_code == 422


def test_search_top_k_zero_returns_422(api):
    response = api.post("/users/u1/search", json={"query": "hi", "top_k": 0})

    assert response.status_code == 422


def test_episodes_happy_path(api, mock_memory):
    mock_memory.search_episodic_memories.return_value = [
        {
            "id": "e1",
            "type": "episodic",
            "content": "trip",
            "created_at": "2026-01-02",
            "title": "Booked Portugal trip",
            "outcome": {"status": "successful", "description": "Flight + hotel booked."},
            "lessons": ["Book refundable rates."],
            "started_at": "2026-01-01",
            "ended_at": "2026-01-02",
            "embedding": [0.1, 0.2],
        }
    ]

    response = api.post("/users/u1/search/episodes", json={"query": "trip"})

    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"items", "count", "truncated"}
    assert body["count"] == 1
    assert body["truncated"] is False
    item = body["items"][0]
    assert item["memory_type"] == "episodic"
    assert item["content"] == "trip"
    assert item["title"] == "Booked Portugal trip"
    assert item["outcome"]["status"] == "successful"
    assert item["lessons"] == ["Book refundable rates."]
    assert "embedding" not in item
    mock_memory.search_episodic_memories.assert_awaited_once_with(
        user_id="u1",
        search_terms="trip",
        top_k=5,
        min_salience=None,
    )


def test_episodes_missing_query_returns_422(api):
    response = api.post("/users/u1/search/episodes", json={})

    assert response.status_code == 422


def test_list_memories_no_thread(api, mock_memory):
    mock_memory.get_memories.return_value = [{"id": "m1", "type": "fact", "content": "hi", "embedding": [0.1]}]

    response = api.get("/users/u1/memories")

    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"items", "count", "truncated"}
    assert body["count"] == 1
    assert body["items"][0]["memory_type"] == "fact"
    assert "embedding" not in body["items"][0]
    mock_memory.get_memories.assert_awaited_once_with(
        user_id="u1",
        thread_id=None,
        memory_types=None,
        recent_k=None,
        tags_any=None,
        tags_all=None,
        exclude_tags=None,
        include_superseded=False,
        min_salience=None,
        min_confidence=None,
        created_after=None,
        created_before=None,
    )


def test_list_memories_with_filters(api, mock_memory):
    mock_memory.get_memories.return_value = []

    response = api.get(
        "/users/u1/memories",
        params={"thread_id": "t1", "memory_types": ["fact", "episodic"], "recent_k": 5},
    )

    assert response.status_code == 200
    kwargs = mock_memory.get_memories.await_args.kwargs
    assert kwargs["thread_id"] == "t1"
    assert kwargs["memory_types"] == ["fact", "episodic"]
    assert kwargs["recent_k"] == 5


def test_list_memories_empty_thread_returns_422(api):
    response = api.get("/users/u1/memories", params={"thread_id": ""})

    assert response.status_code == 422


def test_list_episodes_newest_first(api, mock_memory):
    mock_memory.get_episodes.return_value = [
        {"id": "e2", "type": "episodic", "content": "later", "embedding": [0.1]},
        {"id": "e1", "type": "episodic", "content": "earlier"},
    ]

    response = api.get("/users/u1/episodes")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 2
    assert body["items"][0]["id"] == "e2"
    assert "embedding" not in body["items"][0]
    mock_memory.get_episodes.assert_awaited_once_with(user_id="u1", thread_id=None, recent_k=None)


def test_list_episodes_with_thread_and_recent_k(api, mock_memory):
    mock_memory.get_episodes.return_value = []

    response = api.get("/users/u1/episodes", params={"thread_id": "t1", "recent_k": 3})

    assert response.status_code == 200
    mock_memory.get_episodes.assert_awaited_once_with(user_id="u1", thread_id="t1", recent_k=3)
