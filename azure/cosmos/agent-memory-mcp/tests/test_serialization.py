from agent_memory_mcp.serialization import list_payload, project_record, project_records


def test_project_record_strips_embeddings_internals_and_normalizes_type():
    doc = {
        "id": "m1",
        "type": "semantic",
        "content": "hello",
        "embedding": [1.0, 2.0],
        "custom_vector": [0.1, 0.2, 0.3, 0.4],
        "_rid": "rid",
        "_self": "self",
        "_etag": "etag",
        "_attachments": "attachments",
        "_ts": 123,
    }

    assert project_record(doc) == {"id": "m1", "content": "hello", "memory_type": "semantic"}


def test_project_record_preserves_existing_memory_type_and_passes_non_mappings():
    doc = {"type": "semantic", "memory_type": "episodic", "content": "hello"}

    assert project_record(doc) == {"type": "semantic", "memory_type": "episodic", "content": "hello"}
    assert project_record(None) is None


def test_project_records_maps_projection_across_list():
    docs = [{"id": "1", "embedding": [1]}, {"id": "2", "type": "rule", "_etag": "x"}]

    assert project_records(docs) == [{"id": "1"}, {"id": "2", "memory_type": "rule"}]


def test_list_payload_counts_items_and_strips_embeddings():
    docs = [{"id": "1", "embedding": [1]}, {"id": "2", "contentVector": [1, 2, 3, 4]}]

    assert list_payload(docs) == {"items": [{"id": "1"}, {"id": "2"}], "count": 2, "truncated": False}


def test_list_payload_respects_limit_and_marks_truncated():
    docs = [{"id": "1", "vector": [1]}, {"id": "2"}, {"id": "3"}]

    assert list_payload(docs, limit=2) == {
        "items": [{"id": "1"}, {"id": "2"}],
        "count": 2,
        "truncated": True,
    }
