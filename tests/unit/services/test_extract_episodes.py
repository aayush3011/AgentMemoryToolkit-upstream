from __future__ import annotations

import logging
from typing import Any

from azure.cosmos.agent_memory.services.pipeline import PipelineService
from tests.unit.services.test_extract_dry import (
    _containers_for_store,
    _response,
    _Store,
    _SyncChat,
    _SyncEmbeddings,
    _turn,
)


class _TrackingStore(_Store):
    def __init__(self, docs: list[dict[str, Any]]):
        super().__init__(docs)
        self.supersede_calls: list[dict[str, Any]] = []

    def mark_superseded(self, old_doc: dict[str, Any], superseder_id: str, *, reason: str) -> bool:
        self.supersede_calls.append({"old_doc": old_doc, "superseder_id": superseder_id, "reason": reason})
        return True


def _episode(
    *,
    title: str = "Fixed CI retries",
    summary: str = "The user fixed flaky CI retries and the tests passed.",
    outcome: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "title": title,
        "summary": summary,
        "started_at": "2025-01-01T00:01:00+00:00",
        "ended_at": "2025-01-01T00:02:00+00:00",
        "participants": [],
        "events": [
            {
                "sequence": 1,
                "description": "CI retries were added.",
                "occurred_at": "2025-01-01T00:01:00+00:00",
                "source_turn_ids": ["turn-1", "missing"],
            },
            {
                "sequence": 2,
                "description": "The tests passed.",
                "occurred_at": "2025-01-01T00:02:00+00:00",
                "source_turn_ids": ["turn-2", "turn-1"],
            },
        ],
        "outcome": outcome,
        "lessons": ["Use retries for flaky CI."],
        "salience": 0.8,
        "confidence": 0.9,
    }


def _service(
    responses: list[dict[str, Any]],
    *,
    memories_store: _TrackingStore | None = None,
    embeddings: _SyncEmbeddings | None = None,
) -> tuple[PipelineService, _TrackingStore, _SyncEmbeddings]:
    store = memories_store or _TrackingStore([])
    embedding_client = embeddings or _SyncEmbeddings()
    turns_store = _Store([_turn(1), _turn(2)])
    service = PipelineService(
        store,
        _SyncChat(responses),
        embedding_client,
        containers=_containers_for_store(store, turns_store=turns_store),
    )
    return service, store, embedding_client


def test_build_episode_docs_returns_multiple_docs_without_embeddings() -> None:
    outcome = {"status": "successful", "description": "The tests passed."}
    service, _, embeddings = _service(
        [
            {
                "episodes": [
                    _episode(outcome=outcome),
                    _episode(title="Planned vacation", summary="The user planned a vacation.", outcome=None),
                ]
            }
        ]
    )

    docs = service._build_episode_docs("u1", "t1", [_turn(1), _turn(2)], segment_key="seg-1")

    assert [doc["content"] for doc in docs] == [
        "The user fixed flaky CI retries and the tests passed.",
        "The user planned a vacation.",
    ]
    assert all(doc["id"].startswith("ep_") for doc in docs)
    assert all("embedding" not in doc for doc in docs)
    assert docs[0]["source_turn_ids"] == ["turn-1", "turn-2"]
    assert docs[0]["events"][0]["source_turn_ids"] == ["turn-1"]
    assert docs[0]["outcome"] == outcome
    assert docs[1]["outcome"] is None
    assert embeddings.calls == []


def test_extract_episodes_embeds_content_persists_append_only(monkeypatch) -> None:
    monkeypatch.setenv("EPISODE_TOPIC_DRIFT", "0")
    service, store, embeddings = _service(
        [
            {
                "episodes": [
                    _episode(),
                    _episode(title="Planned vacation", summary="The user planned a vacation.", outcome=None),
                ]
            }
        ]
    )

    result = service.extract_episodes("u1", "t1", flush=True)

    assert result == {"episodes": 2}
    assert embeddings.calls == [
        [
            "The user fixed flaky CI retries and the tests passed.",
            "The user planned a vacation.",
        ]
    ]
    assert [doc["content"] for doc in store.docs] == [
        "The user fixed flaky CI retries and the tests passed.",
        "The user planned a vacation.",
    ]
    assert all(doc["id"].startswith("ep_") for doc in store.docs)
    assert all(doc["embedding"] == [1.0] for doc in store.docs)
    assert store.supersede_calls == []
    assert store.search_calls == []


def test_extract_episodes_empty_window_persists_nothing(monkeypatch) -> None:
    monkeypatch.setenv("EPISODE_TOPIC_DRIFT", "0")
    service, store, embeddings = _service([{"episodes": []}])

    result = service.extract_episodes("u1", "t1", flush=True)

    assert result == {"episodes": 0}
    assert store.docs == []
    assert embeddings.calls == []


def test_extract_episodes_skips_malformed_episode_with_warning(caplog, monkeypatch) -> None:
    monkeypatch.setenv("EPISODE_TOPIC_DRIFT", "0")
    service, store, _ = _service(
        [
            {
                "episodes": [
                    {**_episode(), "title": ""},
                    _episode(title="Valid episode", summary="The valid episode persisted."),
                ]
            }
        ]
    )

    with caplog.at_level(logging.WARNING):
        result = service.extract_episodes("u1", "t1", flush=True)

    assert result == {"episodes": 1}
    assert [doc["title"] for doc in store.docs] == ["Valid episode"]
    assert "dropping malformed episode" in caplog.text


def test_extract_memories_durable_keeps_episodic_empty_regression_guard() -> None:
    store = _TrackingStore([])
    service = PipelineService(
        store,
        _SyncChat([_response()]),
        _SyncEmbeddings(),
        containers=_containers_for_store(store, turns_store=_Store([_turn(1)])),
    )

    output = service.extract_memories_durable("u1", "t1")

    assert output["facts"]
    assert output["episodic"] == []
