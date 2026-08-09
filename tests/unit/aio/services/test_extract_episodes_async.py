from __future__ import annotations

import logging
from typing import Any

import pytest

from azure.cosmos.agent_memory.aio.services.pipeline import AsyncPipelineService
from tests.unit.services.test_extract_dry import (
    _async_containers_for_store,
    _AsyncChat,
    _AsyncEmbeddings,
    _AsyncStore,
    _response,
    _turn,
)
from tests.unit.services.test_extract_episodes import _episode


class _AsyncTrackingStore(_AsyncStore):
    def __init__(self, docs: list[dict[str, Any]]):
        super().__init__(docs)
        self.supersede_calls: list[dict[str, Any]] = []

    async def mark_superseded(self, old_doc: dict[str, Any], superseder_id: str, *, reason: str) -> bool:
        self.supersede_calls.append({"old_doc": old_doc, "superseder_id": superseder_id, "reason": reason})
        return True


def _service(
    responses: list[dict[str, Any]],
    *,
    memories_store: _AsyncTrackingStore | None = None,
    embeddings: _AsyncEmbeddings | None = None,
) -> tuple[AsyncPipelineService, _AsyncTrackingStore, _AsyncEmbeddings]:
    store = memories_store or _AsyncTrackingStore([])
    embedding_client = embeddings or _AsyncEmbeddings()
    turns_store = _AsyncStore([_turn(1), _turn(2)])
    service = AsyncPipelineService(
        store,
        _AsyncChat(responses),
        embedding_client,
        containers=_async_containers_for_store(store, turns_store=turns_store),
    )
    return service, store, embedding_client


@pytest.mark.asyncio
async def test_build_episode_docs_returns_multiple_docs_without_embeddings() -> None:
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

    docs = await service._build_episode_docs("u1", "t1", [_turn(1), _turn(2)], segment_key="seg-1")

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


@pytest.mark.asyncio
async def test_extract_episodes_embeds_content_persists_append_only(monkeypatch) -> None:
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

    result = await service.extract_episodes("u1", "t1", flush=True)

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


@pytest.mark.asyncio
async def test_extract_episodes_empty_window_persists_nothing(monkeypatch) -> None:
    monkeypatch.setenv("EPISODE_TOPIC_DRIFT", "0")
    service, store, embeddings = _service([{"episodes": []}])

    result = await service.extract_episodes("u1", "t1", flush=True)

    assert result == {"episodes": 0}
    assert store.docs == []
    assert embeddings.calls == []


@pytest.mark.asyncio
async def test_extract_episodes_skips_malformed_episode_with_warning(caplog, monkeypatch) -> None:
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
        result = await service.extract_episodes("u1", "t1", flush=True)

    assert result == {"episodes": 1}
    assert [doc["title"] for doc in store.docs] == ["Valid episode"]
    assert "dropping malformed episode" in caplog.text


@pytest.mark.asyncio
async def test_extract_memories_durable_keeps_episodic_empty_regression_guard() -> None:
    store = _AsyncTrackingStore([])
    service = AsyncPipelineService(
        store,
        _AsyncChat([_response()]),
        _AsyncEmbeddings(),
        containers=_async_containers_for_store(store, turns_store=_AsyncStore([_turn(1)])),
    )

    output = await service.extract_memories_durable("u1", "t1")

    assert output["facts"]
    assert output["episodic"] == []
