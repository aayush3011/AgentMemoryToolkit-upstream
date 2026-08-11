"""Boundary-based episodic segmentation (async mirror of test_episode_boundary).

A per-thread ``(created_at, id)`` cursor doc (not a per-turn stamp) advances past
folded turns, so episodic segmentation writes nothing to the turns container and
cannot perturb the change-feed cadence counter."""

from __future__ import annotations

from typing import Any

import pytest

from azure.cosmos.agent_memory.aio.services.pipeline import AsyncPipelineService
from tests.unit.aio.services.test_extract_episodes_async import _AsyncTrackingStore
from tests.unit.services.test_extract_dry import (
    _async_containers_for_store,
    _AsyncChat,
    _AsyncEmbeddings,
    _AsyncStore,
)
from tests.unit.services.test_extract_episodes import _episode


def _turn_at(i: int, minute: int, *, content: str | None = None) -> dict[str, Any]:
    return {
        "id": f"turn-{i}",
        "user_id": "u1",
        "thread_id": "t1",
        "role": "user",
        "type": "turn",
        "content": content if content is not None else f"Turn {i}: routine content",
        "created_at": f"2025-01-01T00:{minute:02d}:00+00:00",
    }


class _AsyncDriftEmbeddings(_AsyncEmbeddings):
    async def generate_batch(self, texts: list[str]) -> list[list[float]]:
        self.calls.append(list(texts))
        return [[0.0, 1.0] if "B:" in text else [1.0, 0.0] for text in texts]


def _service(
    turns: list[dict[str, Any]],
    responses: list[dict[str, Any]] | None = None,
    *,
    embeddings: _AsyncEmbeddings | None = None,
) -> tuple[AsyncPipelineService, _AsyncTrackingStore, _AsyncStore, _AsyncChat]:
    memories = _AsyncTrackingStore([])
    turns_store = _AsyncStore(turns)
    chat = _AsyncChat(responses or [{"episodes": [_episode()]} for _ in range(20)])
    service = AsyncPipelineService(
        memories,
        chat,
        embeddings or _AsyncEmbeddings(),
        containers=_async_containers_for_store(memories, turns_store=turns_store),
    )

    # Stub _run_prompty so boundary tests return canned episode JSON without
    # invoking the real prompty template renderer (fast and hermetic).
    async def _fake_run_prompty(*a: Any, **k: Any) -> str:
        return await chat.generate([])

    service._run_prompty = _fake_run_prompty  # type: ignore[assignment]
    return service, memories, turns_store, chat


def _episodes(store: _AsyncTrackingStore) -> list[dict[str, Any]]:
    return [doc for doc in store.docs if doc.get("type") == "episodic"]


def _folded(memories: _AsyncTrackingStore, turns_store: _AsyncStore) -> list[str]:
    """Turn ids folded into an episode, derived from the per-thread episodic
    cursor doc (the watermark model advances a single cursor instead of stamping
    each turn, so nothing is written to the turns container)."""
    cursor = next((d for d in memories.docs if d.get("type") == "episode_cursor"), None)
    if cursor is None:
        return []
    last_at = str(cursor.get("last_episode_at") or "")
    last_id = str(cursor.get("last_episode_id") or "")
    return sorted(
        str(t.get("id"))
        for t in turns_store.docs
        if t.get("type") == "turn"
        and (
            (str(t.get("created_at") or "") < last_at)
            or (str(t.get("created_at") or "") == last_at and str(t.get("id") or "") <= last_id)
        )
    )


@pytest.mark.asyncio
async def test_time_gap_closes_prior_episode_and_leaves_tail_open(monkeypatch) -> None:
    monkeypatch.setenv("EPISODE_IDLE_GAP_SECONDS", "120")
    monkeypatch.setenv("EPISODE_TOPIC_DRIFT", "0")
    turns = [_turn_at(1, 1), _turn_at(2, 2), _turn_at(3, 30), _turn_at(4, 31)]
    service, memories, turns_store, _ = _service(turns)

    result = await service.extract_episodes("u1", "t1")

    assert result == {"episodes": 1}
    assert len(_episodes(memories)) == 1
    assert _folded(memories, turns_store) == ["turn-1", "turn-2"]


@pytest.mark.asyncio
async def test_reevaluation_is_idempotent_via_watermark(monkeypatch) -> None:
    monkeypatch.setenv("EPISODE_IDLE_GAP_SECONDS", "120")
    monkeypatch.setenv("EPISODE_TOPIC_DRIFT", "0")
    turns = [_turn_at(1, 1), _turn_at(2, 2), _turn_at(3, 30), _turn_at(4, 31)]
    service, memories, _, _ = _service(turns)

    first = await service.extract_episodes("u1", "t1")
    second = await service.extract_episodes("u1", "t1")

    assert first == {"episodes": 1}
    assert second == {"episodes": 0}
    assert len(_episodes(memories)) == 1


@pytest.mark.asyncio
async def test_flush_drains_open_tail(monkeypatch) -> None:
    monkeypatch.setenv("EPISODE_IDLE_GAP_SECONDS", "120")
    monkeypatch.setenv("EPISODE_TOPIC_DRIFT", "0")
    turns = [_turn_at(1, 1), _turn_at(2, 2), _turn_at(3, 30), _turn_at(4, 31)]
    service, memories, turns_store, _ = _service(turns)

    await service.extract_episodes("u1", "t1")
    flushed = await service.extract_episodes("u1", "t1", flush=True)

    assert flushed == {"episodes": 1}
    assert len(_episodes(memories)) == 2
    assert _folded(memories, turns_store) == ["turn-1", "turn-2", "turn-3", "turn-4"]
    assert await service.extract_episodes("u1", "t1", flush=True) == {"episodes": 0}


@pytest.mark.asyncio
async def test_max_turns_forces_a_boundary(monkeypatch) -> None:
    monkeypatch.setenv("EPISODE_IDLE_GAP_SECONDS", "0")
    monkeypatch.setenv("EPISODE_TOPIC_DRIFT", "0")
    monkeypatch.setenv("EPISODE_MAX_TURNS", "2")
    turns = [_turn_at(i, i) for i in range(1, 5)]
    service, memories, turns_store, _ = _service(turns)

    result = await service.extract_episodes("u1", "t1")

    assert result == {"episodes": 2}
    assert _folded(memories, turns_store) == ["turn-1", "turn-2", "turn-3", "turn-4"]


@pytest.mark.asyncio
async def test_topic_drift_closes_episode(monkeypatch) -> None:
    monkeypatch.setenv("EPISODE_IDLE_GAP_SECONDS", "0")
    monkeypatch.setenv("EPISODE_TOPIC_DRIFT", "0.5")
    monkeypatch.setenv("EPISODE_MIN_TURNS", "2")
    turns = [
        _turn_at(1, 1, content="A: apples and orchards"),
        _turn_at(2, 2, content="A: more about apples"),
        _turn_at(3, 3, content="B: rockets and orbits"),
        _turn_at(4, 4, content="B: more about rockets"),
    ]
    service, memories, turns_store, _ = _service(turns, embeddings=_AsyncDriftEmbeddings())

    result = await service.extract_episodes("u1", "t1")

    assert result == {"episodes": 1}
    assert _folded(memories, turns_store) == ["turn-1", "turn-2"]


@pytest.mark.asyncio
async def test_no_boundary_keeps_segment_open_without_calling_the_llm(monkeypatch) -> None:
    monkeypatch.setenv("EPISODE_IDLE_GAP_SECONDS", "1800")
    monkeypatch.setenv("EPISODE_TOPIC_DRIFT", "0")
    monkeypatch.setenv("EPISODE_MAX_TURNS", "40")
    turns = [_turn_at(1, 1), _turn_at(2, 2), _turn_at(3, 3)]
    service, memories, turns_store, chat = _service(turns)

    result = await service.extract_episodes("u1", "t1")

    assert result == {"episodes": 0}
    assert _episodes(memories) == []
    assert _folded(memories, turns_store) == []
    assert chat.calls == 0


@pytest.mark.asyncio
async def test_idle_gap_below_min_turns_does_not_close_episode(monkeypatch) -> None:
    monkeypatch.setenv("EPISODE_IDLE_GAP_SECONDS", "120")
    monkeypatch.setenv("EPISODE_TOPIC_DRIFT", "0")
    monkeypatch.setenv("EPISODE_MAX_TURNS", "40")
    monkeypatch.setenv("EPISODE_MIN_TURNS", "2")
    turns = [_turn_at(1, 1), _turn_at(2, 30), _turn_at(3, 31)]
    service, memories, turns_store, _ = _service(turns)

    result = await service.extract_episodes("u1", "t1")

    assert result == {"episodes": 0}
    assert _episodes(memories) == []
    assert _folded(memories, turns_store) == []


@pytest.mark.asyncio
async def test_idle_gap_below_min_turns_still_flushes_as_one_episode(monkeypatch) -> None:
    monkeypatch.setenv("EPISODE_IDLE_GAP_SECONDS", "120")
    monkeypatch.setenv("EPISODE_TOPIC_DRIFT", "0")
    monkeypatch.setenv("EPISODE_MAX_TURNS", "40")
    monkeypatch.setenv("EPISODE_MIN_TURNS", "2")
    turns = [_turn_at(1, 1), _turn_at(2, 30), _turn_at(3, 31)]
    service, memories, turns_store, _ = _service(turns)

    result = await service.extract_episodes("u1", "t1", flush=True)

    assert result == {"episodes": 1}
    assert _folded(memories, turns_store) == ["turn-1", "turn-2", "turn-3"]


@pytest.mark.asyncio
async def test_extract_episodes_defers_segment_on_retryable_error(monkeypatch) -> None:
    monkeypatch.setenv("EPISODE_IDLE_GAP_SECONDS", "120")
    monkeypatch.setenv("EPISODE_TOPIC_DRIFT", "0")
    monkeypatch.setenv("EPISODE_MAX_TURNS", "40")
    turns = [_turn_at(1, 1), _turn_at(2, 2), _turn_at(3, 30)]  # gap closes [1,2]
    service, memories, turns_store, _ = _service(turns)

    async def _boom(*a: Any, **k: Any) -> str:
        raise RuntimeError("transient rate limit 429")

    service._run_prompty = _boom  # type: ignore[assignment]

    result = await service.extract_episodes("u1", "t1")

    assert result == {"episodes": 0}
    assert _episodes(memories) == []
    assert _folded(memories, turns_store) == []  # un-stamped -> retried next run


@pytest.mark.asyncio
async def test_extract_episodes_quarantines_segment_on_non_retryable_error(monkeypatch) -> None:
    monkeypatch.setenv("EPISODE_IDLE_GAP_SECONDS", "120")
    monkeypatch.setenv("EPISODE_TOPIC_DRIFT", "0")
    monkeypatch.setenv("EPISODE_MAX_TURNS", "40")
    turns = [_turn_at(1, 1), _turn_at(2, 2), _turn_at(3, 30)]
    service, memories, turns_store, _ = _service(turns)

    async def _boom(*a: Any, **k: Any) -> str:
        raise RuntimeError("content_filter triggered")

    service._run_prompty = _boom  # type: ignore[assignment]

    result = await service.extract_episodes("u1", "t1")

    assert result == {"episodes": 0}
    assert _episodes(memories) == []
    assert _folded(memories, turns_store) == ["turn-1", "turn-2"]  # quarantined + advanced


@pytest.mark.asyncio
async def test_advance_episode_cursor_is_monotonic(monkeypatch) -> None:
    monkeypatch.setenv("EPISODE_IDLE_GAP_SECONDS", "120")
    monkeypatch.setenv("EPISODE_TOPIC_DRIFT", "0")
    turns = [_turn_at(1, 1), _turn_at(2, 2), _turn_at(3, 30), _turn_at(4, 31)]
    service, memories, turns_store, _ = _service(turns)

    await service.extract_episodes("u1", "t1")  # gap closes [turn-1, turn-2] -> cursor at turn-2
    assert _folded(memories, turns_store) == ["turn-1", "turn-2"]

    # A late, out-of-order concurrent run advancing to an OLDER turn is a no-op.
    await service._advance_episode_cursor("u1", "t1", _turn_at(1, 1))
    assert _folded(memories, turns_store) == ["turn-1", "turn-2"]

    # A genuinely newer turn still advances the cursor.
    await service._advance_episode_cursor("u1", "t1", _turn_at(4, 31))
    assert _folded(memories, turns_store) == ["turn-1", "turn-2", "turn-3", "turn-4"]


@pytest.mark.asyncio
async def test_episode_cursor_is_per_thread(monkeypatch) -> None:
    monkeypatch.setenv("EPISODE_IDLE_GAP_SECONDS", "120")
    monkeypatch.setenv("EPISODE_TOPIC_DRIFT", "0")

    def _t(i: int, minute: int, thread: str) -> dict[str, Any]:
        return {**_turn_at(i, minute), "thread_id": thread}

    turns = [
        _t(1, 1, "t1"),
        _t(2, 2, "t1"),
        _t(3, 30, "t1"),
        _t(4, 1, "t2"),
        _t(5, 2, "t2"),
        _t(6, 30, "t2"),
    ]
    service, memories, turns_store, _ = _service(turns)

    await service.extract_episodes("u1", "t1")  # only thread t1

    cursors = sorted(d["thread_id"] for d in memories.docs if d.get("type") == "episode_cursor")
    assert cursors == ["t1"]
    t2_segment = await service._load_open_episode_segment("u1", "t2")
    assert [t["id"] for t in t2_segment] == ["turn-4", "turn-5", "turn-6"]
