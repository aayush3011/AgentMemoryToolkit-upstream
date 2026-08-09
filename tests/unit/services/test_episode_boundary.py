"""Boundary-based episodic segmentation (sync).

Episodes are finalized at detected boundaries in the open turn stream - an idle
time-gap, a topic-drift shift, or a max-size cap - never on a fixed turn cadence
and never by the caller signaling "session end". Turns folded into an episode are
stamped ``episode_extracted_at`` (an independent watermark) so re-evaluation is
idempotent and episodes never duplicate.
"""

from __future__ import annotations

from typing import Any

from azure.cosmos.agent_memory.services.pipeline import PipelineService
from tests.unit.services.test_extract_dry import (
    _containers_for_store,
    _Store,
    _SyncChat,
    _SyncEmbeddings,
)
from tests.unit.services.test_extract_episodes import _episode, _TrackingStore


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


class _DriftEmbeddings(_SyncEmbeddings):
    """Content-keyed embeddings: turns tagged 'A:' vs 'B:' land on orthogonal axes."""

    def generate_batch(self, texts: list[str]) -> list[list[float]]:
        self.calls.append(list(texts))
        return [[0.0, 1.0] if "B:" in text else [1.0, 0.0] for text in texts]


def _service(
    turns: list[dict[str, Any]],
    responses: list[dict[str, Any]] | None = None,
    *,
    embeddings: _SyncEmbeddings | None = None,
) -> tuple[PipelineService, _TrackingStore, _Store, _SyncChat]:
    memories = _TrackingStore([])
    turns_store = _Store(turns)
    chat = _SyncChat(responses or [{"episodes": [_episode()]} for _ in range(20)])
    service = PipelineService(
        memories,
        chat,
        embeddings or _SyncEmbeddings(),
        containers=_containers_for_store(memories, turns_store=turns_store),
    )
    # Boundary tests exercise segmentation logic, not prompt rendering: stub
    # _run_prompty so we return canned episode JSON without invoking the real
    # prompty template renderer (keeps these tests fast and hermetic).
    service._run_prompty = lambda *a, **k: chat.generate([])  # type: ignore[assignment]
    return service, memories, turns_store, chat


def _episodes(store: _TrackingStore) -> list[dict[str, Any]]:
    return [doc for doc in store.docs if doc.get("type") == "episodic"]


def _stamped(turns_store: _Store) -> list[str]:
    return sorted(t["id"] for t in turns_store.docs if t.get("episode_extracted_at"))


def test_time_gap_closes_prior_episode_and_leaves_tail_open(monkeypatch) -> None:
    monkeypatch.setenv("EPISODE_IDLE_GAP_SECONDS", "120")
    monkeypatch.setenv("EPISODE_TOPIC_DRIFT", "0")
    monkeypatch.setenv("EPISODE_MAX_TURNS", "40")
    # 00:01, 00:02 then a 28-minute gap to 00:30, 00:31.
    turns = [_turn_at(1, 1), _turn_at(2, 2), _turn_at(3, 30), _turn_at(4, 31)]
    service, memories, turns_store, _ = _service(turns)

    result = service.extract_episodes("u1", "t1")

    assert result == {"episodes": 1}
    assert len(_episodes(memories)) == 1
    # Only the pre-gap segment is closed; the tail stays open.
    assert _stamped(turns_store) == ["turn-1", "turn-2"]


def test_reevaluation_is_idempotent_via_watermark(monkeypatch) -> None:
    monkeypatch.setenv("EPISODE_IDLE_GAP_SECONDS", "120")
    monkeypatch.setenv("EPISODE_TOPIC_DRIFT", "0")
    turns = [_turn_at(1, 1), _turn_at(2, 2), _turn_at(3, 30), _turn_at(4, 31)]
    service, memories, turns_store, _ = _service(turns)

    first = service.extract_episodes("u1", "t1")
    second = service.extract_episodes("u1", "t1")

    assert first == {"episodes": 1}
    # The open tail has no further boundary: no new episode, no duplicate.
    assert second == {"episodes": 0}
    assert len(_episodes(memories)) == 1


def test_flush_drains_open_tail(monkeypatch) -> None:
    monkeypatch.setenv("EPISODE_IDLE_GAP_SECONDS", "120")
    monkeypatch.setenv("EPISODE_TOPIC_DRIFT", "0")
    turns = [_turn_at(1, 1), _turn_at(2, 2), _turn_at(3, 30), _turn_at(4, 31)]
    service, memories, turns_store, _ = _service(turns)

    service.extract_episodes("u1", "t1")  # closes [turn-1, turn-2]
    flushed = service.extract_episodes("u1", "t1", flush=True)  # drains [turn-3, turn-4]

    assert flushed == {"episodes": 1}
    assert len(_episodes(memories)) == 2
    assert _stamped(turns_store) == ["turn-1", "turn-2", "turn-3", "turn-4"]
    # Everything stamped: a further flush is a no-op.
    assert service.extract_episodes("u1", "t1", flush=True) == {"episodes": 0}


def test_max_turns_forces_a_boundary(monkeypatch) -> None:
    monkeypatch.setenv("EPISODE_IDLE_GAP_SECONDS", "0")  # disable gap
    monkeypatch.setenv("EPISODE_TOPIC_DRIFT", "0")  # disable drift
    monkeypatch.setenv("EPISODE_MAX_TURNS", "2")  # force a boundary every 2 turns
    turns = [_turn_at(i, i) for i in range(1, 5)]  # 4 turns, no large gaps
    service, memories, turns_store, _ = _service(turns)

    result = service.extract_episodes("u1", "t1")

    # Two forced segments: [turn-1, turn-2] and [turn-3, turn-4].
    assert result == {"episodes": 2}
    assert _stamped(turns_store) == ["turn-1", "turn-2", "turn-3", "turn-4"]


def test_topic_drift_closes_episode(monkeypatch) -> None:
    monkeypatch.setenv("EPISODE_IDLE_GAP_SECONDS", "0")  # isolate drift
    monkeypatch.setenv("EPISODE_TOPIC_DRIFT", "0.5")
    monkeypatch.setenv("EPISODE_MIN_TURNS", "2")
    monkeypatch.setenv("EPISODE_MAX_TURNS", "40")
    turns = [
        _turn_at(1, 1, content="A: apples and orchards"),
        _turn_at(2, 2, content="A: more about apples"),
        _turn_at(3, 3, content="B: rockets and orbits"),
        _turn_at(4, 4, content="B: more about rockets"),
    ]
    service, memories, turns_store, _ = _service(turns, embeddings=_DriftEmbeddings())

    result = service.extract_episodes("u1", "t1")

    # Topic A closes when topic B arrives at turn-3; the B tail stays open.
    assert result == {"episodes": 1}
    assert _stamped(turns_store) == ["turn-1", "turn-2"]


def test_no_boundary_keeps_segment_open_without_calling_the_llm(monkeypatch) -> None:
    monkeypatch.setenv("EPISODE_IDLE_GAP_SECONDS", "1800")
    monkeypatch.setenv("EPISODE_TOPIC_DRIFT", "0")
    monkeypatch.setenv("EPISODE_MAX_TURNS", "40")
    turns = [_turn_at(1, 1), _turn_at(2, 2), _turn_at(3, 3)]  # close together, small
    service, memories, turns_store, chat = _service(turns)

    result = service.extract_episodes("u1", "t1")

    assert result == {"episodes": 0}
    assert _episodes(memories) == []
    assert _stamped(turns_store) == []  # nothing closed
    assert chat.calls == 0  # extraction LLM only runs at a boundary


def test_deterministic_episode_id_is_stable_and_content_scoped() -> None:
    service, *_ = _service([])
    key = "u1\x00t1\x00turn-1\x00turn-2"
    id_a = service._deterministic_episode_id(key, "hash-aaa")
    id_a_again = service._deterministic_episode_id(key, "hash-aaa")
    id_b = service._deterministic_episode_id(key, "hash-bbb")
    id_other_segment = service._deterministic_episode_id("u1\x00t1\x00turn-3\x00turn-4", "hash-aaa")

    assert id_a.startswith("ep_")
    assert id_a == id_a_again  # same segment + content -> same id (idempotent)
    assert id_a != id_b  # different content -> different id
    assert id_a != id_other_segment  # different segment -> different id
