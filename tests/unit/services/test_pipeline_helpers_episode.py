"""Unit tests for the shared, IO-free episode segmentation helpers.

These pure functions live in ``services/_pipeline_helpers.py`` so the sync and
aio pipelines share one implementation. A single test file covers both.
"""

from __future__ import annotations

from azure.cosmos.agent_memory.services._pipeline_helpers import (
    created_at_sort_key,
    deterministic_episode_id,
    find_episode_boundary,
    is_valid_time_pair,
    parse_iso_datetime,
    segment_time_bounds,
    turn_gap_seconds,
)


def _turn(minute: int) -> dict[str, object]:
    return {"id": f"turn-{minute}", "created_at": f"2025-01-01T00:{minute:02d}:00+00:00"}


class TestDeterministicEpisodeId:
    def test_stable_for_same_segment_and_index(self) -> None:
        assert deterministic_episode_id("seg", 0) == deterministic_episode_id("seg", 0)

    def test_differs_by_index(self) -> None:
        assert deterministic_episode_id("seg", 0) != deterministic_episode_id("seg", 1)

    def test_differs_by_segment_key(self) -> None:
        assert deterministic_episode_id("seg-a", 0) != deterministic_episode_id("seg-b", 0)

    def test_ep_prefix(self) -> None:
        assert deterministic_episode_id("seg", 0).startswith("ep_")


class TestTimeHelpers:
    def test_parse_naive_gets_utc(self) -> None:
        dt = parse_iso_datetime("2025-01-01T00:00:00")
        assert dt is not None and dt.tzinfo is not None

    def test_parse_unparseable_is_none(self) -> None:
        assert parse_iso_datetime("not a date") is None

    def test_is_valid_time_pair_mixed_tz(self) -> None:
        assert is_valid_time_pair("2026-03-09", "2026-03-10T09:08:00+00:00") is True

    def test_is_valid_time_pair_reversed(self) -> None:
        assert is_valid_time_pair("2026-03-10T00:00:00+00:00", "2026-03-09") is False

    def test_turn_gap_seconds(self) -> None:
        assert turn_gap_seconds(_turn(1), _turn(3)) == 120.0

    def test_turn_gap_seconds_missing_is_none(self) -> None:
        assert turn_gap_seconds({"id": "x"}, _turn(3)) is None

    def test_segment_time_bounds(self) -> None:
        assert segment_time_bounds([_turn(3), _turn(1), _turn(2)]) == (
            "2025-01-01T00:01:00+00:00",
            "2025-01-01T00:03:00+00:00",
        )

    def test_segment_time_bounds_empty(self) -> None:
        assert segment_time_bounds([]) == (None, None)

    def test_segment_time_bounds_mixed_offsets_sort_chronologically(self) -> None:
        items = [
            {"created_at": "2025-01-01T05:00:00Z"},
            {"created_at": "2025-01-01T09:00:00+05:00"},
        ]
        assert segment_time_bounds(items) == (
            "2025-01-01T09:00:00+05:00",
            "2025-01-01T05:00:00Z",
        )


class TestCreatedAtSortKey:
    def test_tied_timestamps_break_deterministically_on_id(self) -> None:
        # Turns sharing one timestamp (common when many turns carry the same
        # session date) must keep a stable, id-ordered sequence so the segment's
        # first/last ids - and thus the deterministic episode id - do not drift.
        items = [
            {"id": "c", "created_at": "2025-01-01T00:00:00+00:00"},
            {"id": "a", "created_at": "2025-01-01T00:00:00+00:00"},
            {"id": "b", "created_at": "2025-01-01T00:00:00+00:00"},
        ]
        assert [i["id"] for i in sorted(items, key=created_at_sort_key)] == ["a", "b", "c"]

    def test_mixed_offsets_sort_by_true_instant_not_lexical(self) -> None:
        # 09:00+05:00 (=04:00Z) precedes 05:00Z chronologically, though the raw
        # string "09..." sorts after "05..." lexically.
        items = [
            {"id": "later", "created_at": "2025-01-01T05:00:00Z"},
            {"id": "earlier", "created_at": "2025-01-01T09:00:00+05:00"},
        ]
        assert [i["id"] for i in sorted(items, key=created_at_sort_key)] == ["earlier", "later"]

    def test_missing_or_unparseable_sorts_last_by_id(self) -> None:
        items = [
            {"id": "no-ts"},
            {"id": "bad-ts", "created_at": "not-a-date"},
            {"id": "has-ts", "created_at": "2025-01-01T00:00:00Z"},
        ]
        assert [i["id"] for i in sorted(items, key=created_at_sort_key)] == ["has-ts", "bad-ts", "no-ts"]


class TestFindEpisodeBoundary:
    _KN = {"max_turns": 40, "idle_gap": 120, "drift": 0.0, "min_turns": 2}

    def test_no_boundary_returns_none(self) -> None:
        seg = [_turn(1), _turn(2), _turn(3)]
        assert find_episode_boundary(seg, [], **self._KN) is None

    def test_idle_gap_closes_at_index(self) -> None:
        # 00:01, 00:02, then a 28-minute gap to 00:30 -> boundary at i=2.
        seg = [_turn(1), _turn(2), _turn(30)]
        assert find_episode_boundary(seg, [], **self._KN) == 2

    def test_idle_gap_below_min_turns_is_suppressed(self) -> None:
        # Gap between i=0 and i=1 is below min_turns=2 -> not closed.
        seg = [_turn(1), _turn(30), _turn(31)]
        assert find_episode_boundary(seg, [], **self._KN) is None

    def test_max_turns_cap_closes(self) -> None:
        seg = [_turn(i) for i in range(1, 6)]
        assert find_episode_boundary(seg, [], max_turns=3, idle_gap=0, drift=0.0, min_turns=2) == 3

    def test_max_turns_not_floored_when_below_min_turns(self) -> None:
        # The max-size cap is a hard ceiling and is NOT floored by min_turns.
        seg = [_turn(i) for i in range(1, 5)]
        assert find_episode_boundary(seg, [], max_turns=2, idle_gap=0, drift=0.0, min_turns=3) == 2

    def test_earliest_boundary_wins_across_signals(self) -> None:
        # Idle gap at i=2 vs a max cap at i=3: the earliest boundary (i=2) wins.
        seg = [_turn(1), _turn(2), _turn(30), _turn(31)]
        assert find_episode_boundary(seg, [], max_turns=3, idle_gap=120, drift=0.0, min_turns=2) == 2
